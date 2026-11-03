export { roomMessages }

import { stringify } from '@brillout/json-serializer/stringify'
import { ChannelOverflowError } from 'telefunc/client'
import { untether, createDeferred, type ChannelPublishInfo } from 'telefunc/__internal'
import type { BinaryFrameInfo, ParticipantMeta, RoomMessage, Sender } from '../types.js'
import { Fifo } from '../utils/Fifo.js'
import { utf8ByteLength } from '../utils/utf8ByteLength.js'

/** A queued message, its bytes besides its sender's meta, and that meta as it was charged: a live sender's meta is a
 *  new object once it changes. */
type Queued = { data: RoomMessage; size: number; meta: ParticipantMeta }

type Source = {
  subscribe(
    callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => void,
    options: { signal: AbortSignal },
  ): () => void
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => void,
    options: { signal: AbortSignal },
  ): () => void
  onClose(callback: () => void, options: { signal: AbortSignal }): () => void
}

function roomMessages(source: Source, bufferLimit: number, signal?: AbortSignal): AsyncIterableIterator<RoomMessage> {
  const lifetime = new AbortController()
  let queue = new Fifo<Queued>()
  let started = false
  let bytes = 0
  // A queued message holds its sender, and so its meta, which may outlive the sender's membership. A meta object is
  // frozen until it changes, so each one is measured once, and counted once while any queued message holds it.
  const metaBytes = new WeakMap<ParticipantMeta, number>()
  let heldMeta = new Map<ParticipantMeta, number>()
  let changed = createDeferred()
  let failure: unknown
  const finish = (error?: unknown) => {
    failure = error
    untether(iterator)
    lifetime.abort()
    signal?.removeEventListener('abort', onAbort)
    if (error !== undefined) clear()
    changed.resolve()
  }
  const clear = () => {
    queue = new Fifo()
    heldMeta = new Map()
    bytes = 0
  }
  const sizeOfMeta = (meta: ParticipantMeta) => {
    let size = metaBytes.get(meta)
    if (size === undefined) metaBytes.set(meta, (size = utf8ByteLength(stringify(meta))))
    return size
  }
  const onAbort = () => finish(signal!.reason)
  const receive = (data: RoomMessage, payloadBytes: number) => {
    const size = payloadBytes + 64
    const meta = data.from.meta
    const holders = heldMeta.get(meta) ?? 0
    const added = size + (holders === 0 ? sizeOfMeta(meta) : 0)
    if (bytes + added > bufferLimit) return finish(new ChannelOverflowError('Room message iterator buffer overflow'))
    bytes += added
    heldMeta.set(meta, holders + 1)
    queue.push({ data, size, meta })
    changed.resolve()
  }
  const release = ({ size, meta }: Queued) => {
    bytes -= size
    const holders = heldMeta.get(meta)! - 1
    if (holders > 0) {
      heldMeta.set(meta, holders)
    } else {
      heldMeta.delete(meta)
      bytes -= sizeOfMeta(meta)
    }
  }
  const start = () => {
    started = true
    if (signal?.aborted) return onAbort()
    signal?.addEventListener('abort', onAbort, { once: true })
    source.onClose(() => finish(), { signal: lifetime.signal })
    source.subscribe(
      (data, info, from) => receive({ type: 'text', data, info, from }, utf8ByteLength(stringify(data))),
      { signal: lifetime.signal },
    )
    // The bytes are this listener's own copy.
    source.subscribeBinary(
      (data, info, from) =>
        receive(
          { type: 'binary', data, info, from },
          data.byteLength + (info.meta ? utf8ByteLength(stringify(info.meta)) : 0),
        ),
      { signal: lifetime.signal },
    )
  }
  const iterator: AsyncIterableIterator<RoomMessage> = {
    [Symbol.asyncIterator]() {
      return this
    },
    async next() {
      if (!started && !lifetime.signal.aborted) start()
      while (true) {
        if (failure !== undefined) throw failure
        const entry = queue.shift()
        if (entry) {
          release(entry)
          return { done: false, value: entry.data }
        }
        if (lifetime.signal.aborted) return { done: true, value: undefined }
        const pending = changed
        await pending.promise
        if (changed === pending) changed = createDeferred()
      }
    },
    async return() {
      finish()
      clear()
      return { done: true, value: undefined }
    },
    async throw(error) {
      finish(error)
      throw error
    },
  }
  return iterator
}
