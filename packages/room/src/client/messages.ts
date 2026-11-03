export { roomMessages }

import { stringify } from '@brillout/json-serializer/stringify'
import { ChannelOverflowError } from 'telefunc/client'
import { untether, createDeferred, type ChannelPublishInfo } from 'telefunc/__internal'
import type { BinaryFrameInfo, RoomMessage, Sender } from '../types.js'
import { senderSnapshot } from '../shared/arguments.js'
import { Fifo } from '../utils/Fifo.js'
import { utf8ByteLength } from '../utils/utf8ByteLength.js'

/** A queued message, and its bytes besides its sender. */
type Queued = { data: RoomMessage; size: number }

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
  // A message keeps its sender as the listeners got it when it arrived (a live participant's meta changes, and the
  // participant outlives its membership): a snapshot of the id, the identity and the meta object the sender had then,
  // frozen down to its plain objects and arrays, and read-only beyond them. A sender's messages share one snapshot
  // until its meta changes, and each snapshot is measured once, and counted once while any queued message holds it.
  const snapshots = new WeakMap<Sender, Sender>()
  const senderBytes = new WeakMap<Sender, number>()
  let heldSenders = new Map<Sender, number>()
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
    heldSenders = new Map()
    bytes = 0
  }
  const snapshotOf = (from: Sender) => {
    const known = snapshots.get(from)
    if (known?.meta === from.meta) return known
    const snapshot = senderSnapshot(from.id, from.meta, from.identity)
    snapshots.set(from, snapshot)
    return snapshot
  }
  const sizeOfSender = (sender: Sender) => {
    let size = senderBytes.get(sender)
    if (size === undefined) {
      size = utf8ByteLength(sender.id) + utf8ByteLength(sender.identity ?? '') + utf8ByteLength(stringify(sender.meta))
      senderBytes.set(sender, size)
    }
    return size
  }
  const onAbort = () => finish(signal!.reason)
  const receive = (data: RoomMessage, payloadBytes: number) => {
    const size = payloadBytes + 64
    const holders = heldSenders.get(data.from) ?? 0
    const added = size + (holders === 0 ? sizeOfSender(data.from) : 0)
    if (bytes + added > bufferLimit) return finish(new ChannelOverflowError('Room message iterator buffer overflow'))
    bytes += added
    heldSenders.set(data.from, holders + 1)
    queue.push({ data, size })
    changed.resolve()
  }
  const release = ({ data, size }: Queued) => {
    bytes -= size
    const holders = heldSenders.get(data.from)! - 1
    if (holders > 0) {
      heldSenders.set(data.from, holders)
    } else {
      heldSenders.delete(data.from)
      bytes -= sizeOfSender(data.from)
    }
  }
  const start = () => {
    started = true
    if (signal?.aborted) return onAbort()
    signal?.addEventListener('abort', onAbort, { once: true })
    source.onClose(() => finish(), { signal: lifetime.signal })
    source.subscribe(
      (data, info, from) =>
        receive({ type: 'text', data, info, from: snapshotOf(from) }, utf8ByteLength(stringify(data))),
      { signal: lifetime.signal },
    )
    // The bytes are this listener's own copy.
    source.subscribeBinary(
      (data, info, from) =>
        receive(
          { type: 'binary', data, info, from: snapshotOf(from) },
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
