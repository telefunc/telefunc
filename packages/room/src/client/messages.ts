export { roomMessages }

import { stringify } from '@brillout/json-serializer/stringify'
import { ChannelOverflowError } from 'telefunc/client'
import { untether, createDeferred, type ChannelPublishInfo } from 'telefunc/__internal'
import type { BinaryFrameInfo, RoomMessage, Sender } from '../types.js'
import { senderSnapshot } from '../shared/arguments.js'
import { Fifo } from '../utils/Fifo.js'
import { utf8ByteLength } from '../utils/utf8ByteLength.js'

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
  const queue = new MessageQueue(bufferLimit)
  let started = false
  let changed = createDeferred()
  let failure: unknown
  // A normal end leaves what is queued to drain; a failure drops it.
  const finish = (error?: unknown) => {
    failure = error
    untether(iterator)
    lifetime.abort()
    signal?.removeEventListener('abort', onAbort)
    if (error !== undefined) queue.clear()
    changed.resolve()
  }
  const onAbort = () => finish(signal!.reason)
  const receive = (message: RoomMessage, payloadBytes: number) => {
    if (!queue.push(message, payloadBytes))
      return finish(new ChannelOverflowError('Room message iterator buffer overflow'))
    changed.resolve()
  }
  const start = () => {
    started = true
    if (signal?.aborted) return onAbort()
    signal?.addEventListener('abort', onAbort, { once: true })
    source.onClose(() => finish(), { signal: lifetime.signal })
    source.subscribe(
      (data, info, from) => receive({ type: 'text', data, info, from }, utf8ByteLength(stringify(data))),
      {
        signal: lifetime.signal,
      },
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
        const message = queue.shift()
        if (message) return { done: false, value: message }
        if (lifetime.signal.aborted) return { done: true, value: undefined }
        const pending = changed
        await pending.promise
        if (changed === pending) changed = createDeferred()
      }
    },
    async return() {
      finish()
      queue.clear()
      return { done: true, value: undefined }
    },
    async throw(error) {
      finish(error)
      throw error
    },
  }
  return iterator
}

/**
 * The messages `messages()` holds, within `bufferLimit` bytes. A message keeps its sender as the listeners got it when
 * it arrived (a live participant's meta changes, and the participant outlives its membership): a snapshot of the id,
 * the identity and the meta object the sender had then, frozen down to its plain objects and arrays, and read-only
 * beyond them. A sender's messages share one snapshot until its meta changes. The bytes count each message's payload,
 * and each snapshot once while any queued message holds it.
 */
class MessageQueue {
  private readonly bufferLimit: number
  /** Each queued message, with its bytes besides its sender. */
  private queue = new Fifo<{ message: RoomMessage; size: number }>()
  private bytes = 0
  /** How many queued messages hold each snapshot. */
  private holders = new Map<Sender, number>()
  /** Each live sender's last snapshot, and each snapshot's bytes. */
  private readonly snapshots = new WeakMap<Sender, Sender>()
  private readonly snapshotBytes = new WeakMap<Sender, number>()

  constructor(bufferLimit: number) {
    this.bufferLimit = bufferLimit
  }

  /** Queues `message`, its sender as it is now, unless its bytes would pass the limit; then it queues nothing. */
  push(message: RoomMessage, payloadBytes: number): boolean {
    const from = this.snapshotOf(message.from)
    const size = payloadBytes + 64
    const holders = this.holders.get(from) ?? 0
    const added = size + (holders === 0 ? this.sizeOf(from) : 0)
    if (this.bytes + added > this.bufferLimit) return false
    this.bytes += added
    this.holders.set(from, holders + 1)
    this.queue.push({ message: { ...message, from }, size })
    return true
  }

  shift(): RoomMessage | undefined {
    const entry = this.queue.shift()
    if (entry === undefined) return undefined
    const { message, size } = entry
    this.bytes -= size
    const holders = this.holders.get(message.from)! - 1
    if (holders > 0) {
      this.holders.set(message.from, holders)
    } else {
      this.holders.delete(message.from)
      this.bytes -= this.sizeOf(message.from)
    }
    return message
  }

  clear(): void {
    this.queue = new Fifo()
    this.holders = new Map()
    this.bytes = 0
  }

  private snapshotOf(from: Sender): Sender {
    const known = this.snapshots.get(from)
    if (known?.meta === from.meta) return known
    const snapshot = senderSnapshot(from.id, from.meta, from.identity)
    this.snapshots.set(from, snapshot)
    return snapshot
  }

  private sizeOf(sender: Sender): number {
    let size = this.snapshotBytes.get(sender)
    if (size === undefined) {
      size = utf8ByteLength(sender.id) + utf8ByteLength(sender.identity ?? '') + utf8ByteLength(stringify(sender.meta))
      this.snapshotBytes.set(sender, size)
    }
    return size
  }
}
