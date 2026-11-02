export { roomMessages }
export type { RoomMessage }

import { stringify } from '@brillout/json-serializer/stringify'
import { ChannelOverflowError } from 'telefunc/client'
import { untether, createDeferred } from 'telefunc/__internal'
import { Fifo } from '../utils/Fifo.js'
import { utf8ByteLength } from '../utils/utf8ByteLength.js'

type RoomMessage<T = unknown> = { type: 'text'; data: T } | { type: 'binary'; data: Uint8Array }

type Source = {
  subscribe(callback: (data: unknown) => void, options: { signal: AbortSignal }): () => void
  subscribeBinary(callback: (data: Uint8Array) => void, options: { signal: AbortSignal }): () => void
  onClose(callback: () => void, options: { signal: AbortSignal }): () => void
}

function roomMessages(source: Source, bufferLimit: number, signal?: AbortSignal): AsyncIterableIterator<RoomMessage> {
  const lifetime = new AbortController()
  let queue = new Fifo<{ data: RoomMessage; size: number }>()
  let started = false
  let bytes = 0
  let changed = createDeferred()
  let failure: unknown
  const finish = (error?: unknown) => {
    failure = error
    untether(iterator)
    lifetime.abort()
    signal?.removeEventListener('abort', onAbort)
    if (error !== undefined) {
      queue = new Fifo()
      bytes = 0
    }
    changed.resolve()
  }
  const onAbort = () => finish(signal!.reason)
  const receive = (data: RoomMessage) => {
    const size = (data.type === 'binary' ? data.data.byteLength : utf8ByteLength(stringify(data.data))) + 64
    if (bytes + size > bufferLimit) return finish(new ChannelOverflowError('Room message iterator buffer overflow'))
    bytes += size
    queue.push({ data, size })
    changed.resolve()
  }
  const start = () => {
    started = true
    if (signal?.aborted) return onAbort()
    signal?.addEventListener('abort', onAbort, { once: true })
    source.onClose(() => finish(), { signal: lifetime.signal })
    source.subscribe((data) => receive({ type: 'text', data }), { signal: lifetime.signal })
    source.subscribeBinary((data) => receive({ type: 'binary', data: new Uint8Array(data) }), {
      signal: lifetime.signal,
    })
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
          bytes -= entry.size
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
      queue = new Fifo()
      bytes = 0
      return { done: true, value: undefined }
    },
    async throw(error) {
      finish(error)
      throw error
    },
  }
  return iterator
}
