export { channelMessages, broadcastMessages }
export type { QueueMemory }

import { stringify } from '@brillout/json-serializer/stringify'
import { Fifo } from '../../utils/Fifo.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import { PUBLISH_HOLD_BYTES } from '../constants.js'
import { ChannelOverflowError } from './channelErrors.js'
import type { ChannelPublishInfo } from './channel.js'

interface MessageSource<T> {
  listen(callback: (data: T) => any, options: { signal: AbortSignal }): () => void
  listenBinary(callback: (data: Uint8Array) => any, options: { signal: AbortSignal }): () => void
  readonly closed: Promise<void>
}

interface BroadcastSource<T> {
  subscribe(callback: (data: T, info: ChannelPublishInfo) => any, options: { signal: AbortSignal }): () => void
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo) => any,
    options: { signal: AbortSignal },
  ): () => void
  readonly closed: Promise<void>
}

type Message<T> = { type: 'text'; data: T } | { type: 'binary'; data: Uint8Array }

/** Where a loop's queue counts what it holds: on a server, the ledger. A page's queue is the page's own memory. */
interface QueueMemory {
  admit(bytes: number): boolean
  hold(bytes: number): void
  letGo(bytes: number): void
}

/** What a queued message holds beyond its payload: its entry, its info and the promise its listener got, about 314 B
 *  measured. */
const QUEUED_MESSAGE_OVERHEAD_BYTES = 320

function channelMessages<T>(channel: MessageSource<T>, options?: { signal?: AbortSignal }) {
  return iterateMessages<Message<T>>(
    channel,
    (receive, registration) => {
      channel.listen((data) => receive({ type: 'text', data }), registration)
      channel.listenBinary((data) => receive({ type: 'binary', data }), registration)
    },
    options,
  )
}

/** A broadcast's publishes don't wait for its listeners, so what its loop falls behind by is held here: past
 *  `PUBLISH_HOLD_BYTES` beyond the next message, or what `memory` admits, the loop ends with `ChannelOverflowError`. */
function broadcastMessages<T>(broadcast: BroadcastSource<T>, options?: { signal?: AbortSignal }, memory?: QueueMemory) {
  return iterateMessages<Message<T> & { info: ChannelPublishInfo }>(
    broadcast,
    (receive, registration) => {
      broadcast.subscribe(
        (data, info) => receive({ type: 'text', data, info }, utf8ByteLength(stringify(data))),
        registration,
      )
      broadcast.subscribeBinary((data, info) => receive({ type: 'binary', data, info }, data.byteLength), registration)
    },
    options,
    PUBLISH_HOLD_BYTES,
    memory,
  )
}

/** Without a `budget`, what the loop holds is bounded by its source: a channel's listener promise holds its credit. */
async function* iterateMessages<M extends Message<unknown>>(
  source: { readonly closed: Promise<void> },
  subscribe: (
    receive: (message: M, bytes?: number) => Promise<void> | undefined,
    options: { signal: AbortSignal },
  ) => void,
  { signal }: { signal?: AbortSignal } = {},
  budget?: number,
  memory?: QueueMemory,
): AsyncIterableIterator<M> {
  const registrations = new AbortController()
  const queue = new Fifo<{ message: M; consumed: () => void; size: number }>()
  let held = 0
  let wake: (() => void) | undefined
  let ended = false
  let error: unknown
  let behindCheck: ReturnType<typeof setTimeout> | undefined
  const letGo = (size: number) => {
    held -= size
    memory?.letGo(size)
  }
  const release = () => {
    for (let item = queue.shift(); item; item = queue.shift()) {
      letGo(item.size)
      item.consumed()
    }
  }
  // A failure drops what is queued: the loop throws at its next step. The first failure stays, and a clean end can't
  // erase it.
  const finish = (err?: unknown) => {
    if (error !== undefined || (ended && err === undefined)) return
    ended = true
    error = err
    registrations.abort()
    if (err !== undefined) release()
    wake?.()
  }
  const abort = () => finish(signal!.reason)
  // The next message waits only for the loop's current one to finish, so what counts is what waits beyond it.
  const isBehind = () => held - (queue.peek()?.size ?? 0) > budget!
  // A burst delivered in one task reaches the loop only once that task ends, so the loop is behind where it still
  // holds more than `budget` then, whether or not its source closed meanwhile.
  const checkBehind = () => {
    behindCheck = undefined
    if (!isBehind()) return
    finish(new ChannelOverflowError(`messages() ended: its loop fell over ${budget} bytes behind the publishes`))
  }
  const receive = (message: M, bytes = 0) => {
    if (registrations.signal.aborted) return
    const size = budget ? bytes + QUEUED_MESSAGE_OVERHEAD_BYTES : 0
    if (memory && !memory.admit(size)) {
      finish(new ChannelOverflowError('messages() ended: config.channel.maxMemory is held, and its loop is behind'))
      return
    }
    memory?.hold(size)
    held += size
    return new Promise<void>((consumed) => {
      queue.push({ message, consumed, size })
      if (budget && isBehind()) behindCheck ??= setTimeout(checkBehind, 0)
      wake?.()
    })
  }
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) abort()
  const unobserve = observeClose(source.closed, finish)
  try {
    subscribe(receive, { signal: registrations.signal })
    while (true) {
      if (error !== undefined) throw error
      const next = queue.shift()
      if (!next) {
        if (ended) return
        await new Promise<void>((resolve) => {
          wake = resolve
        })
        continue
      }
      letGo(next.size)
      try {
        yield next.message
      } finally {
        next.consumed()
      }
    }
  } finally {
    unobserve()
    registrations.abort()
    clearTimeout(behindCheck)
    signal?.removeEventListener('abort', abort)
    release()
  }
}

// A pending close promise keeps its callbacks; their scope must release the iterator when it returns.
function observeClose(closed: Promise<void>, callback?: (err?: unknown) => void): () => void {
  void closed.then(
    () => callback?.(),
    (err) => callback?.(err),
  )
  return () => {
    callback = undefined
  }
}
