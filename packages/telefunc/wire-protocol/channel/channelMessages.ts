export { channelMessages, broadcastMessages }
export type { MessageBudget }

import { stringify } from '@brillout/json-serializer/stringify'
import { Fifo } from '../../utils/Fifo.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
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

/** The most bytes of each kind a loop may fall behind by, as `config.channel.bufferLimit` and `bufferLimitBinary` bound
 *  what the server holds for a page. */
type MessageBudget = { text: number; binary: number }

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

/** A broadcast's publishes don't wait for its listeners, so what its loop falls behind by is held here, within
 *  `budget`: past it, the loop ends with `ChannelOverflowError`. */
function broadcastMessages<T>(
  broadcast: BroadcastSource<T>,
  budget: MessageBudget,
  options?: { signal?: AbortSignal },
) {
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
    budget,
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
  budget?: MessageBudget,
): AsyncIterableIterator<M> {
  const registrations = new AbortController()
  const queue = new Fifo<{ message: M; consumed: () => void; size: number }>()
  const held = { text: 0, binary: 0 }
  let wake: (() => void) | undefined
  let ended = false
  let error: unknown
  let behindCheck: ReturnType<typeof setTimeout> | undefined
  const release = () => {
    for (let item = queue.shift(); item; item = queue.shift()) item.consumed()
    held.text = held.binary = 0
  }
  // A failure drops what is queued: the loop throws at its next step.
  const finish = (err?: unknown) => {
    ended = true
    error = err
    registrations.abort()
    clearTimeout(behindCheck)
    if (err !== undefined) release()
    wake?.()
  }
  const abort = () => finish(signal!.reason)
  // A burst delivered in one task reaches the loop only once that task ends, so the loop is behind where it still
  // holds more than `budget` after it could run.
  const checkBehind = () => {
    behindCheck = undefined
    const type = held.text > budget!.text ? 'text' : held.binary > budget!.binary ? 'binary' : null
    if (type === null) return
    const behind = `${budget![type]} bytes of ${type} messages`
    finish(new ChannelOverflowError(`messages() ended: its loop fell behind the publishes by more than ${behind}`))
  }
  const receive = (message: M, bytes = 0) => {
    if (registrations.signal.aborted) return
    const size = budget ? bytes + QUEUED_MESSAGE_OVERHEAD_BYTES : 0
    held[message.type] += size
    if (budget && held[message.type] > budget[message.type]) behindCheck ??= setTimeout(checkBehind, 0)
    return new Promise<void>((consumed) => {
      queue.push({ message, consumed, size })
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
      held[next.message.type] -= next.size
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
