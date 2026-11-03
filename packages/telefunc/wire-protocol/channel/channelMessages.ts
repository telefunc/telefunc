export { channelMessages, broadcastMessages }

import { Fifo } from '../../utils/Fifo.js'
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

function broadcastMessages<T>(broadcast: BroadcastSource<T>, options?: { signal?: AbortSignal }) {
  return iterateMessages<Message<T> & { info: ChannelPublishInfo }>(
    broadcast,
    (receive, registration) => {
      broadcast.subscribe((data, info) => receive({ type: 'text', data, info }), registration)
      broadcast.subscribeBinary((data, info) => receive({ type: 'binary', data, info }), registration)
    },
    options,
  )
}

async function* iterateMessages<M>(
  source: { readonly closed: Promise<void> },
  subscribe: (receive: (message: M) => Promise<void> | undefined, options: { signal: AbortSignal }) => void,
  { signal }: { signal?: AbortSignal } = {},
): AsyncIterableIterator<M> {
  const registrations = new AbortController()
  const queue = new Fifo<{ message: M; consumed: () => void }>()
  let wake: (() => void) | undefined
  let ended = false
  let error: unknown
  const finish = (err?: unknown) => {
    ended = true
    error = err
    registrations.abort()
    wake?.()
  }
  const abort = () => finish(signal!.reason)
  const receive = (message: M) => {
    if (registrations.signal.aborted) return
    return new Promise<void>((consumed) => {
      queue.push({ message, consumed })
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
      try {
        yield next.message
      } finally {
        next.consumed()
      }
    }
  } finally {
    unobserve()
    registrations.abort()
    signal?.removeEventListener('abort', abort)
    for (let item = queue.shift(); item; item = queue.shift()) item.consumed()
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
