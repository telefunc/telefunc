export { channelMessages }

import { Fifo } from '../../utils/Fifo.js'

interface MessageSource<T> {
  listen(callback: (data: T) => any, options: { signal: AbortSignal }): () => void
  listenBinary(callback: (data: Uint8Array) => any, options: { signal: AbortSignal }): () => void
  readonly closed: Promise<void>
}

type Message<T> = { type: 'text'; data: T } | { type: 'binary'; data: Uint8Array }

async function* channelMessages<T>(
  channel: MessageSource<T>,
  { signal }: { signal?: AbortSignal } = {},
): AsyncIterableIterator<Message<T>> {
  const registrations = new AbortController()
  const queue = new Fifo<{ message: Message<T>; consumed: () => void }>()
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
  const receive = (message: Message<T>) => {
    if (registrations.signal.aborted) return
    return new Promise<void>((consumed) => {
      queue.push({ message, consumed })
      wake?.()
    })
  }
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) abort()
  channel.listen((data) => receive({ type: 'text', data }), { signal: registrations.signal })
  channel.listenBinary((data) => receive({ type: 'binary', data }), {
    signal: registrations.signal,
  })
  const unobserve = observeClose(channel.closed, finish)
  try {
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
