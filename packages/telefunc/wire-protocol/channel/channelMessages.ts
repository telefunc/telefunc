export { channelMessages }

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
  const queue: Array<{ message: Message<T>; consumed: () => void }> = []
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
  const receive = (message: Message<T>) =>
    new Promise<void>((consumed) => {
      queue.push({ message, consumed })
      wake?.()
    })
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) abort()
  channel.listen((data) => receive({ type: 'text', data }), { signal: registrations.signal })
  channel.listenBinary((data) => receive({ type: 'binary', data: new Uint8Array(data) }), {
    signal: registrations.signal,
  })
  // The close promise outlives the registrations and doesn't retain the iterator after it exits.
  let onClose: ((err?: unknown) => void) | undefined = finish
  void channel.closed.then(
    () => onClose?.(),
    (err) => onClose?.(err),
  )
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
    onClose = undefined
    registrations.abort()
    signal?.removeEventListener('abort', abort)
    for (const item of queue) item.consumed()
  }
}
