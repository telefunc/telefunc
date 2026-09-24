export { createPushReadableStream }
export type { PushReadableStream }

/**
 * Push-fed Web `ReadableStream` for streaming bodies on the Web side
 * (client SSE upstream, any cross-runtime use that hands a body to fetch /
 * Response). Mirrors `createPushReadable`: the returned object IS-A
 * `ReadableStream`, with `push` / `close` / `isClosed` bolted on as the
 * producer surface — so callers don't have to choose between a wrapper
 * object and the underlying body.
 *
 * Producer calls `push(chunk)` / `close()` synchronously; each chunk lands
 * directly in the stream's internal queue via `controller.enqueue`.
 * Consumers (fetch upstream, `pipeTo`) drain the queue natively — no async
 * generator, no `await` per chunk, no `new ReadableStream({ pull })` adapter.
 *
 * Backpressure is opt-in via `onPull`:
 *   - omit it (SSE upstream, client→server stream-request): `push` always
 *     returns `true`; channel-level credit bounds the producer.
 *   - wire it (inline streaming response): `push` returns the consumer's
 *     `desiredSize > 0` (false = queue full); producer awaits a deferred
 *     between `push`-returns-false and the next `onPull`.
 *
 * `onCancel` fires once when the consumer cancels the stream (fetch
 * aborted, `pipeTo` destination errored, etc.). Producer-side `close()`
 * is a normal end and does *not* fire `onCancel`.
 */

type PushReadableStream<T = Uint8Array<ArrayBuffer>> = ReadableStream<T> & {
  push(chunk: T): boolean
  close(): void
  readonly isClosed: boolean
}

function createPushReadableStream<T = Uint8Array<ArrayBuffer>>(
  onCancel?: () => void,
  onPull?: () => void,
): PushReadableStream<T> {
  let controller!: ReadableStreamDefaultController<T>
  let closed = false
  const stream = new ReadableStream<T>({
    start: (c) => {
      controller = c
    },
    pull: () => {
      onPull?.()
    },
    cancel: () => {
      closed = true
      onCancel?.()
    },
  }) as PushReadableStream<T>
  stream.push = (chunk) => {
    if (closed) return false
    controller.enqueue(chunk)
    return (controller.desiredSize ?? 1) > 0
  }
  stream.close = () => {
    if (closed) return
    closed = true
    controller.close()
  }
  Object.defineProperty(stream, 'isClosed', { get: () => closed })
  return stream
}
