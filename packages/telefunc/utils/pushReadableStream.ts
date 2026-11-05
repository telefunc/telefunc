export { createPushReadableStream }
export type { PushReadableStream }

/**
 * Push-fed Web `ReadableStream` for streaming bodies on the Web side
 * (client SSE upstream, any cross-runtime use that hands a body to fetch /
 * Response). Mirrors `createPushReadable`: `readable` is a plain
 * `ReadableStream`, the body handed on; `push` / `close` / `isClosed` stay
 * with the producer.
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

type PushReadableStream<T extends Uint8Array = Uint8Array<ArrayBuffer>> = {
  readonly readable: ReadableStream<T>
  push(chunk: T): boolean
  close(): void
  readonly isClosed: boolean
  /** Bytes pushed that the consumer hasn't read yet, while open. */
  readonly bufferedAmount: number
}

/** Counted in bytes so the queue reports them. A one-byte mark keeps `push`'s answer what a one-chunk mark gave: room
 *  only while nothing waits, since no chunk pushed is empty. */
const HIGH_WATER_MARK = 1

function createPushReadableStream<T extends Uint8Array = Uint8Array<ArrayBuffer>>(
  onCancel?: () => void,
  onPull?: () => void,
): PushReadableStream<T> {
  let controller!: ReadableStreamDefaultController<T>
  let closed = false
  const readable = new ReadableStream<T>(
    {
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
    },
    { highWaterMark: HIGH_WATER_MARK, size: (chunk) => chunk.byteLength },
  )
  return {
    readable,
    push: (chunk) => {
      if (closed) return false
      controller.enqueue(chunk)
      return (controller.desiredSize ?? 1) > 0
    },
    close: () => {
      if (closed) return
      closed = true
      controller.close()
    },
    get isClosed() {
      return closed
    },
    get bufferedAmount() {
      return closed ? 0 : HIGH_WATER_MARK - controller.desiredSize!
    },
  }
}
