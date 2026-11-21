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
 * generator, no `await` per chunk.
 *
 * Backpressure: `push` returns whether the queue has room once the chunk is in
 * it (`desiredSize > 0`), which is only while a read was waiting and took the
 * chunk, and `false` once closed. `onPull` runs when the consumer wants more:
 * a producer pacing on `push` (the server's SSE stream, an inline streaming
 * response) waits for it after a `false`. One bounded otherwise (the page's
 * SSE upload, by channel credit) passes no `onPull`, ignores the answer and
 * reads `bufferedAmount`.
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
