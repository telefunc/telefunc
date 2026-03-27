export { PushToPullStream }

/**
 * Adapts a push-based producer to a pull-based ReadableStream with proper backpressure.
 *
 * - When the stream has capacity (`desiredSize > 0`), `push()` enqueues directly
 *   to the controller — zero latency.
 * - When backpressured (`desiredSize <= 0`), `push()` returns a promise that
 *   resolves when the consumer calls `pull()`, signalling capacity is available.
 * - `close()` cleanly closes the stream after any buffered data is drained.
 *
 * Used by:
 * - Server SSE downstream (response body)
 * - Client SSE upstream (persistent streaming POST body)
 */
class PushToPullStream<T = Uint8Array<ArrayBuffer>> {
  private controller: ReadableStreamDefaultController<T> | null = null
  private pullWaiters: Array<() => void> = []
  private closed = false
  readonly readable: ReadableStream<T>

  constructor(private readonly onCancel?: () => void) {
    this.readable = new ReadableStream<T>({
      start: (controller) => {
        this.controller = controller
      },
      pull: () => {
        const waiters = this.pullWaiters.splice(0)
        for (const waiter of waiters) waiter()
      },
      cancel: () => {
        this.closed = true
        this.controller = null
        const waiters = this.pullWaiters.splice(0)
        for (const waiter of waiters) waiter()
        this.onCancel?.()
      },
    })
  }

  /** Push data into the stream. Returns a promise when backpressured, void when immediate. */
  push(chunk: T): void | Promise<void> {
    if (this.closed || !this.controller) return
    if (this.controller.desiredSize !== null && this.controller.desiredSize > 0) {
      this.controller.enqueue(chunk)
      return
    }
    return this._pushWhenWritable(chunk)
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    this.controller?.close()
    this.controller = null
    const waiters = this.pullWaiters.splice(0)
    for (const waiter of waiters) waiter()
  }

  get isClosed(): boolean {
    return this.closed
  }

  private async _pushWhenWritable(chunk: T): Promise<void> {
    while (
      !this.closed &&
      this.controller !== null &&
      this.controller.desiredSize !== null &&
      this.controller.desiredSize <= 0
    ) {
      await new Promise<void>((resolve) => {
        this.pullWaiters.push(resolve)
      })
    }
    if (this.closed || !this.controller) return
    this.controller.enqueue(chunk)
  }
}
