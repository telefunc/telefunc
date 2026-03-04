export { ChannelStreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { concat } from '../../frame.js'
import { throwCancelError } from '../../../client/remoteTelefunctionCall/errors.js'
import { ClientChannel } from '../../../client/channel.js'

const EMPTY = new Uint8Array(0)

/** WS backpressure watermarks.
 *
 *  HIGH (2 MB) — send PAUSE when buffer exceeds this.
 *  LOW (512 KB) — send RESUME when buffer drains below this.
 *
 *  Oscillation occurs when (HIGH - LOW) < bytes in flight during one RTT.
 *  Worst case: 200ms RTT, server at 5 MB/s → ~1 MB in flight.
 *  Gap = 1.5 MB > 1 MB → no oscillation.
 *
 *  LOW = 512 KB (not 0): consumer stays fed while RESUME propagates back. */
const HIGH = 2 * 1024 * 1024
const LOW = 512 * 1024

/** WS transport reader — receives binary frames pushed by the server
 *  via WebSocket, with watermark-based backpressure signaling.
 *
 *  The WebSocket is push-based (data arrives via `listenBinary` callback)
 *  but the frame protocol expects pull-based `readExact(n)` calls.
 *  A single `wake` resolver bridges the gap: `readExact` parks on a promise,
 *  `listenBinary` resolves it when new data arrives. */
class ChannelStreamReader extends BaseStreamReader {
  private buffer: Uint8Array = EMPTY
  private wake: (() => void) | null = null
  private paused = false
  private channel: ClientChannel

  constructor(
    channel: ClientChannel,
    callContext: {
      telefunctionName: string
      telefuncFilePath: string
      abortController: AbortController
    },
  ) {
    super(callContext)
    this.channel = channel
    // Abort signal: wake blocked readExact so it can check cancelled/aborted.
    callContext.abortController.signal.addEventListener('abort', () => this.cancel(), { once: true })

    channel.listenBinary((frame: Uint8Array) => {
      this.buffer = concat(this.buffer, frame)
      this.wake?.()
      this.wake = null
      if (!this.paused && this.buffer.byteLength >= HIGH) {
        this.paused = true
        this.channel.send({ p: 1 })
      }
    })
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.length < n) {
      if (this.callContext.abortController.signal.aborted) throwCancelError()
      if (this.cancelled) return EMPTY
      // Park until listenBinary fires with new data (push-to-pull bridge).
      await new Promise<void>((r) => {
        this.wake = r
      })
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = this.buffer.length > n ? this.buffer.subarray(n) : EMPTY
    if (this.paused && this.buffer.byteLength < LOW) {
      this.paused = false
      this.channel.send({ p: 0 })
    }
    return result
  }

  cancel(): void {
    this.cancelled = true
    this.channel.close()
    // Wake blocked readExact so it re-enters the loop and returns EMPTY.
    this.wake?.()
    this.wake = null
  }
}
