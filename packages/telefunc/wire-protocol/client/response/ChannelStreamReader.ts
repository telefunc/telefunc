export { ChannelStreamReader }

import { BaseStreamReader } from './BaseStreamReader.js'
import { concat } from '../../frame.js'
import { throwAbortError } from '../../../client/remoteTelefunctionCall/errors.js'
import { ClientChannel } from '../channel.js'

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

/** Transport reader — receives binary frames pushed by the server
 *  with watermark-based backpressure signaling.
 *  Uses `channel._pause()` / `channel._resume()` when the transport supports it.
 *  SSE currently leaves those hooks as no-ops. */
class ChannelStreamReader extends BaseStreamReader {
  private buffer: Uint8Array<ArrayBuffer> = EMPTY
  private wake: (() => void) | null = null
  private paused = false
  private channel: ClientChannel
  private transportClosed = false
  private transportCloseError: Error | null = null

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
    callContext.abortController.signal.addEventListener('abort', () => this.cancel(), { once: true })
    channel.onClose((err) => {
      this.transportClosed = true
      this.transportCloseError = err ?? null
      this.wake?.()
      this.wake = null
    })

    channel.listenBinary((frame) => {
      this.buffer = concat(this.buffer, frame)
      this.wake?.()
      this.wake = null
      if (!this.paused && this.buffer.byteLength >= HIGH) {
        this.paused = true
        this.channel._pause()
      }
    })
  }

  async readExact(n: number): Promise<Uint8Array<ArrayBuffer>> {
    while (this.buffer.length < n) {
      if (this.callContext.abortController.signal.aborted) {
        throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, undefined)
      }
      if (this.cancelled) return EMPTY
      if (this.transportClosed && this.buffer.length === 0) {
        if (this.transportCloseError) throw this.transportCloseError
        return EMPTY
      }
      await new Promise<void>((r) => {
        this.wake = r
      })
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = this.buffer.length > n ? this.buffer.subarray(n) : EMPTY
    if (this.paused && this.buffer.byteLength < LOW) {
      this.paused = false
      this.channel._resume()
    }
    return result
  }

  cancel(): void {
    this.cancelled = true
    this.channel.close()
    this.wake?.()
    this.wake = null
  }
}
