export { ChannelStreamSource }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from './frame.js'
import { CHANNEL_PUMP_TAG_ERROR } from './constants.js'
import { isObject } from '../utils/isObject.js'
import { assert } from '../utils/assert.js'
import { readChunksToBytes } from './readChunksToBytes.js'
import type { Channel } from './server/channel.js'
import type { AbortError } from '../shared/Abort.js'
import type { StreamSource } from './types.js'

type StreamSourceChannel = Pick<Channel, 'listenBinary' | 'onClose' | 'close' | 'abort'>

/**
 * Receives tagged binary frames from a channel with credit-based backpressure.
 *
 * Each binary frame is tagged: `[TAG_DATA (0x00)][payload]` or
 * `[TAG_ERROR (0x01)][JSON error payload]`.
 *
 * Used by both directions:
 * - Client response: receives chunks from server-side channel pump
 * - Server request: receives chunks from client-side channel pump
 *
 * Raw frames are queued as-is; tags are parsed at dequeue time in readNextChunk.
 * This naturally preserves frame ordering — all data before an error is delivered
 * before the error is thrown.
 *
 * The optional `throwError` callback handles error frames with caller-specific
 * formatting (e.g. telefunc abort/bug errors). If omitted, a generic Error is thrown.
 *
 * Internally uses a read-head index for O(1) dequeues with periodic compaction.
 */
class ChannelStreamSource {
  private queue: Array<{ frame: Uint8Array<ArrayBuffer>; onConsumed: () => void }> = []
  private readHead = 0
  private wake: (() => void) | null = null
  private closed = false
  private closeError: Error | null = null
  private cancelled = false
  private readonly channel: StreamSourceChannel
  private readonly throwError?: (errorPayload: Record<string, unknown>) => never

  private constructor(channel: StreamSourceChannel, throwError?: (errorPayload: Record<string, unknown>) => never) {
    this.channel = channel
    this.throwError = throwError

    channel.listenBinary((frame) => {
      return new Promise<void>((resolve) => {
        this.queue.push({ frame: frame as Uint8Array<ArrayBuffer>, onConsumed: resolve })
        this.wake?.()
        this.wake = null
      })
    })

    channel.onClose((err) => {
      this.closed = true
      this.closeError = err ?? null
      this.wake?.()
      this.wake = null
    })
  }

  /** Returns a unified `StreamSource` — chunk-pull, byte-buffer, and lazy stream views
   *  over the same channel. `throwError` is called when an error frame is dequeued;
   *  if omitted, error frames throw a generic Error. */
  static create(
    channel: StreamSourceChannel,
    throwError?: (errorPayload: Record<string, unknown>) => never,
  ): StreamSource {
    const reader = new ChannelStreamSource(channel, throwError)
    const readNextChunk = () => reader.readNextChunk()
    const cancel = () => reader.cancel()
    const abort = (e: AbortError) => channel.abort(e.abortValue, e.message)
    const isCancelled = () => reader.cancelled
    return {
      readNextChunk,
      bytes: (opts) => readChunksToBytes(readNextChunk, isCancelled, opts?.expectedSize, opts?.onChunk),
      // No pre-fetching — pull is only called when the consumer actually reads.
      // This ensures window updates reflect true application consumption, not
      // data sitting in the ReadableStream's internal buffer.
      stream: (opts) => {
        let received = 0
        return new ReadableStream<Uint8Array<ArrayBuffer>>(
          {
            pull: async (controller) => {
              try {
                const chunk = await readNextChunk()
                if (chunk === null) {
                  if (reader.cancelled) throw new Error('Stream cancelled before all bytes were received')
                  if (opts?.expectedSize !== undefined && received !== opts.expectedSize) {
                    throw new Error(`Stream truncated — received ${received} of ${opts.expectedSize} expected bytes`)
                  }
                  controller.close()
                  return
                }
                received += chunk.byteLength
                opts?.onChunk?.(chunk.byteLength)
                controller.enqueue(chunk)
              } catch (err) {
                reader.cancel()
                controller.error(err)
              }
            },
            cancel,
          },
          { highWaterMark: 0 },
        )
      },
      cancel,
      abort,
    }
  }

  private async readNextChunk(): Promise<Uint8Array<ArrayBuffer> | null> {
    while (this.readHead >= this.queue.length) {
      if (this.closed) {
        if (this.closeError) throw this.closeError
        return null
      }
      await new Promise<void>((r) => {
        this.wake = r
      })
    }
    const entry = this.queue[this.readHead]!
    this.queue[this.readHead] = undefined! // release reference
    this.readHead++
    // Signal the channel that this frame has been consumed — drives window updates.
    entry.onConsumed()
    // Compact when half the array is consumed to avoid unbounded growth.
    if (this.readHead > 16 && this.readHead >= this.queue.length >>> 1) {
      this.queue = this.queue.slice(this.readHead)
      this.readHead = 0
    }
    // Tag is the first byte: 0x00 = data, 0x01 = error.
    const tag = entry.frame[0]
    const payload = entry.frame.subarray(1)
    if (tag === CHANNEL_PUMP_TAG_ERROR) {
      const errorPayload: unknown = parse(textDecoder.decode(payload))
      assert(isObject(errorPayload))
      if (this.throwError) this.throwError(errorPayload)
      throw new Error('Streaming error frame received')
    }
    return payload
  }

  private cancel(): void {
    this.cancelled = true
    this.channel.close()
    this.wake?.()
    this.wake = null
  }
}
