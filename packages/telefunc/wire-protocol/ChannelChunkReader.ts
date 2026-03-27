export { ChannelChunkReader }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from './frame.js'
import { CHANNEL_PUMP_TAG_ERROR, CREDIT_WINDOW_BYTES, WINDOW_UPDATE_THRESHOLD_BYTES } from './constants.js'
import { isObject } from '../utils/isObject.js'
import { assert } from '../utils/assert.js'

/** Minimal channel interface — both ClientChannel and ServerChannel satisfy this.
 *
 *  `_sendWindowUpdate(bytes)` advertises free buffer space to the peer. */
type ChannelSurface = {
  listenBinary(cb: (data: Uint8Array) => void): void
  onClose(cb: (err?: Error) => void): void
  close(): void
  _sendWindowUpdate(bytes: number): void
}

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
class ChannelChunkReader {
  private queue: Uint8Array<ArrayBuffer>[] = []
  private queueBytes = 0
  private readHead = 0
  private wake: (() => void) | null = null
  private closed = false
  private closeError: Error | null = null
  private readonly channel: ChannelSurface
  private readonly throwError?: (errorPayload: Record<string, unknown>) => never
  private consumedSinceLastWindowUpdate = 0

  private constructor(channel: ChannelSurface, throwError?: (errorPayload: Record<string, unknown>) => never) {
    this.channel = channel
    this.throwError = throwError

    channel.listenBinary((frame) => {
      this.queue.push(frame as Uint8Array<ArrayBuffer>)
      this.queueBytes += frame.byteLength
      this.wake?.()
      this.wake = null
    })

    channel.onClose((err) => {
      this.closed = true
      this.closeError = err ?? null
      this.wake?.()
      this.wake = null
    })
  }

  /**
   * Create a chunk reader returning `{ readNextChunk, cancel }`.
   *
   * `throwError` is called when an error frame is dequeued. If omitted,
   * error frames throw a generic Error.
   */
  static create(channel: ChannelSurface, throwError?: (errorPayload: Record<string, unknown>) => never) {
    const reader = new ChannelChunkReader(channel, throwError)
    return {
      readNextChunk: () => reader.readNextChunk(),
      cancel: () => reader.cancel(),
    }
  }

  /** Create a pull-based ReadableStream backed by this reader. */
  static toReadableStream(channel: ChannelSurface, throwError?: (errorPayload: Record<string, unknown>) => never) {
    const reader = new ChannelChunkReader(channel, throwError)
    return new ReadableStream<Uint8Array<ArrayBuffer>>(
      {
        pull: async (controller) => {
          const chunk = await reader.readNextChunk()
          if (chunk === null) controller.close()
          else controller.enqueue(chunk)
        },
        cancel: () => reader.cancel(),
      },
      // No pre-fetching — pull is only called when the consumer actually reads.
      // This ensures window updates reflect true application consumption, not
      // data sitting in the ReadableStream's internal buffer.
      { highWaterMark: 0 },
    )
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
    const frame = this.queue[this.readHead]!
    this.queue[this.readHead] = undefined! // release reference
    this.readHead++
    this.queueBytes -= frame.byteLength
    // Compact when half the array is consumed to avoid unbounded growth.
    if (this.readHead > 16 && this.readHead >= this.queue.length >>> 1) {
      this.queue = this.queue.slice(this.readHead)
      this.readHead = 0
    }
    // Window-based flow control: advertise free buffer space so the sender can unblock.
    // Debounced to 10ms so rapid consumption coalesces into one update.
    this.consumedSinceLastWindowUpdate += frame.byteLength
    if (this.consumedSinceLastWindowUpdate >= WINDOW_UPDATE_THRESHOLD_BYTES) {
      this.consumedSinceLastWindowUpdate = 0
      this.channel._sendWindowUpdate(CREDIT_WINDOW_BYTES - this.queueBytes)
    }
    // Tag is the first byte: 0x00 = data, 0x01 = error.
    const tag = frame[0]
    const payload = frame.subarray(1)
    if (tag === CHANNEL_PUMP_TAG_ERROR) {
      const errorPayload: unknown = parse(textDecoder.decode(payload))
      assert(isObject(errorPayload))
      if (this.throwError) this.throwError(errorPayload)
      throw new Error('Streaming error frame received')
    }
    return payload
  }

  private cancel(): void {
    this.channel.close()
    this.wake?.()
    this.wake = null
  }
}
