export { ChannelChunkReader }

import { parse } from '@brillout/json-serializer/parse'
import { textDecoder } from './frame.js'
import { CHANNEL_PUMP_TAG_ERROR } from './constants.js'
import { isObject } from '../utils/isObject.js'
import { assert } from '../utils/assert.js'

/** Minimal channel interface — both ClientChannel and ServerChannel satisfy this. */
type ChannelSurface = {
  listenBinary(cb: (data: Uint8Array) => Promise<unknown>): void
  onClose(cb: (err?: Error) => void): void
  close(): void
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
  private queue: Array<{ frame: Uint8Array<ArrayBuffer>; onConsumed: () => void }> = []
  private readHead = 0
  private wake: (() => void) | null = null
  private closed = false
  private closeError: Error | null = null
  private readonly channel: ChannelSurface
  private readonly throwError?: (errorPayload: Record<string, unknown>) => never

  private constructor(channel: ChannelSurface, throwError?: (errorPayload: Record<string, unknown>) => never) {
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
    this.channel.close()
    this.wake?.()
    this.wake = null
  }
}
