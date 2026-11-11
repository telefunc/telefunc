export { ChannelStreamSource }

import { parse } from '@brillout/json-serializer/parse'
import { concatAll, textDecoder } from '../wire/envelope.js'
import { CHANNEL_PUMP_TAG_END, CHANNEL_PUMP_TAG_ERROR, CHANNEL_PUMP_TAG_PART } from '../constants.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { isObject } from '../../utils/isObject.js'
import { assert } from '../../utils/assert.js'
import { createReadableChunkStream } from './createReadableChunkStream.js'
import { readChunksToBytes } from './readChunksToBytes.js'
import { Fifo } from '../../utils/Fifo.js'
import type { Channel } from '../channel/server/ServerChannel.js'
import type { AbortError } from '../../shared/Abort.js'
import type { StreamSource } from './types.js'

type StreamSourceChannel = Pick<Channel, 'listenBinary' | 'onClose' | 'close' | 'abort'>

/**
 * Receives tagged binary frames from a channel with credit-based backpressure.
 *
 * Each binary frame is tagged: `[TAG_DATA (0x00)][payload]`, `[TAG_PART (0x03)][payload]` where more of the chunk
 * follows, `[TAG_ERROR (0x01)][JSON error payload]`, or `[TAG_END (0x02)]` at the producer's end.
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
 */
class ChannelStreamSource {
  private readonly queue = new Fifo<{ frame: Uint8Array<ArrayBuffer>; onConsumed: () => void }>()
  private wake: (() => void) | null = null
  private closed = false
  private closeError: Error | null = null
  private cancelled = false
  private ended = false
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
      bytes: (opts) => readChunksToBytes(readNextChunk, isCancelled, opts),
      // No pre-fetching — pull is only called when the consumer actually reads.
      // This ensures window updates reflect true application consumption, not
      // data sitting in the ReadableStream's internal buffer.
      stream: (opts) =>
        createReadableChunkStream({
          readNextChunk,
          cancel,
          isCancelled,
          readOptions: opts,
          highWaterMark: 0,
        }),
      cancel,
      abort,
    }
  }

  /** The producer's next chunk, its parts joined: a value's reviver parses a chunk as one serialized value. */
  private async readNextChunk(): Promise<Uint8Array<ArrayBuffer> | null> {
    // Local, so a read that is cancelled, fails or ends keeps none of the parts it read.
    const parts: Uint8Array<ArrayBuffer>[] = []
    while (true) {
      const frame = await this.readNextFrame()
      if (frame === null) return null
      // Tag is the first byte: 0x00 = data, 0x01 = error, 0x02 = end, 0x03 = a part with more to follow.
      const tag = frame[0]
      if (tag === CHANNEL_PUMP_TAG_END) {
        this.ended = true
        return null
      }
      const payload = frame.subarray(1)
      if (tag === CHANNEL_PUMP_TAG_ERROR) {
        const errorPayload: unknown = parse(textDecoder.decode(payload))
        assert(isObject(errorPayload))
        if (this.throwError) this.throwError(errorPayload)
        throw new Error('Streaming error frame received')
      }
      if (tag === CHANNEL_PUMP_TAG_PART) {
        parts.push(payload)
        continue
      }
      if (parts.length === 0) return payload
      return concatAll([...parts, payload])
    }
  }

  private async readNextFrame(): Promise<Uint8Array<ArrayBuffer> | null> {
    while (this.queue.length === 0) {
      if (this.ended) return null
      if (this.closed) return this.endOfClosed()
      await new Promise<void>((r) => {
        this.wake = r
      })
    }
    const entry = this.queue.shift()!
    // Signal the channel that this frame has been consumed — drives window updates.
    entry.onConsumed()
    return entry.frame
  }

  /** A read of the stream once its channel closed with nothing left queued: the end only if the reader cancelled. */
  private endOfClosed(): null {
    if (this.closeError) throw this.closeError
    if (!this.cancelled) throw new NetworkError('Channel closed before the end of its stream', true)
    return null
  }

  private cancel(): void {
    this.cancelled = true
    this.channel.close()
    this.wake?.()
    this.wake = null
  }
}
