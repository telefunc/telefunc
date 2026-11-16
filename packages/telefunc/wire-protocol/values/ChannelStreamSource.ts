export { ChannelStreamSource }

import { parse } from '@brillout/json-serializer/parse'
import { concatAll, textDecoder } from '../wire/envelope.js'
import { CHANNEL_PUMP_TAG_END, CHANNEL_PUMP_TAG_ERROR, CHANNEL_PUMP_TAG_PART } from '../constants.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { ChannelOverflowError } from '../channel/channelErrors.js'
import { isObject } from '../../utils/isObject.js'
import { assert } from '../../utils/assert.js'
import { createReadableChunkStream } from './createReadableChunkStream.js'
import { readChunksToBytes } from './readChunksToBytes.js'
import { Fifo } from '../../utils/Fifo.js'
import type { Channel } from '../channel/server/ServerChannel.js'
import type { QueueMemory } from '../channel/channelMessages.js'
import type { AbortError } from '../../shared/Abort.js'
import type { StreamSource } from './types.js'

type StreamSourceChannel = Pick<Channel, 'listenBinary' | 'onClose' | 'close' | 'abort'>

type StreamSourceOptions = {
  /** Throws an error frame's payload as its caller's error (telefunc's abort or bug error); a generic Error without it. */
  throwError?: (errorPayload: Record<string, unknown>) => never
  /** Where the parts of an unfinished chunk count: on a server, the ledger; a page counts nothing. */
  memory?: QueueMemory
}

const CHUNK_OVERFLOW_TEXT =
  'Stream ended: a chunk of it passed the most for one channel, or config.channel.maxMemory is held; send smaller chunks'

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
 * Raw frames are queued as-is; tags are parsed at dequeue time in readNextFrame.
 * This naturally preserves frame ordering — all data before an error is delivered
 * before the error is thrown.
 */
class ChannelStreamSource {
  private readonly queue = new Fifo<{ frame: Uint8Array<ArrayBuffer>; onConsumed: () => void }>()
  private wake: (() => void) | null = null
  private closed = false
  private closeError: Error | null = null
  private cancelled = false
  private ended = false
  private readonly channel: StreamSourceChannel
  private readonly throwError: StreamSourceOptions['throwError']
  private readonly memory: StreamSourceOptions['memory']

  private constructor(channel: StreamSourceChannel, { throwError, memory }: StreamSourceOptions) {
    this.channel = channel
    this.throwError = throwError
    this.memory = memory

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
   *  over the same channel. */
  static create(channel: StreamSourceChannel, options: StreamSourceOptions = {}): StreamSource {
    const reader = new ChannelStreamSource(channel, options)
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

  /** The producer's next chunk, one sent in parts joined: a value's reviver parses a chunk as one serialized value. */
  private async readNextChunk(): Promise<Uint8Array<ArrayBuffer> | null> {
    const frame = await this.readNextFrame()
    if (frame === null) return null
    if (frame[0] === CHANNEL_PUMP_TAG_PART) return this.readChunkInParts(frame)
    return frame.subarray(1)
  }

  /** The chunk whose first part is `first`. Its parts left the channel's queue, their credit returned, so they count
   *  toward `memory` until this read returns or throws, and a part that doesn't fit ends the read. */
  private async readChunkInParts(first: Uint8Array<ArrayBuffer>): Promise<Uint8Array<ArrayBuffer> | null> {
    // Local, so a read that is cancelled, fails or ends keeps none of the parts it read.
    const parts: Uint8Array<ArrayBuffer>[] = []
    let held = 0
    try {
      for (let frame: Uint8Array<ArrayBuffer> | null = first; frame !== null; frame = await this.readNextFrame()) {
        parts.push(frame.subarray(1))
        if (frame[0] !== CHANNEL_PUMP_TAG_PART) return concatAll(parts)
        this.holdPart(frame.byteLength)
        held += frame.byteLength
      }
      return null
    } finally {
      this.memory?.letGo(held)
    }
  }

  /** Counts a part of `bytes` that this reader keeps, and throws where the server can't hold it. */
  private holdPart(bytes: number): void {
    if (!this.memory) return
    if (!this.memory.admit(bytes)) throw new ChannelOverflowError(CHUNK_OVERFLOW_TEXT)
    this.memory.hold(bytes)
  }

  /** The stream's next data frame, a chunk or a part of one: null at its end, and an error frame throws. */
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
    // Tag is the first byte: 0x00 = data, 0x01 = error, 0x02 = end, 0x03 = a part with more to follow.
    const tag = entry.frame[0]
    if (tag === CHANNEL_PUMP_TAG_END) {
      this.ended = true
      return null
    }
    if (tag === CHANNEL_PUMP_TAG_ERROR) {
      const errorPayload: unknown = parse(textDecoder.decode(entry.frame.subarray(1)))
      assert(isObject(errorPayload))
      if (this.throwError) this.throwError(errorPayload)
      throw new Error('Streaming error frame received')
    }
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
