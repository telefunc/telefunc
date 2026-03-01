export { parseStreamingResponseBody }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { decodeU32, decodeIndexedFrame, concat } from '../../frame.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../constants.js'
import { createStreamingReviver } from './registry.js'
import { throwCancelError, throwAbortError, throwBugError } from '../../../client/remoteTelefunctionCall/errors.js'

// ===== Streaming response parsing =====

async function parseStreamingResponseBody(
  response: Response,
  callContext: {
    telefunctionName: string
    telefuncFilePath: string
    abortController: AbortController
  },
): Promise<{ ret: unknown }> {
  assert(response.body)
  const reader = response.body.getReader()
  const streamReader = new StreamReader(reader, callContext)

  const cancelUpstream = () => {
    streamReader.cancelled = true
    reader.cancel()
  }

  const demuxer = new FrameDemuxer(streamReader, cancelUpstream)

  // Read metadata header
  const metaLen = await streamReader.readU32()
  const metaBytes = await streamReader.readExact(metaLen)
  const metaText = new TextDecoder().decode(metaBytes)

  const getChunkReader = (index: number) => {
    demuxer.registerConsumer()
    return () => demuxer.readNextChunkForIndex(index)
  }

  const getCancelIndex = (index: number) => () => demuxer.cancelIndex(index)

  const { reviver } = createStreamingReviver(getChunkReader, getCancelIndex)

  const parsed: unknown = parse(metaText, { reviver })
  assert(isObject(parsed) && 'ret' in parsed)

  return { ret: parsed.ret }
}

// ===== Frame demultiplexer =====

/** Demultiplexes indexed frames from a single HTTP stream to multiple consumers.
 *
 *  Best-effort backpressure: stops reading when an idle consumer's buffer hits
 *  MAX_BUFFER_PER_INDEX, resumes when drained. Active consumers (registered as
 *  waiters) receive frames via direct dispatch — zero buffering, zero delay.
 *
 *  Note: an index's buffer may briefly exceed MAX_BUFFER_PER_INDEX. This happens when
 *  another consumer's drain restarts the read loop, and the next frame on the wire
 *  is for the already-full index. Each restart adds at most 1 frame of overshoot.
 *  This is the unavoidable cost of multiplexing over a single stream — we can't
 *  peek at the next frame's index without reading it.
 *
 *  Cancellation follows .tee() semantics: cancelling one consumer marks its index
 *  as cancelled and drops future frames for it. Other consumers continue normally.
 *  The upstream reader is only cancelled when ALL consumers are cancelled. */
class FrameDemuxer {
  private static readonly MAX_BUFFER_BYTES_PER_INDEX = 1024 * 1024 // 1 MB
  private streamReader: StreamReader
  private pendingFrames = new Map<number, Uint8Array[]>()
  private pendingBytes = new Map<number, number>()
  private indexWaiters = new Map<number, { resolve: (v: Uint8Array | null) => void; reject: (e: unknown) => void }>()
  private reading = false
  private ended = false
  private streamError: unknown = null
  private cancelledIndices = new Set<number>()
  private doneIndices = new Set<number>()
  private totalConsumers = 0
  private cancelUpstream: (() => void) | null = null

  constructor(streamReader: StreamReader, cancelUpstream: () => void) {
    this.streamReader = streamReader
    this.cancelUpstream = cancelUpstream
  }

  registerConsumer() {
    this.totalConsumers++
  }

  /** Cancel the given index. Follows .tee() semantics:
   *  drops its buffered/future frames, resolves any pending waiter with null.
   *  Upstream is cancelled only when all consumers are cancelled. */
  cancelIndex(index: number): void {
    if (this.cancelledIndices.has(index)) return
    this.cancelledIndices.add(index)
    // Drop buffered frames for this index
    this.pendingFrames.delete(index)
    // Resolve any pending waiter with null (stream ended for this consumer)
    const waiter = this.indexWaiters.get(index)
    if (waiter) {
      this.indexWaiters.delete(index)
      waiter.resolve(null)
    }
    // Cancel upstream when all consumers are cancelled
    if (this.cancelledIndices.size >= this.totalConsumers) {
      this.cancelUpstream?.()
      this.cancelUpstream = null
    }
  }

  async readNextChunkForIndex(index: number): Promise<Uint8Array | null> {
    if (this.cancelledIndices.has(index)) return null
    if (this.streamError) throw this.streamError

    const pending = this.pendingFrames.get(index)
    if (pending && pending.length > 0) {
      const frame = pending.shift()!
      this.pendingBytes.set(index, (this.pendingBytes.get(index) ?? 0) - frame.byteLength)
      this.ensureReading()
      return frame
    }
    if (this.doneIndices.has(index)) return null
    if (this.ended) return null

    let resolve: (v: Uint8Array | null) => void
    let reject: (e: unknown) => void
    const promise = new Promise<Uint8Array | null>((res, rej) => {
      resolve = res
      reject = rej
    })
    this.indexWaiters.set(index, { resolve: resolve!, reject: reject! })
    this.ensureReading()
    return promise
  }

  private async ensureReading() {
    if (this.reading) return
    this.reading = true
    try {
      while (this.indexWaiters.size > 0) {
        const frame = await this.streamReader.readNextFrame()
        if (frame === null) {
          this.ended = true
          for (const [, w] of this.indexWaiters) w.resolve(null)
          this.indexWaiters.clear()
          return
        }

        // Drop frames for cancelled indices
        if (this.cancelledIndices.has(frame.index)) continue

        const waiter = this.indexWaiters.get(frame.index)

        // Empty payload = per-index "done" signal
        if (frame.payload.length === 0) {
          this.doneIndices.add(frame.index)
          if (waiter) {
            this.indexWaiters.delete(frame.index)
            waiter.resolve(null)
          }
          continue
        }

        // Direct dispatch — no buffering, no delay
        if (waiter) {
          this.indexWaiters.delete(frame.index)
          waiter.resolve(frame.payload)
          continue
        }

        // No consumer waiting — buffer it
        const pending = this.pendingFrames.get(frame.index)
        if (pending) pending.push(frame.payload)
        else this.pendingFrames.set(frame.index, [frame.payload])
        const newBytes = (this.pendingBytes.get(frame.index) ?? 0) + frame.payload.byteLength
        this.pendingBytes.set(frame.index, newBytes)

        // Per-index backpressure: stop reading when this index's buffer exceeds 1 MB.
        // The loop restarts when the consumer drains via readNextChunkForIndex().
        if (newBytes >= FrameDemuxer.MAX_BUFFER_BYTES_PER_INDEX) break
      }
    } catch (err) {
      this.streamError ??= err
      for (const [, w] of this.indexWaiters) w.reject(err)
      this.indexWaiters.clear()
    } finally {
      this.reading = false
    }
  }
}

// ===== Client StreamReader =====

const EMPTY = new Uint8Array(0)

/** Buffered reader for the HTTP response body stream.
 *
 *  readExact: read N bytes (low-level byte I/O with buffering).
 *  readNextFrame: read one indexed frame (wire protocol + error handling). */
class StreamReader {
  private reader: ReadableStreamDefaultReader<Uint8Array>
  private callContext: {
    telefunctionName: string
    telefuncFilePath: string
    abortController: AbortController
  }
  private buffer: Uint8Array = EMPTY
  cancelled = false

  constructor(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    callContext: {
      telefunctionName: string
      telefuncFilePath: string
      abortController: AbortController
    },
  ) {
    this.reader = reader
    this.callContext = callContext

    // When the fetch is aborted, cancel the reader first to prevent the browser
    // from generating a spurious unhandled "BodyStreamBuffer was aborted" rejection.
    callContext.abortController.signal.addEventListener('abort', () => reader.cancel(), { once: true })
  }

  async readU32(): Promise<number> {
    const buf = await this.readExact(4) // sizeof uint32
    return this.cancelled ? 0 : decodeU32(buf)
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.length < n) {
      let done: boolean
      let value: Uint8Array | undefined
      let readError: unknown
      try {
        ;({ done, value } = await this.reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (this.callContext.abortController.signal.aborted) throwCancelError()
        if (this.cancelled) return EMPTY
        throw readError ?? new Error('Connection lost — the server closed the stream before all data was received.')
      }
      this.buffer = this.buffer.length === 0 ? value! : concat(this.buffer, value!)
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }

  /** Read the next indexed frame from the wire.
   *  Returns { index, payload } or null on terminator. Throws on error frames. */
  async readNextFrame(): Promise<{ index: number; payload: Uint8Array } | null> {
    const len = await this.readU32()
    if (this.cancelled) return null
    if (len === 0) return null
    if (len === STREAMING_ERROR_FRAME_MARKER) {
      // Error frame: [ERROR_MARKER][u32 payload_len][payload_bytes]
      const errorLen = await this.readU32()
      const errorBytes = await this.readExact(errorLen)
      const errorPayload: unknown = parse(new TextDecoder().decode(errorBytes))
      assert(isObject(errorPayload) && 'type' in errorPayload)
      if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT) {
        assert('abortValue' in errorPayload)
        throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, errorPayload.abortValue)
      }
      throwBugError()
    }
    const frameData = await this.readExact(len)
    if (this.cancelled) return null
    return decodeIndexedFrame(frameData)
  }
}
