export { parseStreamingResponseBody, parseWsStreamingResponse, FrameDemuxer }
export { BaseStreamReader } from './BaseStreamReader.js'

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { createStreamingReviver } from './registry.js'
import { setAbortController } from '../../../client/abort.js'
import { BaseStreamReader } from './BaseStreamReader.js'
import { StreamReader } from './StreamReader.js'
import { SSEStreamReader } from './SSEStreamReader.js'
import { ChannelStreamReader } from './ChannelStreamReader.js'
import { ClientChannel } from '../../../client/channel.js'

// ===== Streaming response parsing =====

type CallContext = {
  telefunctionName: string
  telefuncFilePath: string
  abortController: AbortController
}

/** Shared core: wire a BaseStreamReader through FrameDemuxer + streaming reviver.
 *
 *  Both HTTP streaming and WS transports create a transport-specific reader,
 *  obtain the metadata text, then delegate here to reconstruct live values. */
function reviveStreamingResponse(
  streamReader: BaseStreamReader,
  metadataText: string,
  callContext: CallContext,
): { ret: unknown } {
  const demuxer = new FrameDemuxer(streamReader)

  const getChunkReader = (index: number) => {
    demuxer.registerConsumer()
    return () => demuxer.readNextChunkForIndex(index)
  }

  const getCancelIndex = (index: number) => () => demuxer.cancelIndex(index)

  const { reviver, channels } = createStreamingReviver(getChunkReader, getCancelIndex)

  const parsed: unknown = parse(metadataText, { reviver })
  assert(isObject(parsed) && 'ret' in parsed)

  // Close all revived channels when the call is aborted
  if (channels.length > 0) {
    callContext.abortController.signal.addEventListener(
      'abort',
      () => {
        for (const ch of channels) ch.close()
      },
      { once: true },
    )
  }

  const { ret } = parsed
  if (isObject(ret)) {
    setAbortController(ret, callContext.abortController)
  }

  return { ret }
}

/** HTTP streaming (binary / SSE): read metadata header from the stream,
 *  then revive streaming values via FrameDemuxer. */
async function parseStreamingResponseBody(response: Response, callContext: CallContext): Promise<{ ret: unknown }> {
  assert(response.body)
  const reader = response.body.getReader()
  const isSSE = (response.headers.get('content-type') ?? '').includes('text/event-stream')
  const streamReader = isSSE ? new SSEStreamReader(reader, callContext) : new StreamReader(reader, callContext)

  // Read metadata header from the stream
  const metaLen = await streamReader.readU32()
  const metaBytes = await streamReader.readExact(metaLen)
  const metaText = new TextDecoder().decode(metaBytes)

  return reviveStreamingResponse(streamReader, metaText, callContext)
}

/** WS transport: metadata is already in the HTTP body, data frames arrive
 *  via the ClientChannel. */
function parseWsStreamingResponse(
  channel: ClientChannel,
  metadataBody: string,
  callContext: CallContext,
): { ret: unknown } {
  const streamReader = new ChannelStreamReader(channel, callContext)

  return reviveStreamingResponse(streamReader, metadataBody, callContext)
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
  private streamReader: BaseStreamReader
  private pendingFrames = new Map<number, Uint8Array[]>()
  private pendingBytes = new Map<number, number>()
  private indexWaiters = new Map<number, { resolve: (v: Uint8Array | null) => void; reject: (e: unknown) => void }>()
  private reading = false
  private ended = false
  private streamError: unknown = null
  private cancelledIndices = new Set<number>()
  private doneIndices = new Set<number>()
  private totalConsumers = 0

  constructor(streamReader: BaseStreamReader) {
    this.streamReader = streamReader
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
      this.streamReader.cancel()
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
