export { parseResponse }
export { BaseStreamReader } from './BaseStreamReader.js'

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import { createStreamingReviver } from './registry.js'
import { setAbortController } from '../../../client/abort.js'
import { setCloseHandlers } from '../../../client/close.js'
import { makeAbortError } from '../../../client/remoteTelefunctionCall/errors.js'
import { BaseStreamReader } from './BaseStreamReader.js'
import { StreamReader } from './StreamReader.js'
import { SSEStreamReader } from './SSEStreamReader.js'
import { ChannelStreamReader } from './ChannelStreamReader.js'
import { ClientChannel } from '../channel.js'
import { extractFrameChannel } from '../../frame-channel.js'
import type { ChannelTransports } from '../../constants.js'

// ===== Types =====

type CallContext = {
  telefunctionName: string
  telefuncFilePath: string
  abortController: AbortController
  channel: { transports: ChannelTransports }
}

// ===== Public entry point =====

/** Single entry point for all response transports.
 *
 *  Detects the transport from response headers / body and delegates to
 *  the right reviver — the caller never needs to know which transport
 *  was used.
 *
 *  - `application/octet-stream` → HTTP binary stream
 *  - `text/event-stream`        → SSE stream
 *  - text with `__frameChannel` → WS streaming
 *  - text without               → plain (placeholder-only) */
async function parseResponse(response: Response, callContext: CallContext, sessionToken?: string): Promise<unknown> {
  const transports = callContext.channel.transports
  const contentType = response.headers.get('content-type') ?? ''
  const isStreaming = contentType.includes('application/octet-stream') || contentType.includes('text/event-stream')

  // HTTP streaming (binary / SSE)
  if (isStreaming) {
    assert(response.body)
    const reader = response.body.getReader()
    const isSSE = contentType.includes('text/event-stream')
    const streamReader = isSSE ? new SSEStreamReader(reader, callContext) : new StreamReader(reader, callContext)
    const metaLen = await streamReader.readU32()
    const metaBytes = await streamReader.readExact(metaLen)
    const metaText = new TextDecoder().decode(metaBytes)
    return reviveStreamingResponse(metaText, callContext, streamReader, transports, sessionToken)
  }

  // Text response: channel streaming or plain
  const body = await response.text()
  const frameChannel = extractFrameChannel(body)
  if (frameChannel) {
    const channel = new ClientChannel({
      channelId: frameChannel.metadata.channelId,
      transports,
      sessionToken,
      defer: false,
    })
    const streamReader = new ChannelStreamReader(channel, callContext)
    return reviveStreamingResponse(frameChannel.strippedBody, callContext, streamReader, transports, sessionToken)
  }
  return revivePlainResponse(body, callContext, transports, sessionToken)
}

// ===== Streaming response (HTTP / WS) =====

/** Revive a response that contains streaming values (generators, streams, promises).
 *
 *  The demuxer is created before parse — this is required because some
 *  streaming types (e.g. Promise) eagerly read chunks inside `createValue`. */
function reviveStreamingResponse(
  metadataText: string,
  callContext: CallContext,
  streamReader: BaseStreamReader,
  transports: ChannelTransports,
  sessionToken?: string,
): unknown {
  const demuxer = new FrameDemuxer(streamReader)

  const getChunkReader = (index: number) => {
    demuxer.registerConsumer()
    return () => demuxer.readNextChunkForIndex(index)
  }
  const getCancelIndex = (index: number) => () => demuxer.cancelIndex(index)

  const { reviver, channels, closeHandlers } = createStreamingReviver(
    getChunkReader,
    getCancelIndex,
    transports,
    sessionToken,
  )
  const parsed: unknown = parse(metadataText, { reviver })
  if (!isObject(parsed)) return parsed

  return finalizeResponse(parsed, channels, closeHandlers, callContext)
}

// ===== Plain text response =====

/** Revive a non-streaming response. Only placeholder types (e.g. Channel) are revived. */
function revivePlainResponse(
  body: string,
  callContext: CallContext,
  transports: ChannelTransports,
  sessionToken?: string,
): unknown {
  const unreachable = (): never => {
    assert(false, 'Unexpected streaming value in plain response')
  }
  const { reviver, channels, closeHandlers } = createStreamingReviver(
    () => unreachable,
    () => unreachable,
    transports,
    sessionToken,
  )
  const parsed: unknown = parse(body, { reviver })
  if (!isObject(parsed)) return parsed

  return finalizeResponse(parsed, channels, closeHandlers, callContext)
}

// ===== Shared cleanup =====

function finalizeResponse(
  parsed: Record<string, unknown>,
  channels: ClientChannel[],
  closeHandlers: WeakMap<object, () => void>,
  callContext: CallContext,
): Record<string, unknown> {
  if (channels.length > 0) {
    callContext.abortController.signal.addEventListener(
      'abort',
      () => {
        const abortError = makeAbortError(undefined, callContext)
        for (const ch of channels) ch._abortLocally(abortError.abortValue, abortError.message)
      },
      { once: true },
    )
  }

  const { ret } = parsed
  if (isObjectOrFunction(ret)) {
    setAbortController(ret, callContext.abortController)
    setCloseHandlers(ret, closeHandlers)
  }

  return { ret }
}

// ===== Frame demultiplexer =====

/** Demultiplexes indexed frames from a single HTTP stream to multiple consumers.
 *
 *  Best-effort backpressure: stops reading when an idle consumer's buffer hits
 *  MAX_BUFFER_BYTES_PER_INDEX, resumes when drained. Active consumers (registered as
 *  waiters) receive frames via direct dispatch — zero buffering, zero delay.
 *
 *  Note: an index's buffer may briefly exceed MAX_BUFFER_BYTES_PER_INDEX. This happens when
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
  private pendingFrames = new Map<number, Uint8Array<ArrayBuffer>[]>()
  private pendingBytes = new Map<number, number>()
  private indexWaiters = new Map<
    number,
    { resolve: (v: Uint8Array<ArrayBuffer> | null) => void; reject: (e: unknown) => void }
  >()
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

  async readNextChunkForIndex(index: number): Promise<Uint8Array<ArrayBuffer> | null> {
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

    let resolve: (v: Uint8Array<ArrayBuffer> | null) => void
    let reject: (e: unknown) => void
    const promise = new Promise<Uint8Array<ArrayBuffer> | null>((res, rej) => {
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
