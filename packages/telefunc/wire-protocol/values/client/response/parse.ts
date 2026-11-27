export { parseResponse }
export { BaseStreamReader } from './BaseStreamReader.js'

import { parse } from '@brillout/json-serializer/parse'
import { assert, assertWarning } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import { isObjectOrFunction } from '../../../../utils/isObjectOrFunction.js'
import { Fifo } from '../../../../utils/Fifo.js'
import { unrefTimer } from '../../../../utils/unrefTimer.js'
import { createStreamingReviver } from './registry.js'
import type { StreamSource, ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { setAbortController } from '../../../../client/abort.js'
import { setCloseHandlers, addExtraCloseHandlers, type CloseHandler } from '../../../../client/close.js'
import { makeAbortError, throwAbortError, throwBugError } from '../../../../client/remoteTelefunctionCall/errors.js'
import { BaseStreamReader } from './BaseStreamReader.js'
import { PendingValue, resolvePendingValues } from './PendingValue.js'
import { StreamReader } from './StreamReader.js'
import { SseStreamReader } from './SseStreamReader.js'
import { ClientChannel, type ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import { ClientBroadcast } from '../../../channel/client/ClientBroadcast.js'
import { wrapProxy } from '../../../../utils/wrapProxy.js'
import { GcRegistry } from '../../../../utils/GcRegistry.js'
import { ChannelStreamSource } from '../../ChannelStreamSource.js'
import { createReadableChunkStream } from '../../createReadableChunkStream.js'
import { readChunksToBytes } from '../../readChunksToBytes.js'
import { STREAMING_ERROR_TYPE } from '../../../constants.js'
import { ownIndexedPayload } from '../../../wire/envelope.js'
import { getGlobalObject } from '../../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/values/client/response/parse.ts', {
  gcRegistry: new GcRegistry(),
})

// ===== Types =====

type CallContext = {
  telefunctionName: string
  telefuncFilePath: string
  abortController: AbortController
  requestCloseHandlers: CloseHandler[]
  extensionResponseTypes: ReviverType<TypeContract, ClientReviverContext>[]
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
 *  - text                       → plain or channel-pump streaming */
async function parseResponse(
  response: Response,
  callContext: CallContext,
  connection: ConnectionOptions,
): Promise<unknown> {
  const contentType = response.headers.get('content-type') ?? ''
  const isStreaming = contentType.includes('application/octet-stream') || contentType.includes('text/event-stream')

  // HTTP streaming (binary / SSE)
  if (isStreaming) {
    assert(response.body)
    const reader = response.body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>
    const isSSE = contentType.includes('text/event-stream')
    const streamReader = isSSE
      ? //
        new SseStreamReader(reader, callContext)
      : new StreamReader(reader, callContext)
    const metaLen = await streamReader.readU32()
    const metaBytes = await streamReader.readExact(metaLen)
    const metaText = new TextDecoder().decode(metaBytes)
    return reviveResponse(metaText, callContext, connection, streamReader)
  }

  // Text response: plain or channel-pump streaming (each streaming value owns its own channel).
  const body = await response.text()
  return reviveResponse(body, callContext, connection, null)
}

// ===== Response revival =====

async function reviveResponse(
  body: string,
  callContext: CallContext,
  connection: ConnectionOptions,
  bodyStreamReader: BaseStreamReader | null,
): Promise<unknown> {
  const { extensionResponseTypes } = callContext
  const closeHandlers = new WeakMap<object, () => void>()

  const throwStreamError = (errorPayload: Record<string, unknown>): never => {
    if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT && 'abortValue' in errorPayload) {
      throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, errorPayload.abortValue)
    }
    throwBugError()
  }

  const pendingValues: PendingValue<unknown>[] = []
  let demuxer: FrameDemuxer | undefined
  const context: ClientReviverContext = {
    createChannel(opts) {
      return new ClientChannel({
        channelId: opts.channelId,
        ack: opts.ack,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    createBroadcast(opts) {
      return new ClientBroadcast({
        channelId: opts.channelId,
        key: opts.key,
        lossy: opts.lossy,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    receiveStream(metadata) {
      if ('channelId' in metadata) {
        const channel = new ClientChannel({ channelId: metadata.channelId, connection })
        return ChannelStreamSource.create(channel, { throwError: throwStreamError })
      }
      assert(bodyStreamReader, 'Unexpected receiveStream call in non-streaming response')
      demuxer ??= new FrameDemuxer(bodyStreamReader, callContext.telefunctionName)
      return demuxer.create(metadata.__index)
    },
  }

  const allCloseHandlers = [...callContext.requestCloseHandlers]

  const reviver = createStreamingReviver(
    context,
    function handOut(revived) {
      const { value, abort, close, onEnd } = revived
      allCloseHandlers.push(close)
      callContext.abortController.signal.addEventListener(
        'abort',
        () => {
          abort(makeAbortError(undefined, callContext))
        },
        { once: true },
      )

      if (value instanceof PendingValue) {
        // Suppress unhandled-rejection noise if `parse()` throws before the pending values are awaited.
        value.promise.catch(() => {})
        pendingValues.push(value)
        return value
      }

      assert(isObjectOrFunction(value))
      const wrapper = wrapProxy(value)
      const forget = globalObject.gcRegistry.register(wrapper, close)
      // Forgotten at its value's end, as the value may keep what its wrapper returned, which keeps the wrapper.
      onEnd?.(forget)
      closeHandlers.set(wrapper, close)
      // This is what the user gets
      return wrapper
    },
    extensionResponseTypes,
  )

  let parsed: unknown
  try {
    parsed = parse(body, { reviver })
    if (pendingValues.length > 0) {
      parsed = await resolvePendingValues(parsed, pendingValues, (value) => closeHandlers.has(value))
    }
    demuxer?.valuesHandedOut()
  } catch (err) {
    for (const close of allCloseHandlers) close()
    throw err
  }
  if (!isObject(parsed)) return parsed

  const { ret } = parsed
  if (isObjectOrFunction(ret)) {
    setAbortController(ret, callContext.abortController)
    setCloseHandlers(ret, closeHandlers)
    addExtraCloseHandlers(ret, callContext.requestCloseHandlers)
  }

  return { ret }
}

// ===== Frame demultiplexer =====

/** How long code may wait behind another stream's full buffer before the page warns. Handling a chunk takes far less, and
 *  the wait only counts while nothing is being read, so a slow network doesn't count. */
const STALL_WARNING_MS = 5_000

/** Demultiplexes indexed frames from a single HTTP stream to multiple consumers.
 *
 *  Best-effort backpressure: stops reading when an idle consumer's buffer hits
 *  MAX_BUFFER_BYTES_PER_INDEX, resumes once that consumer drains or cancels. Active consumers (registered as
 *  waiters) receive frames via direct dispatch — zero buffering, zero delay.
 *
 *  Note: an index's buffer may briefly exceed MAX_BUFFER_BYTES_PER_INDEX. This happens when
 *  its consumer's read of one chunk restarts the read loop, and the next frame on the wire
 *  is for that same index. Each restart adds at most 1 frame of overshoot.
 *  This is the unavoidable cost of multiplexing over a single stream — we can't
 *  peek at the next frame's index without reading it.
 *
 *  Cancellation follows .tee() semantics: cancelling one consumer marks its index
 *  as cancelled and drops future frames for it. Other consumers continue normally.
 *  The upstream reader is cancelled once every consumer is terminal and at least one cancelled. */
class FrameDemuxer {
  private static readonly MAX_BUFFER_BYTES_PER_INDEX = 1024 * 1024 // 1 MB
  private streamReader: BaseStreamReader
  private telefunctionName: string
  private pendingFrames = new Map<number, Fifo<Uint8Array<ArrayBuffer>>>()
  private pendingBytes = new Map<number, number>()
  private indexWaiters = new Map<
    number,
    { resolve: (v: Uint8Array<ArrayBuffer> | null) => void; reject: (e: unknown) => void }
  >()
  private reading = false
  /** The index whose unread buffer stopped reading; reading only restarts once that consumer drains or cancels. */
  private pausedByIndex: number | null = null
  private ended = false
  private streamError: unknown = null
  private cancelledIndices = new Set<number>()
  private doneIndices = new Set<number>()
  private totalConsumers = 0
  /** Until the page has what the call returned, it can't read a stream to make way for a file the call waits for. */
  private canStopReading = false
  /** The stall warning's timer, while reading stopped with a consumer waiting; `'warned'` once it warned. */
  private stallWarning: ReturnType<typeof setTimeout> | 'warned' | undefined

  constructor(streamReader: BaseStreamReader, telefunctionName: string) {
    this.streamReader = streamReader
    this.telefunctionName = telefunctionName
  }

  valuesHandedOut(): void {
    this.canStopReading = true
  }

  /** Cancel the given index. Follows .tee() semantics:
   *  drops its buffered/future frames, resolves any pending waiter with null. */
  cancelIndex(index: number): void {
    if (this.cancelledIndices.has(index)) return
    // Drop buffered frames for this index. A finished one isn't counted as cancelled too: counted twice, it would cancel
    // the body under a stream still being read.
    this.pendingFrames.delete(index)
    this.pendingBytes.delete(index)
    if (this.doneIndices.has(index)) return
    this.cancelledIndices.add(index)
    // Resolve any pending waiter with null (stream ended for this consumer)
    const waiter = this.indexWaiters.get(index)
    if (waiter) {
      this.indexWaiters.delete(index)
      waiter.resolve(null)
      // With no one left waiting, no warning is due.
      if (this.indexWaiters.size === 0) this.clearStallTimer()
    }
    this.cancelUpstreamIfAllTerminal()
    // Reading may have stopped on this index's buffer, with another consumer waiting behind it.
    this.resumeIfUnblocked(index)
    this.ensureReading()
  }

  private cancelUpstreamIfAllTerminal(): void {
    if (this.cancelledIndices.size > 0 && this.cancelledIndices.size + this.doneIndices.size >= this.totalConsumers) {
      this.streamReader.cancel()
    }
  }

  async readNextChunkForIndex(index: number): Promise<Uint8Array<ArrayBuffer> | null> {
    if (this.cancelledIndices.has(index)) return null
    if (this.streamError) throw this.streamError

    const buffered = this.pendingFrames.get(index)?.shift()
    if (buffered) {
      this.pendingBytes.set(index, (this.pendingBytes.get(index) ?? 0) - buffered.byteLength)
      this.resumeIfUnblocked(index)
      this.ensureReading()
      return buffered
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

  create(index: number): StreamSource {
    this.totalConsumers++
    const readNextChunk = () => this.readNextChunkForIndex(index)
    const cancel = () => this.cancelIndex(index)
    const isCancelled = () => this.cancelledIndices.has(index)
    const readOwnedChunk = async () => {
      const chunk = await readNextChunk()
      return chunk && ownIndexedPayload(chunk)
    }
    return {
      readNextChunk,
      bytes: (opts) => readChunksToBytes(readNextChunk, isCancelled, opts),
      stream: (opts) =>
        createReadableChunkStream({ readNextChunk: readOwnedChunk, cancel, isCancelled, readOptions: opts }),
      cancel,
      // Inline streams abort via HTTP body reader cancellation, which reading that has stopped doesn't see.
      abort: (abortError) => this.failWaiters(abortError),
    }
  }

  private async ensureReading() {
    if (this.reading) return
    // Reading may have stopped while no consumer waited, so no warning is on its way for this one.
    if (this.pausedByIndex !== null) {
      if (this.stallWarning === undefined) this.warnIfStillStalledLater()
      return
    }
    this.reading = true
    this.clearStallTimer()
    try {
      while (this.indexWaiters.size > 0) {
        const frame = await this.streamReader.readNextFrame()
        if (frame === null) return this.endAll()
        if (this.dispatch(frame.index, frame.payload)) return this.warnIfStillStalledLater()
      }
    } catch (err) {
      this.failWaiters(err)
    } finally {
      this.reading = false
    }
  }

  /** The response ended: every waiting index ends too. */
  private endAll(): void {
    this.ended = true
    for (const [, w] of this.indexWaiters) w.resolve(null)
    this.indexWaiters.clear()
  }

  /** Hands a frame to its index's waiter or buffer; returns whether reading stops till that index's consumer drains. */
  private dispatch(index: number, payload: Uint8Array<ArrayBuffer>): boolean {
    // Drop frames for cancelled indices
    if (this.cancelledIndices.has(index)) return false
    const waiter = this.indexWaiters.get(index)
    // Empty payload = per-index "done" signal
    if (payload.length === 0) {
      this.doneIndices.add(index)
      this.cancelUpstreamIfAllTerminal()
      if (waiter) {
        this.indexWaiters.delete(index)
        waiter.resolve(null)
      }
      return false
    }
    // Direct dispatch — no buffering, no delay
    if (waiter) {
      this.indexWaiters.delete(index)
      waiter.resolve(payload)
      return false
    }
    // No consumer waiting — buffer it
    let pending = this.pendingFrames.get(index)
    if (!pending) this.pendingFrames.set(index, (pending = new Fifo()))
    pending.push(payload)
    const newBytes = (this.pendingBytes.get(index) ?? 0) + payload.byteLength
    this.pendingBytes.set(index, newBytes)
    // Per-index backpressure: stop reading when this index's buffer exceeds 1 MB.
    if (!this.canStopReading || newBytes < FrameDemuxer.MAX_BUFFER_BYTES_PER_INDEX) return false
    this.pausedByIndex = index
    return true
  }

  /** Lifts the pause once its index drained below the cap, unless an unfinished index's buffer is still full. */
  private resumeIfUnblocked(index: number): void {
    if (this.pausedByIndex !== index) return
    this.pausedByIndex = null
    for (const [other, bytes] of this.pendingBytes) {
      if (bytes < FrameDemuxer.MAX_BUFFER_BYTES_PER_INDEX || this.doneIndices.has(other)) continue
      this.pausedByIndex = other
      return
    }
  }

  private failWaiters(err: unknown): void {
    this.clearStallTimer()
    this.streamError ??= err
    for (const [, w] of this.indexWaiters) w.reject(err)
    this.indexWaiters.clear()
  }

  /** Reading stopped with a consumer waiting: the next read restarts it, which clears this. */
  private warnIfStillStalledLater(): void {
    if (this.stallWarning === 'warned' || this.indexWaiters.size === 0) return
    this.stallWarning = unrefTimer(setTimeout(() => this.warnStalled(), STALL_WARNING_MS))
  }

  private clearStallTimer(): void {
    if (this.stallWarning === 'warned') return
    clearTimeout(this.stallWarning)
    this.stallWarning = undefined
  }

  private warnStalled(): void {
    this.stallWarning = 'warned'
    assertWarning(
      false,
      `Reading what ${this.telefunctionName}() returned is waiting for a stream of it with 1 MiB unread: read all of the streams a call returned concurrently, or cancel the ones you don't want.`,
      { onlyOnce: false },
    )
  }
}
