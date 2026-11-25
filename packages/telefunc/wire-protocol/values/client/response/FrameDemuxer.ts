export { FrameDemuxer }

import { assertWarning } from '../../../../utils/assert.js'
import { Fifo } from '../../../../utils/Fifo.js'
import { unrefTimer } from '../../../../utils/unrefTimer.js'
import type { StreamSource } from '../../types.js'
import type { BaseStreamReader } from './BaseStreamReader.js'
import { createReadableChunkStream } from '../../createReadableChunkStream.js'
import { readChunksToBytes } from '../../readChunksToBytes.js'
import { ownIndexedPayload } from '../../../wire/envelope.js'

/** How long code may wait behind another stream's full buffer before the page warns. Handling a chunk takes far less, and
 *  the wait only counts while nothing is being read, so a slow network doesn't count. */
const STALL_WARNING_MS = 5_000

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
  private ended = false
  private streamError: unknown = null
  private cancelledIndices = new Set<number>()
  private doneIndices = new Set<number>()
  private totalConsumers = 0
  /** Until the page has what the call returned, it can't read a stream to make way for a file the call waits for. */
  private canStopReading = false
  private stallTimer: ReturnType<typeof setTimeout> | undefined
  private stallWarned = false

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
    if (this.doneIndices.has(index)) return
    this.cancelledIndices.add(index)
    // Resolve any pending waiter with null (stream ended for this consumer)
    const waiter = this.indexWaiters.get(index)
    if (waiter) {
      this.indexWaiters.delete(index)
      waiter.resolve(null)
    }
    this.cancelUpstreamIfAllTerminal()
    // Reading may have stopped on this index's buffer, with another consumer waiting behind it.
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
      this.ensureReading()
      return buffered
    }
    if (this.doneIndices.has(index)) return null
    if (this.ended) return null

    return new Promise<Uint8Array<ArrayBuffer> | null>((resolve, reject) => {
      this.indexWaiters.set(index, { resolve, reject })
      this.ensureReading()
    })
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
    this.reading = true
    clearTimeout(this.stallTimer)
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
    return this.canStopReading && newBytes >= FrameDemuxer.MAX_BUFFER_BYTES_PER_INDEX
  }

  private failWaiters(err: unknown): void {
    clearTimeout(this.stallTimer)
    this.streamError ??= err
    for (const [, w] of this.indexWaiters) w.reject(err)
    this.indexWaiters.clear()
  }

  /** Reading stopped with a consumer waiting: the next read restarts it, which clears this. */
  private warnIfStillStalledLater(): void {
    if (this.stallWarned || this.indexWaiters.size === 0) return
    this.stallTimer = unrefTimer(setTimeout(() => this.warnStalled(), STALL_WARNING_MS))
  }

  private warnStalled(): void {
    this.stallWarned = true
    assertWarning(
      false,
      `Reading what ${this.telefunctionName}() returned is waiting for a stream of it with 1 MiB unread: read all of the streams a call returned concurrently, or cancel the ones you don't want.`,
      { onlyOnce: false },
    )
  }
}
