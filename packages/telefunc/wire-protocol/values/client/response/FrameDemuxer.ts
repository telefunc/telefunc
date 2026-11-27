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
 *  Once every consumer is terminal, the upstream reader is cancelled if one cancelled, else released. */
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
  private cancelIndex(index: number): void {
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
    this.endUpstreamIfAllTerminal()
    // Reading may have stopped on this index's buffer, with another consumer waiting behind it.
    this.resumeIfUnblocked(index)
    this.ensureReading()
  }

  private endUpstreamIfAllTerminal(): void {
    if (this.cancelledIndices.size + this.doneIndices.size < this.totalConsumers) return
    if (this.cancelledIndices.size > 0) this.streamReader.cancel()
    else this.streamReader.release()
  }

  private async readNextChunkForIndex(index: number): Promise<Uint8Array<ArrayBuffer> | null> {
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
    // Reading may have stopped while no consumer waited, so no warning is on its way for this one.
    if (this.pausedByIndex !== null) {
      if (this.stallTimer === undefined) this.warnIfStillStalledLater()
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
      this.streamReader.cancel()
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
      this.endUpstreamIfAllTerminal()
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
    // Per-index backpressure: once the call's values are handed out, stop reading when this index's buffer reaches 1 MiB.
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
    if (this.stallWarned || this.indexWaiters.size === 0) return
    this.stallTimer = unrefTimer(setTimeout(() => this.warnStalled(), STALL_WARNING_MS))
  }

  private clearStallTimer(): void {
    clearTimeout(this.stallTimer)
    this.stallTimer = undefined
  }

  private warnStalled(): void {
    this.stallWarned = true
    this.stallTimer = undefined
    assertWarning(
      false,
      `Reading what ${this.telefunctionName}() returned is waiting for a stream of it with 1 MiB unread: read all of the streams a call returned concurrently, or cancel the ones you don't want.`,
      { onlyOnce: false },
    )
  }
}
