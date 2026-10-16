export { SseTransport }

import { assert } from '../../utils/assert.js'
import {
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_TRANSPORT,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_FLOOR_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_POST_MAX_BYTES,
  SSE_POST_MIN_BYTES,
  SSE_POST_MIN_BYTES_PER_S,
  SSE_POST_TARGET_MS,
  SSE_RECONCILE_DEADLINE_MS,
  STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS,
  UPGRADE_DRAIN_TIMEOUT_MS,
} from '../constants.js'
import { encodeU32, encodeLengthPrefixedFrames } from '../frame.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { Uplink } from './uplink.js'
import { Downlink } from './downlink.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../request-kind.js'
import { TAG, decode, isSequencedTag, type ReconciledPayload } from '../shared-ws.js'
import { encodeSseBatch, encodeSseRequest, encodeSseRequestMetadata } from '../sse-request.js'
import { DeadlineScheduler } from './deadlineScheduler.js'
import { SSEEvents } from './SSEEvents.js'
import { randomUuid } from '../../utils/randomUuid.js'
import { settledOrAborted } from '../../utils/settledOrAborted.js'
import type {
  BarrierEmission,
  ClientConnection,
  OutboundFrame,
  OutboundFrameKind,
  UpgradeSource,
} from './connection.js'
import type { Heartbeat } from './heartbeat.js'

type OutboxEntry = { frame: Uint8Array<ArrayBuffer>; deadline: number }

/** A POST body built ahead from the outbox's first `count` entries, `first` through `last`. */
type OutboxFold = { blob: Blob; count: number; first: OutboxEntry; last: OutboxEntry }

type SseInitialBatchStage = {
  initialFrames: OutboundFrame[]
  movedOutbox: OutboxEntry[]
  movedBufferedFrames: OutboundFrame[]
}

/** A wire's streaming upload POST: its body, what aborts it alone, and its end. */
type StreamUpload = {
  body: PushReadableStream<Uint8Array<ArrayBuffer>>
  abort: AbortController
  ended: Promise<'fetch-ended'>
}

type SseEventStreamReader = ReturnType<typeof createSseEventStreamReader>

/** What `SseTransport.foldOutbox` adds to the next POST's body at a time. */
const SSE_FOLD_BYTES = 1024 * 1024

class SseTransport implements UpgradeSource {
  readonly type = CHANNEL_TRANSPORT.SSE
  readonly sendReconcileOnOpen = false
  readonly reconcileMode = 'batch-on-reconcile' as const
  /** Each wire's own, so a POST still in flight for a wire that died isn't dispatched on the next one. */
  private connId = randomUuid()
  uplink = new Uplink()
  downlink = new Downlink(this.uplink)
  get batched(): boolean {
    return this.streamRequest.tag !== 'active'
  }
  heartbeat: Heartbeat | null = null
  private connecting = false
  private startTimer: ReturnType<typeof setTimeout> | null = null
  /** Abort handle for the active transport's fetches. `null` means no active transport. */
  private transportAbort: AbortController | null = null
  private abandonedStream: AbortController | null = null
  private readonly abandonedControllers = new WeakSet<AbortController>()
  private outbox: OutboxEntry[] = []
  private readonly flushScheduler = new DeadlineScheduler(() => {
    void this.flushOutbox()
  })
  private flushing = false
  /** Bytes pushed to the outbox since `foldOutbox` last folded: when to fold, not what. */
  private unfolded = 0
  /** The next POST's body, as far as `foldOutbox` built it. */
  private folded: OutboxFold | null = null
  private lastPostStartedAt = 0
  /** What a batch POST is sized to carry: what its bytes take `SSE_POST_TARGET_MS` to cross at the rate the last ones did. */
  private postBytes = SSE_POST_MIN_BYTES
  /** The body of the batch POST out, 0 with none. */
  private postOutBytes = 0
  /** The frames in `outbox`. */
  private outboxBytes = 0
  /** The batch POST out is lost if this passes. */
  private postWatch: ReturnType<typeof setTimeout> | null = null
  private flushThrottleMs = SSE_FLUSH_THROTTLE_MS
  private postIdleFlushDelayMs = SSE_POST_IDLE_FLUSH_DELAY_MS
  private heartbeatFlushDelayMs = Math.floor(CHANNEL_PING_INTERVAL_MS / 2)
  private drainCallbacks: Array<() => void> = []
  /** Client→server upstream POST. `failed` is sticky → fall back to outbox+batch POSTs forever. */
  private streamRequest:
    | { tag: 'idle' }
    | {
        tag: 'active'
        body: PushReadableStream<Uint8Array<ArrayBuffer>>
        /** Written before the server acknowledged the POST; null once it did. */
        unconfirmed: Uint8Array<ArrayBuffer>[] | null
      }
    | { tag: 'failed' } = { tag: 'idle' }

  constructor(
    private readonly telefuncUrl: string,
    private readonly fetchImpl: typeof fetch,
    private readonly userHeaders: Record<string, string> | undefined,
    private readonly owner: ClientConnection,
  ) {}

  async emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    if (this.streamRequest.tag === 'active') return this.emitBarrierStreamRequest(buildFrame)
    return this.emitBarrierBatch(buildFrame, signal)
  }

  private emitBarrierStreamRequest(buildFrame: () => OutboundFrame): BarrierEmission {
    assert(this.streamRequest.tag === 'active' && !this.flushing && this.outbox.length === 0)
    const frame = buildFrame()
    this.streamRequest.body.push(encodeU32(frame.frame.byteLength))
    this.streamRequest.body.push(frame.frame)
    this.closeStreamRequest()
    return 'emitted'
  }

  private async emitBarrierBatch(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    if (this.flushing || this.outbox.length > 0) {
      const drained = new Promise<void>((resolve) => this.drainCallbacks.push(resolve))
      await Promise.race([drained, new Promise<void>((resolve) => setTimeout(resolve, UPGRADE_DRAIN_TIMEOUT_MS))])
    }
    // A POST still out then is an upload, which the barrier would wait behind as long as it takes.
    if (this.flushing || !this.hasWire() || signal.aborted) return 'not-emitted'
    this.flushScheduler.cancel()
    const queued = this.outbox.splice(0, this.outbox.length).map((entry) => entry.frame)
    this.outboxBytes = 0
    queued.push(buildFrame().frame)
    // Not `flushOutbox`: it re-queues on failure, and a barrier must never be replayed onto the
    // next wire. `emitted` is already decided — this POST cannot report whether the server saw it,
    // and a barrier that may have arrived counts as arrived. The wait only delays the flip.
    await settledOrAborted(this.sendStandalonePost(queued), signal)
    return 'emitted'
  }

  start(): void {
    if (this.connecting || this.hasWire()) return
    this.connecting = true
    // Defer one reconcile window so startup code can register channels before the initial batch.
    this.startTimer = setTimeout(() => {
      this.startTimer = null
      if (!this.connecting || this.hasWire()) return
      void this.openStream()
    }, SSE_RECONCILE_DEADLINE_MS)
  }

  hasWire(): boolean {
    return this.transportAbort !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    if (this.streamRequest.tag === 'active') {
      this.streamRequest.body.push(encodeU32(frame.frame.byteLength))
      this.streamRequest.body.push(frame.frame)
      this.streamRequest.unconfirmed?.push(frame.frame)
      return
    }
    const now = Date.now()
    const deadline = this.getFrameDeadline(frame.kind, now)
    const entry = { frame: frame.frame, deadline }
    this.outbox.push(entry)
    this.outboxBytes += frame.frame.byteLength
    this.unfolded = (this.outbox.length === 1 ? 0 : this.unfolded) + frame.frame.byteLength
    if (this.flushing) {
      this.foldOutbox()
      if (frame.kind === 'heartbeat') this.unholdPing(entry)
    }
    // A timer pending is for the earliest deadline queued before this frame: only this frame's can come earlier.
    if (this.flushScheduler.pending && this.hasWire()) this.flushScheduler.schedule(deadline)
    else this.scheduleFlush()
    // A whole POST is held: it goes without waiting out the throttle, which would cap an upload at a POST per throttle.
    if (deadline <= now || this.sendRoom() <= 0) void this.flushOutbox()
  }

  /** While a POST is out, folds the outbox into the next body a MiB at a time: the flush after copies only the rest. */
  private foldOutbox(): void {
    if (this.unfolded < SSE_FOLD_BYTES) return
    this.unfolded = 0
    const folded = this.foldHeading(this.outbox)
    const from = folded?.count ?? 0
    const parts: (Blob | Uint8Array<ArrayBuffer>)[] = folded ? [folded.blob] : []
    for (let i = from; i < this.outbox.length; i++) {
      const { frame } = this.outbox[i]!
      parts.push(encodeU32(frame.byteLength), frame)
    }
    this.folded = {
      blob: new Blob(parts),
      count: this.outbox.length,
      first: this.outbox[0]!,
      last: this.outbox[this.outbox.length - 1]!,
    }
  }

  /** The fold, if it still heads `entries`. */
  private foldHeading(entries: OutboxEntry[]): OutboxFold | null {
    const folded = this.folded
    return folded !== null && entries[0] === folded.first && entries[folded.count - 1] === folded.last ? folded : null
  }

  private async openStream(): Promise<void> {
    this.connId = randomUuid()
    this.uplink = new Uplink()
    this.downlink = new Downlink(this.uplink)
    const abortController = new AbortController()
    this.transportAbort = abortController
    const stage = this.stageInitialBatch()

    // SSE downstream + upstream POST fire in parallel. If upstream fails, we fall back to outbox+batch.
    const ssePromise = this.post(
      encodeSseRequest(
        { connId: this.connId, streamResponse: true },
        encodeLengthPrefixedFrames(stage.initialFrames, (entry) => entry.frame),
      ),
      abortController.signal,
      { accept: 'text/event-stream' },
    )
    const upload = this.streamRequest.tag === 'failed' ? null : this.openUpload(abortController)
    this.owner._onTransportReconcileSent(this)

    let response: Response
    try {
      response = await ssePromise
    } catch {
      this.failOpen(stage, abortController, false)
      return
    }
    if (!response.ok || !response.body) {
      this.failOpen(stage, abortController, true)
      return
    }

    const reader = createSseEventStreamReader(
      response.body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
      abortController,
      (bytes) => this.heartbeat?.noteReceived(bytes),
    )
    // Run the SSE loop concurrently with the handshake wait — frames (including the first
    // RECONCILED) can arrive while the open-ack is still in flight and must be dispatched.
    let resolveHandshakeOk!: () => void
    const handshakeOkP = new Promise<'ok'>((resolve) => {
      resolveHandshakeOk = () => resolve('ok')
    })
    void this.readEvents(reader, abortController, resolveHandshakeOk)

    // Race upstream readiness: ack (ok), fetch ended (dead), or timeout. Non-ok → outbox+batch.
    if (upload) {
      const timeoutP = new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS),
      )
      const result = await Promise.race([handshakeOkP, timeoutP, upload.ended])
      if (result === 'ok') this.confirmUpload(upload, abortController)
      else this.abandonUpload(upload)
    }

    this.connecting = false
    this.owner._onTransportOpen(this)
    if (this.outbox.length > 0) void this.flushOutbox()
  }

  /** The streaming upload POST, aborted with the wire's `abortController` or alone. */
  private openUpload(abortController: AbortController): StreamUpload {
    const abort = new AbortController()
    abortController.signal.addEventListener('abort', () => abort.abort(), { once: true })
    const body = createPushReadableStream<Uint8Array<ArrayBuffer>>()
    // Metadata header first — the server classifies the POST by it; `streamRequest: true`
    // makes it emit `reconciled` inline (the body never ends, can't defer to body-end).
    body.push(encodeSseRequestMetadata({ connId: this.connId, streamRequest: true }))
    const fetch = this.postAndRead(body, abort.signal, { duplex: 'half' })
    this.streamRequest = { tag: 'active', body, unconfirmed: [] }
    // The POST ends with its body or when cut on the way; `ended` handles its rejection if openStream exits.
    const ended = fetch.then(
      () => 'fetch-ended' as const,
      () => 'fetch-ended' as const,
    )
    return { body, abort, ended }
  }

  /** The server acknowledged the upload: what it was written is confirmed. */
  private confirmUpload({ body, ended }: StreamUpload, abortController: AbortController): void {
    if (this.streamRequest.tag === 'active') this.streamRequest.unconfirmed = null
    // Ended while its body is still ours: nothing written to it reaches the server any more, so this wire ends.
    void ended.then(() => {
      if (this.streamRequest.tag === 'active' && this.streamRequest.body === body) abortController.abort()
    })
  }

  /** The upload went unacknowledged: frames go in batch POSTs from now on. */
  private abandonUpload({ abort }: StreamUpload): void {
    // The server may not have read a body it never acknowledged: resend it, first; it drops a seq it already has.
    const unsent = this.streamRequest.tag === 'active' ? this.streamRequest.unconfirmed : null
    // Aborted, not ended: a proxy holding the body forwards it once it ends, and the server would run it after what follows.
    abort.abort()
    this.streamRequest = { tag: 'failed' }
    if (unsent) {
      this.outbox = unsent.map((frame) => ({ frame, deadline: Date.now() })).concat(this.outbox)
      for (const frame of unsent) this.outboxBytes += frame.byteLength
    }
    this.owner._onTransportBatched(this)
  }

  /** The downstream didn't open: what the reconcile took goes back to the outbox, and the wire closes. */
  private failOpen(stage: SseInitialBatchStage, abortController: AbortController, rejectedByServer: boolean): void {
    this.rollbackInitialBatch(stage)
    this.closeStreamRequest()
    abortController.abort()
    this.transportAbort = null
    this.connecting = false
    this.owner._onTransportClosed(this, { rejectedByServer })
  }

  /** Dispatches the downstream's events until it ends; `onOpenAck` runs as the upload's acknowledgement arrives. */
  private async readEvents(reader: SseEventStreamReader, abortController: AbortController, onOpenAck: () => void) {
    try {
      while (true) {
        const raw = await reader.readNextEntry()
        if (!raw) break
        if (raw[0] === TAG.STREAM_REQUEST_OPEN_ACK) {
          onOpenAck()
          continue
        }
        const frame = decode(raw)
        if (frame.tag === TAG.PONG) {
          this.heartbeat?.resetPong()
          this.owner._onTransportPong(frame.ended)
          continue
        }
        this.owner._onTransportFrame(frame, this, raw.byteLength, this.abandonedControllers.has(abortController))
      }
    } catch {
      if (abortController.signal.aborted) return
    } finally {
      reader.cancel()
      // The old SSE reader's death must NOT trample a successor openStream's streamRequest /
      // transportAbort. Only mutate transport state if this controller is still the active one.
      if (this.transportAbort === abortController) {
        this.closeStreamRequest()
        this.transportAbort = null
        // Its batch POST still in flight settles now rather than hold the next wire's outbox.
        abortController.abort()
      }
      // Abandoned controllers are owned by a successor transport — don't notify closed.
      if (!this.abandonedControllers.has(abortController)) this.owner._onTransportClosed(this)
    }
  }

  private stageInitialBatch(): SseInitialBatchStage {
    const reconcileBatch = this.owner.stageReconcileBatch(true)
    const initialFrames: OutboundFrame[] = []
    initialFrames.push(reconcileBatch.reconcileFrame)
    const movedBufferedFrames = reconcileBatch.movedBufferedFrames
    // Sequenced frames replay after RECONCILED: sent now, they'd pass a POST in flight, making its frames duplicates.
    const movedOutbox = this.outbox.filter(isWindowRefresh)
    this.outbox = []
    this.outboxBytes = 0
    for (const entry of movedOutbox) initialFrames.push({ kind: 'data', frame: entry.frame })
    for (const frame of movedBufferedFrames) initialFrames.push(frame)
    return { initialFrames, movedOutbox, movedBufferedFrames }
  }

  private rollbackInitialBatch(stage: SseInitialBatchStage): void {
    if (stage.movedOutbox.length === 0 && stage.movedBufferedFrames.length === 0) return
    const now = Date.now()
    const movedBuffered: OutboxEntry[] = stage.movedBufferedFrames.map((entry) => ({
      frame: entry.frame,
      deadline: this.getFrameDeadline(entry.kind, now),
    }))
    this.outbox = stage.movedOutbox.concat(movedBuffered, this.outbox)
    this.outboxBytes += bytesOf(stage.movedOutbox) + bytesOf(movedBuffered)
  }

  private async flushOutbox(): Promise<void> {
    if (!this.hasWire() || this.flushing || this.outbox.length === 0) return
    assert(this.transportAbort)
    this.flushScheduler.cancel()
    this.flushing = true
    try {
      const now = Date.now()
      const queued = this.outbox.splice(0, this.outbox.length)
      this.outboxBytes = 0
      const head = this.foldHeading(queued)
      this.folded = null
      this.lastPostStartedAt = now
      const wire = this.transportAbort

      try {
        const body = encodeSseBatch(
          { connId: this.connId },
          queued.slice(head?.count ?? 0).map((entry) => entry.frame),
          head?.blob,
        )
        this.postOutBytes = body.size
        this.watchPost(wire, SSE_POST_FLOOR_MS + (body.size * 1000) / SSE_POST_MIN_BYTES_PER_S)
        const response = await this.postAndRead(body, wire.signal)
        if (!response.ok) throw new Error('POST failed')
        this.sizePosts(body.size, Date.now() - now)
      } catch {
        // Its wire ended already: what the POST carried goes as that wire's outbox does (stageInitialBatch).
        if (wire !== this.transportAbort) {
          const refreshes = queued.filter(isWindowRefresh)
          this.outbox = refreshes.concat(this.outbox)
          this.outboxBytes += bytesOf(refreshes)
          return
        }
        this.outbox = queued.concat(this.outbox)
        this.outboxBytes += bytesOf(queued)
        this.abandonActiveTransport()
        this.owner._onTransportClosed(this)
        return
      }
    } finally {
      if (this.postWatch) clearTimeout(this.postWatch)
      this.postWatch = null
      this.flushing = false
      this.postOutBytes = 0
      this.flushNextOrDrain()
      this.owner._onTransportRoom(this)
    }
  }

  /** Sizes next POSTs to what this one's rate carries in `SSE_POST_TARGET_MS`, unless it was quicker and not full. */
  private sizePosts(bytes: number, ms: number): void {
    if (ms < SSE_POST_TARGET_MS && bytes < this.postBytes) return
    this.postBytes = Math.min(
      SSE_POST_MAX_BYTES,
      Math.max(SSE_POST_MIN_BYTES, (bytes * SSE_POST_TARGET_MS) / Math.max(1, ms)),
    )
  }

  sendRoom(): number {
    return this.batched ? this.postBytes - this.postOutBytes - this.outboxBytes : Infinity
  }

  /** The wire is lost with the POST out for `ms`, as one that missed its pong: a reconnect resends what it carried. */
  private watchPost(wire: AbortController, ms: number): void {
    this.postWatch = setTimeout(() => {
      this.owner._onTransportPostLate(this)
      if (this.transportAbort === wire) this.watchPost(wire, SSE_POST_FLOOR_MS)
    }, ms)
  }

  /** After a POST: the outbox goes next, or those waiting for the drain are told it's empty. */
  private flushNextOrDrain(): void {
    if (this.outbox.length > 0) {
      if (this.sendRoom() <= 0) void this.flushOutbox()
      else this.scheduleFlush()
      return
    }
    this.settleDrained()
  }

  /** A PING still held at its deadline goes alone: the next POST would bring it past the server's ping deadline. */
  private unholdPing(entry: OutboxEntry): void {
    setTimeout(
      () => {
        const at = this.outbox.indexOf(entry)
        if (!this.flushing || at === -1) return
        this.outbox.splice(at, 1)
        this.outboxBytes -= entry.frame.byteLength
        void this.sendStandalonePost([entry.frame])
      },
      Math.max(0, entry.deadline - Date.now()),
    )
  }

  /** Every request this transport makes. The three senders differ only in what they send and what
   *  a failure means to them, so that is all they say. */
  private post(body: BodyInit, signal: AbortSignal, extra?: { accept?: string; duplex?: 'half' }): Promise<Response> {
    return this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
      method: 'POST',
      headers: {
        ...this.userHeaders,
        ...(extra?.accept ? { Accept: extra.accept } : undefined),
        'Content-Type': 'application/octet-stream',
        [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
      },
      body,
      signal,
      // @ts-ignore duplex is not yet in TypeScript's RequestInit
      duplex: extra?.duplex,
    })
  }

  /** Chrome reports a POST whose response is left unread as canceled. */
  private async postAndRead(body: BodyInit, signal: AbortSignal, extra?: { duplex: 'half' }): Promise<Response> {
    const response = await this.post(body, signal, extra)
    await response.arrayBuffer()
    return response
  }

  /** For frames that cannot wait for the next flush. Failure is swallowed: its two senders are a
   *  heartbeat ping (its own deadline notices a dead wire) and the barrier (delivered either way). */
  private async sendStandalonePost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
    if (!this.hasWire()) return
    assert(this.transportAbort)
    try {
      await this.postAndRead(
        encodeSseRequest({ connId: this.connId }, encodeLengthPrefixedFrames(frames)),
        this.transportAbort.signal,
      )
    } catch {}
  }

  /** Not the control frames waiting for the next POST: a channel out of credit with only those waiting was starved. */
  bufferedAmount(): number {
    if (this.streamRequest.tag === 'active') return this.streamRequest.body.bufferedAmount
    let bytes = 0
    for (const { frame } of this.outbox) if (isSequencedTag(frame[0]!)) bytes += frame.byteLength
    return bytes
  }

  private scheduleFlush(): void {
    if (this.outbox.length === 0 || !this.hasWire()) return
    let earliest = Infinity
    for (const entry of this.outbox) if (entry.deadline < earliest) earliest = entry.deadline
    this.flushScheduler.schedule(earliest)
  }

  private getFrameDeadline(kind: OutboundFrameKind, now = Date.now()): number {
    switch (kind) {
      case 'reconcile':
      case 'released':
        return now + SSE_RECONCILE_DEADLINE_MS
      case 'control':
      case 'urgent-flow-control':
        return now
      case 'heartbeat':
        return now + this.heartbeatFlushDelayMs
      case 'flow-control':
      case 'acknowledgement':
      case 'ack':
      case 'data':
        // From the last POST's start: a frame coming as that POST's credit returns would hold the next one longer.
        return Math.max(now + this.postIdleFlushDelayMs, this.lastPostStartedAt + this.flushThrottleMs)
    }
  }

  abandonActiveTransport(): void {
    const abortController = this.transportAbort
    if (!abortController) return
    this.transportAbort = null
    if (this.streamRequest.tag === 'active') this.streamRequest = { tag: 'idle' }
    this.closeAbandonedTransport()
    this.abandonedStream = abortController
    this.abandonedControllers.add(abortController)
    this.settleDrained()
  }

  closeAbandonedTransport(): void {
    const abortController = this.abandonedStream
    if (!abortController) return
    this.abandonedStream = null
    abortController.abort()
  }

  applyReconciledSettings(ctrl: ReconciledPayload): void {
    this.flushThrottleMs = ctrl.sseFlushThrottle
    this.postIdleFlushDelayMs = ctrl.ssePostIdleFlushDelay
  }

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    if (!this.hasWire()) return
    this.sendFrame({ kind: 'heartbeat', frame })
  }

  /** Its PINGs wait for a POST half the heartbeat's interval at most. */
  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
    this.heartbeatFlushDelayMs = Math.floor(hb.intervalMs / 2)
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  drained(): Promise<void> {
    if (!this.hasWire() || (!this.flushing && this.outbox.length === 0)) return Promise.resolve()
    return new Promise((resolve) => this.drainCallbacks.push(resolve))
  }

  private settleDrained(): void {
    for (const cb of this.drainCallbacks.splice(0)) cb()
  }

  dispose(): void {
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    this.flushScheduler.cancel()
    this.outbox = []
    this.outboxBytes = 0
    this.folded = null
    this.closeStreamRequest()
    this.transportAbort?.abort()
    this.transportAbort = null
    this.closeAbandonedTransport()
    this.settleDrained()
  }

  // ── Persistent client→server stream-request POST (half-duplex streaming body) ──

  private closeStreamRequest(): void {
    // Only 'active' has a body to close; 'failed' is sticky-terminal so don't regress to 'idle'.
    if (this.streamRequest.tag !== 'active') return
    this.streamRequest.body.close()
    this.streamRequest = { tag: 'idle' }
  }
}

function bytesOf(entries: readonly OutboxEntry[]): number {
  let bytes = 0
  for (const { frame } of entries) bytes += frame.byteLength
  return bytes
}

function isWindowRefresh({ frame }: OutboxEntry): boolean {
  return frame[0] === TAG.WINDOW || frame[0] === TAG.MSG_WINDOW
}

/** `onChunk`: each read, so an event taking longer than the pong deadline to arrive shows the wire alive meanwhile. */
function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
  onChunk: (bytes: number) => void,
): {
  cancel: () => void
  readNextEntry: () => Promise<Uint8Array<ArrayBuffer> | null>
} {
  const sse = new SSEEvents()
  const ready: Array<Uint8Array<ArrayBuffer>> = []
  let cancelled = false

  const cancel = () => {
    if (cancelled) return
    cancelled = true
    reader.cancel().catch(() => {})
  }

  abortController.signal.addEventListener('abort', cancel, { once: true })

  const onEvent = (event: Uint8Array<ArrayBuffer>) => void ready.push(event)

  const readNextEntry = async (): Promise<Uint8Array<ArrayBuffer> | null> => {
    while (true) {
      if (ready.length > 0) return ready.shift()!

      let done: boolean
      let value: Uint8Array<ArrayBuffer> | undefined
      let readError: unknown
      try {
        ;({ done, value } = await reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (abortController.signal.aborted || cancelled) return null
        throw readError ?? new Error('Connection lost before all SSE frames were received.')
      }
      onChunk(value!.byteLength)
      sse.push(value!, onEvent)
    }
  }

  return { cancel, readNextEntry }
}
