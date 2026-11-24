export { SseTransport }

import { assert } from '../../../utils/assert.js'
import {
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_TRANSPORT,
  LOSSY_CONFIRM_SLACK_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_FLOOR_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_POST_MAX_BYTES,
  SSE_POST_MIN_BYTES,
  SSE_POST_MIN_BYTES_PER_S,
  SSE_POST_TARGET_MS,
  SSE_RECONCILE_DEADLINE_MS,
  UPGRADE_DRAIN_TIMEOUT_MS,
} from '../../constants.js'
import { heartbeatHold } from '../../wire/pingDeadline.js'
import {
  U32_SIZE,
  encodeU32,
  encodeLengthPrefixedBlob,
  encodeSseRequest,
  encodeSseRequestMetadata,
} from '../../wire/envelope.js'
import { createPushReadableStream, type PushReadableStream } from '../../../utils/pushReadableStream.js'
import { Uplink } from '../../flowControl/client/Uplink.js'
import { Downlink } from '../../flowControl/client/Downlink.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../../wire/requestKind.js'
import { getWireUrl } from './getSessionUrl.js'
import { decode } from '../../wire/frame.js'
import { TAG, isSequencedTag } from '../../wire/tags.js'
import { readU16 } from '../../wire/header.js'
import { pairPublishes } from './pairPublishes.js'
import type { ReconciledPayload } from '../../wire/payloads.js'
import { DeadlineScheduler } from './DeadlineScheduler.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { Fifo } from '../../../utils/Fifo.js'
import { randomUuid } from '../../../utils/randomUuid.js'
import { settledOrAborted } from '../../../utils/settledOrAborted.js'
import type { OutboundFrame, OutboundFrameKind } from './ClientConnection.js'
import type { TransportEnd, TransportOwner } from './WireLifecycle.js'
import type { BarrierEmission, UpgradeSource } from './Upgrade.js'
import type { Heartbeat } from './Heartbeat.js'
import { asksLater, endOfWire, readAnswer, telefuncAnswer, telefuncEndOf, type HttpAnswer } from './readAnswer.js'

type OutboxEntry = { frame: Uint8Array<ArrayBuffer>; deadline: number; queuedAt: number }

/** A POST body built ahead from the outbox's first `count` entries, `first` through `last`. */
type OutboxFold = { blob: Blob; count: number; first: OutboxEntry; last: OutboxEntry }

type SseInitialBatchStage = {
  initialFrames: OutboundFrame[]
  movedOutbox: OutboxEntry[]
  movedBufferedFrames: OutboundFrame[]
}

/** A wire's streaming upload POST: its body, what aborts it alone, and its end: its answer, null where none came. */
type StreamUpload = {
  body: PushReadableStream<Uint8Array<ArrayBuffer>>
  abort: AbortController
  ended: Promise<HttpAnswer | null>
}

type SseEventStreamReader = ReturnType<typeof createSseEventStreamReader>

/** What `SseTransport.foldOutbox` adds to the next POST's body at a time. */
const SSE_FOLD_BYTES = 1024 * 1024

type SseTransportOptions = {
  telefuncUrl: string
  fetchImpl: typeof fetch
  userHeaders: Record<string, string> | undefined
  owner: TransportOwner
}

/** The SSE transport: an event stream down, batch POSTs or one streaming POST up. */
class SseTransport implements UpgradeSource {
  private readonly telefuncUrl: string
  private readonly fetchImpl: typeof fetch
  private readonly userHeaders: Record<string, string> | undefined
  private readonly owner: TransportOwner
  readonly type = CHANNEL_TRANSPORT.SSE
  readonly carriesReconcileInOpen = true
  /** Each wire's own, so a POST still in flight for a wire that died isn't dispatched on the next one. */
  private connId = randomUuid()
  uplink = new Uplink()
  downlink = new Downlink(this.uplink)
  /** Whether the wire's frames go in batch POSTs for good: a channel's window is then the batch-POST one. A wire whose
   *  upload awaits the server's acknowledgement sends in batch POSTs meanwhile, but isn't batched yet. */
  get batched(): boolean {
    return this.streamRequest.tag === 'idle' || this.streamRequest.tag === 'failed'
  }
  heartbeat: Heartbeat | null = null
  private connecting = false
  /** The wire's open POST is unanswered: the server holds a POST on the wire until it has the open. */
  private openPending = false
  private startTimer: ReturnType<typeof setTimeout> | null = null
  /** Aborts the active transport's fetches; null when no transport is active. */
  private transportAbort: AbortController | null = null
  private abandonedStream: AbortController | null = null
  /** When a read of the wire's event stream last ended inside an event: a frame was crossing. */
  private partwayAt = Number.NEGATIVE_INFINITY
  private readonly abandonedControllers = new WeakSet<AbortController>()
  private outbox: OutboxEntry[] = []
  /** Data whose POST failed or was refused, waiting out `heldTimer` till `heldUntil` (`hold`). */
  private held: OutboxEntry[] = []
  private heldTimer: ReturnType<typeof setTimeout> | null = null
  private heldUntil = 0
  private readonly flushScheduler = new DeadlineScheduler(() => {
    void this.flushOutbox()
  })
  private flushing = false
  /** Bytes pushed to the outbox since `foldOutbox` last folded: when to fold, not what. */
  private unfolded = 0
  /** The next POST's body, as far as `foldOutbox` built it. */
  private folded: OutboxFold | null = null
  private lastPostStartedAt = Number.NEGATIVE_INFINITY
  /** What a batch POST is sized to carry: what its bytes take `SSE_POST_TARGET_MS` to cross at the rate the last ones did. */
  private postBytes = SSE_POST_MIN_BYTES
  /** The body of the batch POST out, 0 with none. */
  private postOutBytes = 0
  /** The frames in `outbox`. */
  private outboxBytes = 0
  private flushThrottleMs = SSE_FLUSH_THROTTLE_MS
  private postIdleFlushDelayMs = SSE_POST_IDLE_FLUSH_DELAY_MS
  private heartbeatFlushDelayMs = heartbeatHold(CHANNEL_PING_INTERVAL_MS, CHANNEL_TRANSPORT.SSE)
  private drainCallbacks: Array<() => void> = []
  /** A barrier waiting for the POST out: it goes next, with the outbox. */
  private barrierTurn: (() => void) | null = null
  /** The wire's streaming upload POST. `probing`: it carries its metadata alone until the server acknowledges it, and
   *  frames go in batch POSTs meanwhile; `acknowledged`, it waits for the POST out (`moveToUpload`). `active`: frames go
   *  on it. `idle`: none on this wire, the next wire probes again. `failed`: an answer said the path refuses a body that
   *  streams, and no wire of the connection uploads again. */
  private streamRequest:
    | { tag: 'idle' }
    | { tag: 'probing'; upload: StreamUpload; acknowledged: boolean }
    | { tag: 'active'; upload: StreamUpload }
    | { tag: 'failed' } = { tag: 'idle' }

  constructor({ telefuncUrl, fetchImpl, userHeaders, owner }: SseTransportOptions) {
    this.telefuncUrl = telefuncUrl
    this.fetchImpl = fetchImpl
    this.userHeaders = userHeaders
    this.owner = owner
    if (typeof window !== 'undefined') window.addEventListener('pagehide', this.onPageHide)
  }

  /** A page that leaves for good closes its wire, as a WebSocket's does; one kept in the back/forward cache keeps it.
   *  Nothing reads the answer of its POST: the page is gone, and the server's ping deadline ends a wire it never got. */
  private readonly onPageHide = (event: PageTransitionEvent): void => {
    if (event.persisted) return
    this.postClose()
    // The unload ends its requests next, which loses no wire: a WebSocket its upgrade opened says it closes for good.
    this.abandonActiveTransport()
  }

  /** Tells the server the wire closes for good. */
  private postClose(): void {
    // `keepalive`, as the request outlives the page; not `sendBeacon`, which can't send the request-kind header.
    void this.post(encodeSseRequest({ connId: this.connId, close: true }), undefined, { keepalive: true }).catch(
      () => {},
    )
  }

  async emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    if (this.streamRequest.tag === 'active') return this.emitBarrierStreamRequest(buildFrame)
    // The page leaves this wire: an upload not yet carrying anything goes with it.
    if (this.streamRequest.tag === 'probing') this.closeStreamRequest()
    return this.emitBarrierBatch(buildFrame, signal)
  }

  /** The barrier goes last on the streaming upload, whose end owns its answer as for every frame on it. */
  private emitBarrierStreamRequest(buildFrame: () => OutboundFrame): BarrierEmission {
    assert(this.streamRequest.tag === 'active' && !this.flushing && this.outbox.length === 0)
    const frame = buildFrame()
    this.streamRequest.upload.body.push(encodeU32(frame.frame.byteLength))
    this.streamRequest.upload.body.push(frame.frame)
    this.closeStreamRequest()
    return 'emitted'
  }

  /** The barrier goes last in a POST of its own with the outbox, whatever its answer (see `sendStandalonePost`). Past a
   *  size the page learned, the outbox's data goes ahead in POSTs of its own, under the same byte rule and with the
   *  answers every POST has (`flushOutbox`). It ends: sends are held from the barrier's emission (`gatesSends`). */
  private async emitBarrierBatch(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    // The outbox goes with the barrier, so it waits only for the POST out, whose frames the server must take first.
    if (!(await this.takeTurn())) return 'not-emitted'
    while (this.owner.ssePostLimit < Infinity && this.outbox.some(isSequenced) && this.hasWire() && !signal.aborted) {
      void this.flushOutbox()
      if (!this.flushing) break
      if (!(await this.takeTurn())) return 'not-emitted'
    }
    if (!this.hasWire() || signal.aborted) {
      // The turn the POST's end handed it held the outbox back: it goes on as that end would have sent it.
      this.flushNextOrDrain()
      return 'not-emitted'
    }
    this.flushScheduler.cancel()
    const queued = this.takeOutbox().map((entry) => entry.frame)
    this.dropHeld()
    queued.push(buildFrame().frame)
    // Not `flushOutbox`: it re-queues on failure, and a barrier must never be replayed onto the
    // next wire. `emitted` is already decided — this POST cannot report whether the server saw it,
    // and a barrier that may have arrived counts as arrived. The wait only delays the flip.
    await settledOrAborted(this.sendStandalonePost(queued), signal)
    return 'emitted'
  }

  /** Waits for the POST out to end, `UPGRADE_DRAIN_TIMEOUT_MS` at most; its end hands the turn here (`barrierTurn`)
   *  instead of sending the outbox. Whether no POST is out then: one still out is an upload, which the barrier would
   *  wait behind as long as it takes. */
  private async takeTurn(): Promise<boolean> {
    if (this.flushing) {
      let timer: ReturnType<typeof setTimeout> | undefined
      await new Promise<void>((resolve) => {
        this.barrierTurn = resolve
        timer = setTimeout(resolve, UPGRADE_DRAIN_TIMEOUT_MS)
      })
      clearTimeout(timer)
      this.barrierTurn = null
    }
    return !this.flushing
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

  cancelAttempt(): void {
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    const abortController = this.transportAbort
    if (!abortController) return
    this.transportAbort = null
    this.openPending = false
    // Its open, upload and reader end with it, and say nothing: the open's frames go back to the outbox.
    this.abandonedControllers.add(abortController)
    this.closeStreamRequest()
    abortController.abort()
    this.settleDrained()
  }

  /** When the wire last delivered part of a frame and not its end: a frame was still crossing. */
  get frameCrossingAt(): number {
    return this.partwayAt
  }

  hasWire(): boolean {
    return this.transportAbort !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    if (this.streamRequest.tag === 'active') {
      this.streamRequest.upload.body.push(encodeU32(frame.frame.byteLength))
      this.streamRequest.upload.body.push(frame.frame)
      return
    }
    const now = performance.now()
    const own = this.getFrameDeadline(frame.kind, now)
    const deadline = frame.released ? Math.min(own, now + SSE_RECONCILE_DEADLINE_MS) : own
    const entry = { frame: frame.frame, deadline, queuedAt: now }
    if (this.held.length > 0 && isSequenced(entry)) {
      const ix = readU16(entry.frame, 1)
      if (this.held.some((held) => isDataOf(held, ix))) {
        this.held.push(entry)
        return
      }
    }
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
    if (deadline <= now || this.headroom() <= 0) void this.flushOutbox()
  }

  /** While a POST is out, folds the outbox into the next body a MiB at a time: the flush after copies only the rest. */
  private foldOutbox(): void {
    if (this.unfolded < SSE_FOLD_BYTES) return
    this.unfolded = 0
    const folded = this.foldHeading(this.outbox)
    const rest = this.outbox.slice(folded?.count ?? 0).map((entry) => entry.frame)
    this.folded = {
      blob: encodeLengthPrefixedBlob(rest, folded?.blob),
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
    const startedAt = performance.now()
    this.connId = randomUuid()
    this.uplink = new Uplink()
    this.downlink = new Downlink(this.uplink)
    const abortController = new AbortController()
    this.transportAbort = abortController
    const stage = this.stageInitialBatch()

    // The event stream and the upload's probe go together; frames go in batch POSTs until the server acknowledges it.
    const ssePromise = this.post(
      encodeSseRequest(
        { connId: this.connId, streamResponse: true },
        stage.initialFrames.map((entry) => entry.frame),
      ),
      abortController.signal,
      { accept: 'text/event-stream' },
    )
    const upload = this.streamRequest.tag === 'failed' ? null : this.openUpload(abortController)
    this.owner.onTransportReconcileSent(this)

    const body = await this.openDownstream(ssePromise, stage, abortController)
    if (!body) return
    this.owner.onTransportAnswered(this)
    if (this.outbox.length > 0) void this.flushOutbox()

    const reader = createSseEventStreamReader(
      body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
      abortController,
      (bytes, partway) => {
        const heartbeat = this.heartbeatOf(abortController)
        if (heartbeat === null) return
        const now = performance.now()
        heartbeat.noteReceived(bytes, now)
        if (partway) this.partwayAt = now
      },
    )
    void this.readEvents(reader, abortController, () => {
      if (upload) this.acknowledgeUpload(upload, abortController)
    })
    this.connecting = false
    this.owner.onTransportOpen(this, startedAt)
    if (this.outbox.length > 0) void this.flushOutbox()
  }

  /** The downstream's body, or `null` when it didn't open or its wire was dropped meanwhile. */
  private async openDownstream(
    ssePromise: Promise<Response>,
    stage: SseInitialBatchStage,
    abortController: AbortController,
  ): Promise<ReadableStream<Uint8Array> | null> {
    let response: Response
    this.openPending = true
    try {
      response = await ssePromise
    } catch {
      // Its wire was dropped already: what the open carried goes as that wire's outbox does (see `flushOutbox`).
      if (this.transportAbort !== abortController) this.requeue(stage.movedOutbox)
      else this.failOpen(stage, abortController, {})
      return null
    } finally {
      if (this.transportAbort === abortController) this.openPending = false
    }
    // Its wire was dropped while the open went unanswered: the answer is ignored, as a successor may be opening.
    if (this.transportAbort !== abortController) {
      abortController.abort()
      return null
    }
    const answer = readAnswer(response, 'event-stream')
    if (telefuncAnswer(answer)) {
      this.failOpen(stage, abortController, telefuncEndOf(answer))
      return null
    }
    // A login page or an app's HTML fallback, also behind a followed redirect, is no event stream.
    if (answer.tag === 'refused' || answer.tag === 'tooLarge') {
      this.failOpen(stage, abortController, {
        refusal: response.ok
          ? `The server answered the SSE connection with ${answer.answer}, not an event stream`
          : `Server rejected SSE connection with ${answer.answer}`,
        retryAfterMs: answer.retryAfterMs,
      })
      return null
    }
    assert(response.body)
    return response.body
  }

  /** The streaming upload POST, its metadata alone, aborted with the wire's `abortController` or alone: the probe. */
  private openUpload(abortController: AbortController): StreamUpload {
    const abort = new AbortController()
    abortController.signal.addEventListener('abort', () => abort.abort(), { once: true })
    const body = createPushReadableStream<Uint8Array<ArrayBuffer>>()
    // Metadata header first — the server classifies the POST by it; `streamRequest: true`
    // makes it emit `reconciled` inline (the body never ends, can't defer to body-end).
    body.push(encodeSseRequestMetadata({ connId: this.connId, streamRequest: true }))
    const fetch = this.postAndRead(body.readable, abort.signal, { duplex: 'half' })
    // The POST ends with its body or when cut on the way; `ended` handles its rejection if openStream exits.
    const upload = { body, abort, ended: answerOf(fetch) }
    this.streamRequest = { tag: 'probing', upload, acknowledged: false }
    void this.readUploadEnd(upload, abortController)
    return upload
  }

  /** The server acknowledged `upload`, the probe of the wire `abortController` aborts: what it reads from it reaches the
   *  server as it is sent. An acknowledgement on a wire the page left, or of a probe dropped, moves nothing. */
  private acknowledgeUpload(upload: StreamUpload, abortController: AbortController): void {
    const probing = this.streamRequest
    if (this.transportAbort !== abortController || probing.tag !== 'probing' || probing.upload !== upload) return
    probing.acknowledged = true
    this.moveToUpload()
  }

  /** An acknowledged probe takes the wire's frames once no POST is out: the server reads the upload's frames after
   *  what the POST out carried, which it took before answering, so no seq passes an earlier one. What waits goes first,
   *  in its order: held data still owed, then the outbox. */
  private moveToUpload(): void {
    const probing = this.streamRequest
    if (probing.tag !== 'probing' || !probing.acknowledged || this.flushing) return
    this.flushScheduler.cancel()
    this.folded = null
    const held = this.held.filter((entry) => this.owner.stillOwed(entry.frame))
    this.dropHeld()
    const waiting = held.concat(this.takeOutbox())
    const { body } = probing.upload
    this.streamRequest = { tag: 'active', upload: probing.upload }
    for (const { frame } of waiting) {
      body.push(encodeU32(frame.byteLength))
      body.push(frame)
    }
    this.settleDrained()
    this.owner.onTransportHeadroom(this)
  }

  /** The upload's end, while it is the wire's, read as a batch POST's answer is (`readAnswer`, `asksLater`). A rejected
   *  probe carried nothing: it is dropped and nothing is remembered, the wire goes on in batch POSTs and the next one
   *  probes again. An answer about the request says the path doesn't carry a body that streams, and no wire uploads
   *  again: a 4xx not Telefunc's, or any answer to a probe the server never acknowledged, as from something that took
   *  its body whole. A probe so answered carried nothing, and its wire goes on in batch POSTs. Anything else ends the
   *  wire as on the batch path: Telefunc's own, or a loss (an answer that asks for later; none, or the end of an upload
   *  that carried frames). */
  private async readUploadEnd(upload: StreamUpload, abortController: AbortController): Promise<void> {
    const answer = await upload.ended
    const state = this.streamRequest
    if (this.transportAbort !== abortController || state.tag === 'idle' || state.tag === 'failed') return
    if (state.upload !== upload) return
    const probing = state.tag === 'probing'
    if (answer === null && probing) {
      this.streamRequest = { tag: 'idle' }
      this.owner.onTransportBatched(this)
      return
    }
    const asksAgain =
      answer === null || telefuncAnswer(answer) || (answer.tag === 'refused' && asksLater(answer.status))
    if (!asksAgain && probing) {
      this.streamRequest = { tag: 'failed' }
      this.owner.onTransportBatched(this)
      return
    }
    if (!asksAgain && answer.tag !== 'expected') this.streamRequest = { tag: 'failed' }
    this.abandonActiveTransport()
    this.owner.onTransportClosed(this, endOf(answer))
  }

  /** The downstream didn't open: what the reconcile took goes back to the outbox, and the wire closes. `answer`: what the
   *  server answered with, where it answered. */
  private failOpen(stage: SseInitialBatchStage, abortController: AbortController, answer: TransportEnd): void {
    this.rollbackInitialBatch(stage)
    this.closeStreamRequest()
    abortController.abort()
    this.transportAbort = null
    this.connecting = false
    this.owner.onTransportClosed(this, answer)
  }

  /** Dispatches the downstream's events until it ends; `onOpenAck` runs as the upload's acknowledgement arrives. */
  private async readEvents(reader: SseEventStreamReader, abortController: AbortController, onOpenAck: () => void) {
    const pair = pairPublishes()
    /** What the stream's last event says ended the wire, where the server decided its end; a loss where none says. */
    let end: TransportEnd | undefined
    try {
      while (true) {
        const raw = await reader.readNextEntry()
        if (!raw) break
        if (raw[0] === TAG.STREAM_REQUEST_OPEN_ACK) {
          onOpenAck()
          continue
        }
        const frame = decode(raw)
        if (frame.tag === TAG.END) {
          end = endOfWire(frame.refusal)
          break
        }
        if (frame.tag === TAG.PONG) {
          // A PONG still on its way on a wire the page left says nothing of the wire in use.
          if (this.transportAbort !== abortController) continue
          this.heartbeat?.resetPong()
          this.owner.onTransportPong(frame.ended)
          continue
        }
        const paired = pair(frame)
        if (paired) {
          this.owner.onTransportFrame(paired, this, raw.byteLength, this.abandonedControllers.has(abortController))
        }
      }
    } catch {
      // A failed read ends the wire as its end does.
    } finally {
      reader.cancel()
      // The old SSE reader's death must NOT trample a successor openStream's streamRequest /
      // transportAbort. Only mutate transport state if this controller is still the active one.
      if (this.transportAbort === abortController) {
        this.closeStreamRequest()
        this.transportAbort = null
        // Also where it ended while its upload still waited for an acknowledgement: no open is under way any more.
        this.connecting = false
        // Its batch POST still in flight settles now rather than hold the next wire's outbox.
        abortController.abort()
      }
      // Abandoned controllers are owned by a successor transport — don't notify closed.
      if (!this.abandonedControllers.has(abortController)) this.owner.onTransportClosed(this, end)
    }
  }

  /** The heartbeat of the wire `abortController` aborts; none once the page left that wire, whose stream then says
   *  nothing of the wire in use. */
  private heartbeatOf(abortController: AbortController): Heartbeat | null {
    return this.transportAbort === abortController ? this.heartbeat : null
  }

  /** What the open carries: the RECONCILE and the control frames buffered behind it, never split. The open's answer
   *  owns them: refused, the open fails (`failOpen`) and they go back to the outbox for the next wire. */
  private stageInitialBatch(): SseInitialBatchStage {
    const reconcileBatch = this.owner.stageInitialReconcileBatch()
    const initialFrames: OutboundFrame[] = []
    initialFrames.push(reconcileBatch.reconcileFrame)
    const movedBufferedFrames = reconcileBatch.movedBufferedFrames
    // Sequenced frames replay after RECONCILED: sent now, they'd pass a POST in flight, making its frames duplicates.
    const movedOutbox = this.takeOutbox().filter(isWindowRefresh)
    // So does what waited behind a failed try on a wire before, however that wire ended.
    this.dropHeld()
    for (const entry of movedOutbox) initialFrames.push({ kind: 'data', frame: entry.frame })
    for (const frame of movedBufferedFrames) initialFrames.push(frame)
    return { initialFrames, movedOutbox, movedBufferedFrames }
  }

  private rollbackInitialBatch(stage: SseInitialBatchStage): void {
    if (stage.movedOutbox.length === 0 && stage.movedBufferedFrames.length === 0) return
    const now = performance.now()
    const movedBuffered: OutboxEntry[] = stage.movedBufferedFrames.map((entry) => ({
      frame: entry.frame,
      deadline: this.getFrameDeadline(entry.kind, now),
      queuedAt: now,
    }))
    this.requeue(stage.movedOutbox.concat(movedBuffered))
  }

  /** Empties the outbox, and returns what it held. */
  private takeOutbox(): OutboxEntry[] {
    const entries = this.outbox
    this.outbox = []
    this.outboxBytes = 0
    return entries
  }

  /** Takes what the next POST carries from the head of the outbox, in bytes as its body goes on the wire (its metadata,
   *  and four bytes of length before each frame): entries while the body comes to less than `postBytes`, which one may
   *  pass, and never past the connection's `ssePostLimit`, except a first entry larger alone, which goes alone. A replay
   *  that refills the outbox goes so too. The data of a channel the server hasn't confirmed since a loss goes in POSTs
   *  of its own, so that a refusal or a cut of it takes no other channel's data with it. */
  private takeOutboxHead(): OutboxEntry[] {
    let count = 0
    let bytes = 0
    let body = encodeSseRequestMetadata({ connId: this.connId }).byteLength
    const stalled = this.owner.stalledChannels
    let carries: number | 'others' | null = null
    for (const { frame } of this.outbox) {
      const size = U32_SIZE + frame.byteLength
      if (count > 0 && (body >= this.postBytes || body + size > this.owner.ssePostLimit)) break
      const tag = frame[0]
      if (stalled.size > 0 && tag !== undefined && isSequencedTag(tag)) {
        const ix = readU16(frame, 1)
        const of = stalled.has(ix) ? ix : 'others'
        if (carries !== null && carries !== of) break
        carries = of
      }
      body += size
      bytes += frame.byteLength
      count++
    }
    if (count === this.outbox.length) return this.takeOutbox()
    this.outboxBytes -= bytes
    return this.outbox.splice(0, count)
  }

  /** Puts `entries` back at the head of the outbox. */
  private requeue(entries: OutboxEntry[]): void {
    this.outbox = entries.concat(this.outbox)
    this.outboxBytes += bytesOf(entries)
  }

  private async flushOutbox(): Promise<void> {
    // Each POST held for the open would take one of the browser's connections to the host, which the open may need.
    if (!this.hasWire() || this.openPending || this.flushing || this.outbox.length === 0) return
    assert(this.transportAbort)
    this.flushScheduler.cancel()
    this.flushing = true
    const now = performance.now()
    // One that goes before its deadline, because the outbox is full, waited for no throttle, so the next frame doesn't
    // wait for one after it either.
    if (this.outbox.some(({ deadline }) => deadline <= now)) this.lastPostStartedAt = now
    const queued = this.takeOutboxHead()
    const head = this.foldHeading(queued)
    this.folded = null
    const wire = this.transportAbort
    try {
      const body = encodeSseRequest(
        { connId: this.connId },
        queued.slice(head?.count ?? 0).map((entry) => entry.frame),
        head?.blob,
      )
      this.postOutBytes = body.size
      const answer = await this.answerOn(this.postWithDeadline(body, wire.signal), wire)
      // Its wire ended already: what the POST carried goes as that wire's outbox does (stageInitialBatch). A window
      // refresh goes again whatever it was answered: its limit is cumulative.
      if (answer === 'left') {
        this.requeue(queued.filter(isWindowRefresh))
        return
      }
      if (answer?.tag === 'expected') {
        this.sizePosts(body.size, performance.now() - now)
        this.uplink.roundTrips.postAnswered(performance.now() - oldestQueuedAt(queued))
        return
      }
      // Telefunc's own: the server no longer has the wire, ended a channel or the page's session, or isn't served so.
      // Nothing of it is counted as a refusal of the data.
      if (telefuncAnswer(answer)) {
        this.requeue(queued)
        this.abandonActiveTransport()
        this.owner.onTransportClosed(this, telefuncEndOf(answer))
        return
      }
      if (answer?.tag === 'tooLarge') {
        this.owner.onTransportTooLarge(body.size)
        this.postBytes = Math.min(this.postBytes, this.owner.ssePostLimit)
        // Refused whole, for its size: the server has none of it, and its frames go again in smaller POSTs, once the
        // wait the answer asked for is out.
        if (queued.length > 1) {
          if (answer.retryAfterMs > 0) this.hold(queued, answer.retryAfterMs)
          else this.requeue(queued)
          return
        }
      }
      const counted = countedAnswer(answer)
      if (counted !== null) {
        // Every channel whose data it refused ended: the wire goes on without that data.
        if (
          !this.owner.onTransportRefused(
            queued.map(({ frame }) => frame),
            counted,
          )
        ) {
          this.requeue(queued.filter((entry) => !isSequenced(entry)))
          return
        }
      }
      // Stalled channels' data alone, unanswered or asked to come later, while the event stream is up: the wire goes on,
      // and that data goes again on the stalled schedule (that try is those channels' loss), no sooner than the answer
      // asked.
      const askedLater = answer?.tag === 'refused' && asksLater(answer.status) ? answer : null
      const cause =
        answer === null
          ? 'The request carrying it got no answer'
          : askedLater !== null
            ? `The server refused what the page sent with ${askedLater.answer}`
            : null
      if (cause !== null && this.carriesStalledOnly(queued)) {
        this.hold(queued, Math.max(this.owner.onStalledPostLost(cause), askedLater?.retryAfterMs ?? 0))
        return
      }
      this.requeue(queued)
      this.abandonActiveTransport()
      this.owner.onTransportClosed(this, endOf(answer))
    } finally {
      this.flushing = false
      this.postOutBytes = 0
      if (this.barrierTurn) this.barrierTurn()
      else {
        this.moveToUpload()
        this.flushNextOrDrain()
      }
      this.owner.onTransportHeadroom(this)
    }
  }

  /** Whether `queued` carries sequenced data, all of channels whose data the server hasn't confirmed since a loss. */
  private carriesStalledOnly(queued: OutboxEntry[]): boolean {
    const stalled = this.owner.stalledChannels
    const data = queued.filter(isSequenced)
    return data.length > 0 && data.every(({ frame }) => stalled.has(readU16(frame, 1)))
  }

  /** The data `queued` carried waits, out of the outbox, at least `wait` ms, behind one wait for the whole wire, and
   *  goes again then if the server still lacks it; what else it carried goes on at once. */
  private hold(queued: OutboxEntry[], wait: number): void {
    this.requeue(queued.filter((entry) => !isSequenced(entry)))
    const held = queued.filter((entry) => isSequenced(entry) && this.owner.stillOwed(entry.frame))
    // What its channels sent since, queued or held, waits behind it: the server drops a seq below one it has.
    const ixs = new Set(held.map(({ frame }) => readU16(frame, 1)))
    const later = this.outbox.filter((entry) => isSequenced(entry) && ixs.has(readU16(entry.frame, 1)))
    if (later.length > 0) {
      this.outbox = this.outbox.filter((entry) => !later.includes(entry))
      this.outboxBytes -= bytesOf(later)
    }
    this.held = held.concat(this.held, later)
    const until = performance.now() + wait
    if (this.held.length === 0 || (this.heldTimer !== null && until <= this.heldUntil)) return
    if (this.heldTimer) clearTimeout(this.heldTimer)
    this.heldUntil = until
    this.heldTimer = setTimeout(() => this.releaseHeld(), wait)
  }

  /** The wait ended: what the server still lacks goes back to the head of the outbox. */
  private releaseHeld(): void {
    this.heldTimer = null
    const held = this.held.filter((entry) => this.owner.stillOwed(entry.frame))
    this.held = []
    this.requeue(held)
    if (this.outbox.length > 0) void this.flushOutbox()
    else this.flushNextOrDrain()
  }

  /** The held data goes with its wire, as the next one opens, or with the barrier that leaves it: the next wire's replay
   *  sends what the server lacks. */
  private dropHeld(): void {
    if (this.heldTimer) clearTimeout(this.heldTimer)
    this.heldTimer = null
    this.held = []
  }

  /** Sizes next POSTs to what this one's rate carries in `SSE_POST_TARGET_MS`, unless it was quicker and not full. */
  private sizePosts(bytes: number, ms: number): void {
    if (ms < SSE_POST_TARGET_MS && bytes < this.postBytes) return
    this.postBytes = Math.min(
      this.owner.ssePostLimit,
      SSE_POST_MAX_BYTES,
      Math.max(SSE_POST_MIN_BYTES, (bytes * SSE_POST_TARGET_MS) / Math.max(1, ms)),
    )
  }

  /** Its frames wait for their POST's turn: the throttle, or the POST ahead of them, which takes a round trip. */
  get behindAfter(): number {
    const roundTrip = this.uplink.roundTrips.pathRtt
    return LOSSY_CONFIRM_SLACK_MS + Math.max(this.flushThrottleMs, roundTrip) + roundTrip
  }

  headroom(): number {
    return this.streamRequest.tag === 'active' ? Infinity : this.postBytes - this.postOutBytes - this.outboxBytes
  }

  /** A batch POST out past its deadline is given up, as a wire that missed its pong: its wire reconnects, resending what
   *  it carried. */
  private async postWithDeadline(body: Blob, wireSignal: AbortSignal): Promise<Response> {
    const post = new AbortController()
    const abort = () => post.abort()
    const deadline = setTimeout(abort, SSE_POST_FLOOR_MS + (body.size * 1000) / SSE_POST_MIN_BYTES_PER_S)
    wireSignal.addEventListener('abort', abort, { once: true })
    try {
      return await this.postAndRead(body, post.signal)
    } finally {
      clearTimeout(deadline)
      wireSignal.removeEventListener('abort', abort)
    }
  }

  /** After a POST: the outbox goes next, or those waiting for the drain are told it's empty. */
  private flushNextOrDrain(): void {
    if (this.outbox.length > 0) {
      // As in `sendFrame`: a frame whose deadline passed while the POST was out goes now, as does a whole POST.
      const now = performance.now()
      if (this.headroom() <= 0 || this.outbox.some(({ deadline }) => deadline <= now)) void this.flushOutbox()
      else this.scheduleFlush()
      return
    }
    if (this.held.length === 0) this.settleDrained()
  }

  /** A PING still held at its deadline goes alone: the next POST would bring it past the server's ping deadline. */
  private unholdPing(entry: OutboxEntry): void {
    setTimeout(
      () => {
        const at = this.outbox.indexOf(entry)
        if (!this.flushing || at === -1) return
        this.outbox.splice(at, 1)
        this.outboxBytes -= entry.frame.byteLength
        void this.sendPingAlone(entry.frame)
      },
      Math.max(0, entry.deadline - performance.now()),
    )
  }

  /** Every request this transport makes. Its senders differ only in what they send and what
   *  a failure means to them, so that is all they say. */
  private post(
    body: BodyInit,
    signal: AbortSignal | undefined,
    extra?: { accept?: string; duplex?: 'half'; keepalive?: true },
  ): Promise<Response> {
    const headers: Record<string, string> = {
      ...this.userHeaders,
      'Content-Type': 'application/octet-stream',
      [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
    }
    if (extra?.accept) headers.Accept = extra.accept
    return this.fetchImpl(getMarkedRequestUrl(getWireUrl(this.telefuncUrl), REQUEST_KIND.SSE), {
      method: 'POST',
      headers,
      body,
      signal,
      keepalive: extra?.keepalive,
      // @ts-ignore duplex is not yet in TypeScript's RequestInit
      duplex: extra?.duplex,
    })
  }

  /** The answer to a POST that went on `wire`, read; null where none came. `left` where the page left that wire
   *  meanwhile: an answer, however late, says nothing of the wire after it, so this is where it stops, before the size
   *  it would teach, the refusal it would count and the round trip it would sample. */
  private async answerOn(request: Promise<Response>, wire: AbortController): Promise<HttpAnswer | null | 'left'> {
    const answer = await answerOf(request)
    return wire === this.transportAbort ? answer : 'left'
  }

  /** Chrome reports a POST whose response is left unread as canceled. */
  private async postAndRead(body: BodyInit, signal: AbortSignal, extra?: { duplex: 'half' }): Promise<Response> {
    const response = await this.post(body, signal, extra)
    await response.arrayBuffer()
    return response
  }

  /** A PING that passes the POST out, alone. Its answer goes to the decision every POST's goes to: a refusal is counted
   *  (`onTransportRefused`), and ends the wire as a refused POST does. Only for the wire it went on; a network failure
   *  is the heartbeat's deadline to notice. */
  private async sendPingAlone(frame: Uint8Array<ArrayBuffer>): Promise<void> {
    if (!this.hasWire()) return
    const wire = this.transportAbort
    assert(wire)
    const answer = await this.answerOn(
      this.postAndRead(encodeSseRequest({ connId: this.connId }, [frame]), wire.signal),
      wire,
    )
    if (answer === 'left' || answer === null || answer.tag === 'expected') return
    const counted = countedAnswer(answer)
    if (counted !== null && !this.owner.onTransportRefused([frame], counted)) return
    this.abandonActiveTransport()
    this.owner.onTransportClosed(this, endOf(answer))
  }

  /** For the barrier, which cannot wait for the next flush and is delivered either way: it is neither split nor sent
   *  again. Telefunc's own answer ends the wire as a batch POST's does; any other, or none, is swallowed, since the
   *  barrier may have arrived. */
  private async sendStandalonePost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
    if (!this.hasWire()) return
    const wire = this.transportAbort
    assert(wire)
    const answer = await this.answerOn(
      this.postAndRead(encodeSseRequest({ connId: this.connId }, frames), wire.signal),
      wire,
    )
    if (!telefuncAnswer(answer)) return
    this.abandonActiveTransport()
    this.owner.onTransportClosed(this, telefuncEndOf(answer))
  }

  /** Not the control frames waiting for the next POST: a channel out of credit with only those waiting was starved. */
  bufferedAmount(): number {
    if (this.streamRequest.tag === 'active') return this.streamRequest.upload.body.bufferedAmount
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

  private getFrameDeadline(kind: OutboundFrameKind, now = performance.now()): number {
    switch (kind) {
      case 'reconcile':
        return now + SSE_RECONCILE_DEADLINE_MS
      // The app awaits the server's answer to an `ack` send: nothing that could join its POST brings that sooner.
      case 'control':
      case 'urgent-flow-control':
      case 'ack':
        return now
      case 'heartbeat':
        return now + this.heartbeatFlushDelayMs
      case 'flow-control':
      case 'acknowledgement':
      case 'data':
        // From the last POST's start: a frame coming as that POST's credit returns would hold the next one longer.
        return Math.max(now + this.postIdleFlushDelayMs, this.lastPostStartedAt + this.flushThrottleMs)
    }
  }

  dropQueued(ix: number): void {
    this.held = this.held.filter((entry) => !isDataOf(entry, ix))
    const dropped = this.outbox.filter((entry) => isDataOf(entry, ix))
    if (dropped.length === 0) return
    this.outbox = this.outbox.filter((entry) => !isDataOf(entry, ix))
    this.outboxBytes -= bytesOf(dropped)
  }

  abandonActiveTransport(): void {
    const abortController = this.transportAbort
    if (!abortController) return
    this.transportAbort = null
    // An open still unanswered goes with its wire: the next `start()` opens another.
    this.connecting = false
    this.openPending = false
    // Its upload goes with it, aborted with its controller (`openUpload`).
    if (this.streamRequest.tag === 'active' || this.streamRequest.tag === 'probing')
      this.streamRequest = { tag: 'idle' }
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

  /** Its PINGs wait for a POST as long as the ping deadline allows the page to hold them. */
  attachHeartbeat(heartbeat: Heartbeat): void {
    this.heartbeat = heartbeat
    this.heartbeatFlushDelayMs = heartbeatHold(heartbeat.intervalMs, CHANNEL_TRANSPORT.SSE)
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  drained(): Promise<void> {
    if (!this.hasWire() || (!this.flushing && this.outbox.length === 0 && this.held.length === 0))
      return Promise.resolve()
    return new Promise((resolve) => this.drainCallbacks.push(resolve))
  }

  private settleDrained(): void {
    for (const cb of this.drainCallbacks.splice(0)) cb()
  }

  dispose(): void {
    if (typeof window !== 'undefined') window.removeEventListener('pagehide', this.onPageHide)
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    this.flushScheduler.cancel()
    this.takeOutbox()
    this.dropHeld()
    this.folded = null
    this.closeStreamRequest()
    // A wire still on as its connection ends closes for good, as a WebSocket's does with `WS_CLOSE_PAGE_LEFT`.
    if (this.transportAbort) this.postClose()
    this.transportAbort?.abort()
    this.transportAbort = null
    this.closeAbandonedTransport()
    this.settleDrained()
  }

  // ── Persistent client→server stream-request POST (half-duplex streaming body) ──

  /** The wire's upload ends: an active one with its body, so the server reads what it carried; a probe cut, as it
   *  carried nothing. `failed` stays the connection's. */
  private closeStreamRequest(): void {
    const state = this.streamRequest
    if (state.tag === 'active') state.upload.body.close()
    else if (state.tag === 'probing') state.upload.abort.abort()
    else return
    this.streamRequest = { tag: 'idle' }
  }
}

function oldestQueuedAt(entries: readonly OutboxEntry[]): number {
  let oldest = Infinity
  for (const { queuedAt } of entries) if (queuedAt < oldest) oldest = queuedAt
  return oldest
}

function bytesOf(entries: readonly OutboxEntry[]): number {
  let bytes = 0
  for (const { frame } of entries) bytes += frame.byteLength
  return bytes
}

/** Whether `entry` holds a data frame of channel `ix`. */
function isDataOf(entry: OutboxEntry, ix: number): boolean {
  return isSequenced(entry) && readU16(entry.frame, 1) === ix
}

function isSequenced({ frame }: OutboxEntry): boolean {
  const tag = frame[0]
  return tag !== undefined && isSequencedTag(tag)
}

function isWindowRefresh({ frame }: OutboxEntry): boolean {
  return frame[0] === TAG.WINDOW || frame[0] === TAG.MSG_WINDOW
}

/** `onChunk`: each read that carried any of an event, so an event taking longer than the pong deadline to arrive shows
 *  the wire alive meanwhile; `partway` where the read ended inside one. */
function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
  onChunk: (bytes: number, partway: boolean) => void,
): {
  cancel: () => void
  readNextEntry: () => Promise<Uint8Array<ArrayBuffer> | null>
} {
  const sse = new SseEvents()
  const ready = new Fifo<Uint8Array<ArrayBuffer>>()
  let cancelled = false

  const cancel = () => {
    if (cancelled) return
    cancelled = true
    reader.cancel().catch(() => {})
  }

  abortController.signal.addEventListener('abort', cancel, { once: true })

  const onEvent = (event: Uint8Array<ArrayBuffer>) => ready.push(event)

  /** The next event, or `null` once the stream ended; a failed read rejects. */
  const readNextEntry = async (): Promise<Uint8Array<ArrayBuffer> | null> => {
    while (true) {
      const event = ready.shift()
      if (event !== undefined) return event
      const { done, value } = await reader.read()
      if (done) return null
      if (sse.push(value, onEvent)) onChunk(value.byteLength, sse.partway)
    }
  }

  return { cancel, readNextEntry }
}

/** The answer to `request`, read; null where none came. */
async function answerOf(request: Promise<Response>): Promise<HttpAnswer | null> {
  try {
    return readAnswer(await request, 'ok')
  } catch {
    return null
  }
}

/** Why a wire ends whose batch POST got `answer`: null where none came, a dropped connection. */
function endOf(answer: HttpAnswer | null): TransportEnd {
  if (answer === null || answer.tag === 'expected') return {}
  if (telefuncAnswer(answer)) return telefuncEndOf(answer)
  return { refused: answer.answer, retryAfterMs: answer.retryAfterMs }
}

/** What a refusal counted against the data it carried names (`onTransportRefused`); null where it isn't one: a
 *  refusal that asks for later, Telefunc's own, or none. */
function countedAnswer(answer: HttpAnswer | null | 'left'): string | null {
  if (answer === null || answer === 'left') return null
  if (answer.tag === 'tooLarge') return answer.answer
  if (answer.tag === 'refused') return asksLater(answer.status) ? null : answer.answer
  return null
}
