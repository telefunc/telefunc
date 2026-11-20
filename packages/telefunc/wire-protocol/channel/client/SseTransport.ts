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
  STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS,
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
import { createDeferred } from '../../../utils/createDeferred.js'
import { readAnswer, type HttpAnswer } from './readAnswer.js'

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
  get batched(): boolean {
    return this.streamRequest.tag !== 'active'
  }
  heartbeat: Heartbeat | null = null
  private connecting = false
  /** The wire's open POST is unanswered: the server holds a POST on the wire until it has the open. */
  private openPending = false
  private startTimer: ReturnType<typeof setTimeout> | null = null
  /** Aborts the active transport's fetches; null when no transport is active. */
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
  /** The streaming upload POST. Once `failed`, it stays so, and frames go in batch POSTs from the outbox. */
  private streamRequest:
    | { tag: 'idle' }
    | {
        tag: 'active'
        body: PushReadableStream<Uint8Array<ArrayBuffer>>
        /** Written before the server acknowledged the POST; null once it did. */
        unconfirmed: Uint8Array<ArrayBuffer>[] | null
      }
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
    // `keepalive`, as the request outlives the page; not `sendBeacon`, which can't send the request-kind header.
    void this.post(encodeSseRequest({ connId: this.connId, close: true }), undefined, { keepalive: true }).catch(
      () => {},
    )
    // The unload ends its requests next, which loses no wire: a WebSocket its upgrade opened says it closes for good.
    this.abandonActiveTransport()
  }

  async emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    if (this.streamRequest.tag === 'active') return this.emitBarrierStreamRequest(buildFrame)
    return this.emitBarrierBatch(buildFrame, signal)
  }

  /** The barrier goes last on the streaming upload, whose end owns its answer as for every frame on it. */
  private emitBarrierStreamRequest(buildFrame: () => OutboundFrame): BarrierEmission {
    assert(this.streamRequest.tag === 'active' && !this.flushing && this.outbox.length === 0)
    const frame = buildFrame()
    this.streamRequest.body.push(encodeU32(frame.frame.byteLength))
    this.streamRequest.body.push(frame.frame)
    this.closeStreamRequest()
    return 'emitted'
  }

  /** The barrier goes last in a POST of its own with the outbox, whatever its answer (see `sendStandalonePost`). */
  private async emitBarrierBatch(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    // The outbox goes with the barrier, so it waits only for the POST out, whose frames the server must take first.
    if (this.flushing) {
      let timer: ReturnType<typeof setTimeout> | undefined
      await new Promise<void>((resolve) => {
        this.barrierTurn = resolve
        timer = setTimeout(resolve, UPGRADE_DRAIN_TIMEOUT_MS)
      })
      clearTimeout(timer)
      this.barrierTurn = null
    }
    // A POST still out then is an upload, which the barrier would wait behind as long as it takes.
    if (this.flushing) return 'not-emitted'
    if (!this.hasWire() || signal.aborted) {
      // The turn the POST's end handed it held the outbox back: it goes on as that end would have sent it.
      this.flushNextOrDrain()
      return 'not-emitted'
    }
    this.flushScheduler.cancel()
    const queued = this.takeOutbox().map((entry) => entry.frame)
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
    const now = performance.now()
    const deadline = this.getFrameDeadline(frame.kind, now)
    const entry = { frame: frame.frame, deadline, queuedAt: now }
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
        stage.initialFrames.map((entry) => entry.frame),
      ),
      abortController.signal,
      { accept: 'text/event-stream' },
    )
    const upload = this.streamRequest.tag === 'failed' ? null : this.openUpload(abortController)
    this.owner.onTransportReconcileSent(this)

    const body = await this.openDownstream(ssePromise, stage, abortController)
    if (!body) return
    if (this.outbox.length > 0) void this.flushOutbox()

    const reader = createSseEventStreamReader(
      body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
      abortController,
      (bytes) => this.heartbeatOf(abortController)?.noteReceived(bytes),
    )
    // Run the SSE loop concurrently with the handshake wait — frames (including the first
    // RECONCILED) can arrive while the open-ack is still in flight and must be dispatched.
    const { promise: handshakeOkP, resolve: resolveHandshakeOk } = createDeferred<'ok'>()
    void this.readEvents(reader, abortController, () => resolveHandshakeOk('ok'))
    if (upload && !(await this.settleUpload(upload, handshakeOkP, abortController))) return

    this.connecting = false
    this.owner.onTransportOpen(this)
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
    if (answer.tag === 'otherWireVersion') {
      this.failOpen(stage, abortController, { otherWireVersion: true })
      return null
    }
    // A login page or an app's HTML fallback, also behind a followed redirect, is no event stream.
    if (answer.tag === 'refused' || answer.tag === 'tooLarge') {
      this.failOpen(stage, abortController, {
        refusal: response.ok
          ? `The server answered the SSE connection with ${answer.answer}, not an event stream`
          : `Server rejected SSE connection with ${answer.answer}`,
      })
      return null
    }
    assert(response.body)
    return response.body
  }

  /** Races the upload's acknowledgement against its end and a timeout; `false` when its wire was dropped meanwhile. */
  private async settleUpload(
    upload: StreamUpload,
    handshakeOk: Promise<'ok'>,
    abortController: AbortController,
  ): Promise<boolean> {
    const timeout = createDeferred<'timeout'>()
    const timer = setTimeout(() => timeout.resolve('timeout'), STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS)
    const result = await Promise.race([handshakeOk, timeout.promise, upload.ended])
    clearTimeout(timer)
    if (this.transportAbort !== abortController) {
      abortController.abort()
      return false
    }
    // Not acknowledged in time, or the POST ended: frames go in batch POSTs from now on.
    if (result === 'ok') this.confirmUpload(upload, abortController)
    else this.abandonUpload(upload)
    return true
  }

  /** The streaming upload POST, aborted with the wire's `abortController` or alone. */
  private openUpload(abortController: AbortController): StreamUpload {
    const abort = new AbortController()
    abortController.signal.addEventListener('abort', () => abort.abort(), { once: true })
    const body = createPushReadableStream<Uint8Array<ArrayBuffer>>()
    // Metadata header first — the server classifies the POST by it; `streamRequest: true`
    // makes it emit `reconciled` inline (the body never ends, can't defer to body-end).
    body.push(encodeSseRequestMetadata({ connId: this.connId, streamRequest: true }))
    const fetch = this.postAndRead(body.readable, abort.signal, { duplex: 'half' })
    this.streamRequest = { tag: 'active', body, unconfirmed: [] }
    // The POST ends with its body or when cut on the way; `ended` handles its rejection if openStream exits.
    return { body, abort, ended: answerOf(fetch) }
  }

  /** The server acknowledged the upload: what it was written is confirmed. */
  private confirmUpload(upload: StreamUpload, abortController: AbortController): void {
    if (this.streamRequest.tag === 'active') this.streamRequest.unconfirmed = null
    void this.endWireWithUpload(upload, abortController)
  }

  /** Ended while its body is still ours: nothing written to it reaches the server any more, so this wire ends. */
  private async endWireWithUpload({ body, ended }: StreamUpload, abortController: AbortController): Promise<void> {
    const answer = await ended
    if (this.streamRequest.tag !== 'active' || this.streamRequest.body !== body) return
    // Something in front refuses a body that never ends (a limit on its size): the next wires send in batch POSTs.
    if (answer?.tag === 'refused' || answer?.tag === 'tooLarge') {
      this.closeStreamRequest()
      this.streamRequest = { tag: 'failed' }
    }
    abortController.abort()
  }

  /** The upload went unacknowledged: frames go in batch POSTs from now on. */
  private abandonUpload({ abort }: StreamUpload): void {
    // The server may not have read a body it never acknowledged: resend it, first; it drops a seq it already has.
    const unsent = this.streamRequest.tag === 'active' ? this.streamRequest.unconfirmed : null
    // Aborted, not ended: a proxy holding the body forwards it once it ends, and the server would run it after what follows.
    abort.abort()
    this.streamRequest = { tag: 'failed' }
    const now = performance.now()
    if (unsent) this.requeue(unsent.map((frame) => ({ frame, deadline: now, queuedAt: now })))
    this.owner.onTransportBatched(this)
  }

  /** The downstream didn't open: what the reconcile took goes back to the outbox, and the wire closes. `answer`: what the
   *  server answered with, where it answered. */
  private failOpen(
    stage: SseInitialBatchStage,
    abortController: AbortController,
    answer: { refusal?: string; otherWireVersion?: boolean },
  ): void {
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
      if (!this.abandonedControllers.has(abortController)) this.owner.onTransportClosed(this)
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
   *  that refills the outbox goes so too. */
  private takeOutboxHead(): OutboxEntry[] {
    let count = 0
    let bytes = 0
    let body = encodeSseRequestMetadata({ connId: this.connId }).byteLength
    for (const { frame } of this.outbox) {
      const size = U32_SIZE + frame.byteLength
      if (count > 0 && (body >= this.postBytes || body + size > this.owner.ssePostLimit)) break
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
    const queued = this.takeOutboxHead()
    const head = this.foldHeading(queued)
    this.folded = null
    this.lastPostStartedAt = now
    const wire = this.transportAbort
    try {
      const body = encodeSseRequest(
        { connId: this.connId },
        queued.slice(head?.count ?? 0).map((entry) => entry.frame),
        head?.blob,
      )
      this.postOutBytes = body.size
      const answer = await answerOf(this.postWithDeadline(body, wire.signal))
      // Its wire ended already: what the POST carried goes as that wire's outbox does (stageInitialBatch), and its
      // answer, however late, says nothing of the wire after it.
      if (wire !== this.transportAbort) {
        if (answer?.tag !== 'expected') this.requeue(queued.filter(isWindowRefresh))
        return
      }
      if (answer?.tag === 'expected') {
        this.sizePosts(body.size, performance.now() - now)
        this.uplink.roundTrips.postAnswered(performance.now() - oldestQueuedAt(queued))
        return
      }
      if (answer?.tag === 'tooLarge') {
        this.owner.onTransportTooLarge(body.size)
        this.postBytes = Math.min(this.postBytes, this.owner.ssePostLimit)
        // Refused whole, for its size: the server has none of it, and its frames go again in smaller POSTs.
        if (queued.length > 1) {
          this.requeue(queued)
          return
        }
      }
      if (answer?.tag === 'refused' || answer?.tag === 'tooLarge') {
        // Every channel whose data it refused ended: the wire goes on without that data.
        if (
          !this.owner.onTransportRefused(
            queued.map(({ frame }) => frame),
            answer.answer,
          )
        ) {
          this.requeue(queued.filter((entry) => !isSequenced(entry)))
          return
        }
      }
      this.requeue(queued)
      this.abandonActiveTransport()
      this.owner.onTransportClosed(this, endOf(answer))
    } finally {
      this.flushing = false
      this.postOutBytes = 0
      if (this.barrierTurn) this.barrierTurn()
      else this.flushNextOrDrain()
      this.owner.onTransportHeadroom(this)
    }
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
    return this.batched ? this.postBytes - this.postOutBytes - this.outboxBytes : Infinity
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
      if (this.headroom() <= 0) void this.flushOutbox()
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

  /** Chrome reports a POST whose response is left unread as canceled. */
  private async postAndRead(body: BodyInit, signal: AbortSignal, extra?: { duplex: 'half' }): Promise<Response> {
    const response = await this.post(body, signal, extra)
    await response.arrayBuffer()
    return response
  }

  /** For frames that cannot wait for the next flush. Failure is swallowed, a refusal too, and neither is split nor sent
   *  again: its two senders are a heartbeat ping passing a POST out (its own deadline notices a wire whose PINGs don't
   *  arrive) and the barrier (delivered either way). */
  private async sendStandalonePost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
    if (!this.hasWire()) return
    assert(this.transportAbort)
    try {
      await this.postAndRead(encodeSseRequest({ connId: this.connId }, frames), this.transportAbort.signal)
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

  private getFrameDeadline(kind: OutboundFrameKind, now = performance.now()): number {
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

  dropQueued(ix: number): void {
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
    if (!this.hasWire() || (!this.flushing && this.outbox.length === 0)) return Promise.resolve()
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

/** `onChunk`: each read, so an event taking longer than the pong deadline to arrive shows the wire alive meanwhile. */
function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
  onChunk: (bytes: number) => void,
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
      onChunk(value.byteLength)
      sse.push(value, onEvent)
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
  if (answer.tag === 'otherWireVersion') return { otherWireVersion: true }
  return { refused: { answer: answer.answer, retryAfterMs: answer.retryAfterMs } }
}
