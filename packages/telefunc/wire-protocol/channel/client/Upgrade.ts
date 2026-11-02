export { Upgrade }
export type { BarrierEmission, ProbeOwner, ProbeSession, ProbeWire, UpgradeOwner, UpgradeSource, UpgradeTarget }

import type { AttachExchange } from './AttachExchange.js'
import type { ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport, OutboundFrame } from './ClientConnection.js'
import type { Liveness } from './Liveness.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { DecodedFrame } from '../../wire/frame.js'
import type { ReadyPayload, ReconciledPayload } from '../../wire/payloads.js'
import {
  CHANNEL_TRANSPORT,
  UPGRADE_ATTEMPT_TIMEOUT_MS,
  UPGRADE_HANDOFF_BUFFER_BYTES,
  UPGRADE_HANDOFF_BUFFER_FRAMES,
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS,
} from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { assert } from '../../../utils/assert.js'
import { randomUuid } from '../../../utils/randomUuid.js'
import { Heartbeat } from './Heartbeat.js'

/** The probe wire `WsTransport.probe` returns. Until the swap commits, its caller keeps it alive: `Upgrade` does so
 *  with a Heartbeat of its own. */
type ProbeWire = {
  ping: () => void
  /** Exchanges PREPARE/READY without the probe becoming the transport — adopting it early would put a sessionless
   *  wire in the connection's `transport`. */
  send: (frame: Uint8Array<ArrayBuffer>) => void
  /** Closes it; it tells its owner nothing more. */
  close: () => void
  /** The uplink of the wire it opens. */
  uplink: Uplink
}

/** What a probed WebSocket tells the upgrade, from the PONG that proves it until it closes or is adopted. */
interface ProbeOwner {
  onProbePong(): void
  onProbeFrame(frame: DecodedFrame, byteLength: number): void
  onProbeClosed(): void
}

type ProbeSession = {
  upgradeId: string
  sessionId: string
  probeHeartbeat: Heartbeat
  uplink: Uplink
}

/** Kept apart, as recovery differs: `emitted` may have reached the server, `not-emitted` leaves the wire usable. */
type BarrierEmission = 'emitted' | 'not-emitted'

/** The wire an upgrade moves FROM: it has to carry the barrier as its last frame. */
type UpgradeSource = ClientChannelTransport & {
  emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission>
}

/** The wire an upgrade moves TO: it has to be openable without becoming the transport, and
 *  adoptable once the barrier commits. */
type UpgradeTarget = ClientChannelTransport & {
  probe(owner: ProbeOwner): Promise<ProbeWire | null>
  adoptProbe(): void
}

type UpgradeState =
  | { tag: 'none' }
  | {
      tag: 'staging'
      attempt: AbortController
      deadline: ReturnType<typeof setTimeout> | null
      /** Keeps the probe alive once its first PONG proved it (see `keepProbeAlive`). */
      probeHeartbeat: Heartbeat | null
      /** Settles the wait for the probe's READY, once its PREPARE went. */
      settleReady: ((payload: ReadyPayload | null) => void) | null
      /** READY arrived: the barrier comes next, and a registration waits for the handoff. */
      ready: boolean
    }
  | {
      tag: 'committing'
      attempt: AbortController
      deadline: ReturnType<typeof setTimeout> | null
      from: UpgradeSource
      to: UpgradeTarget
      probeHeartbeat: Heartbeat
      upgradeId: string
      /** The join's two limbs, one per wire. Both must land before dispatching resumes. */
      finReceived: boolean
      committed: boolean
      /** Frames the probe delivered before the flip. The swap has not happened, so not even a
       *  control frame may be acted on yet; the flip is what releases them. */
      heldBeforeFlip: BufferedWireFrame[]
      buffer: UpgradeBuffer
      bufferedBytes: number
      bufferedFrames: number
      deferredOmitted: number[]
      joinTimer: ReturnType<typeof setTimeout> | null
    }

type CommittingUpgrade = Extract<UpgradeState, { tag: 'committing' }>

type BufferedWireFrame = { frame: DecodedFrame; byteLength: number }

/** Per source wire, `old` draining first: the server wrote all of it before anything the new wire carries. */
type UpgradeBuffer = { old: BufferedWireFrame[]; new: BufferedWireFrame[] }

/** FIN (old wire) and RECONCILED (new wire) are the join's two limbs; everything else is payload. */
function isJoinLimb(frame: DecodedFrame): boolean {
  return frame.tag === TAG.FIN || frame.tag === TAG.RECONCILED
}

/** The connection an upgrade moves from its SSE wire to a WebSocket. */
interface UpgradeOwner {
  readonly transport: ClientChannelTransport
  readonly connected: boolean
  drainBufferedFramesToWire(): void
  /** A WebSocket transport of the connection, its wire not opened yet. */
  createWebSocket(): UpgradeTarget
  /** The connection moves to `transport`, whose wire the upgrade opened. */
  adoptTransport(transport: UpgradeTarget): void
  dispatchFrame(frame: DecodedFrame): void
  handleReconciled(ctrl: ReconciledPayload): void
  fallbackToSse(err: Error): void
  startTtlIfIdle(): void
}

type UpgradeOptions = {
  owner: UpgradeOwner
  channels: ChannelTable
  liveness: Liveness
  exchange: AttachExchange
  /** The page's transports include a WebSocket. */
  allowsWebSocket: boolean
}

/** The SSE→WS upgrade: a WebSocket probe staged beside the SSE wire, the barrier that ends the old wire, and the join
 *  of both wires' frames once the connection flipped to the new one. */
class Upgrade implements ProbeOwner {
  private readonly owner: UpgradeOwner
  private readonly channels: ChannelTable
  private readonly liveness: Liveness
  private readonly exchange: AttachExchange
  private readonly allowsWebSocket: boolean
  /** Only while the connection is open: it goes with the open state it was in (see `forget`). */
  private state: UpgradeState = { tag: 'none' }
  /** Set by a permanent upgrade abort, and kept through every state change until dispose. */
  private disabled = false
  /** Server-allowed transports from the last settled RECONCILED. Kept so `maybeStart`
   *  can run from both `handleReconciled` and `onTransportOpen` — transport-open and the
   *  first RECONCILED can arrive in either order on the SSE path. */
  serverTransports: ReconciledPayload['transports'] | null = null

  constructor({ owner, channels, liveness, exchange, allowsWebSocket }: UpgradeOptions) {
    this.owner = owner
    this.channels = channels
    this.liveness = liveness
    this.exchange = exchange
    this.allowsWebSocket = allowsWebSocket
  }

  get committing(): CommittingUpgrade | null {
    return this.state.tag === 'committing' ? this.state : null
  }

  /** Whether the connection's transport is already the new wire. The flip happens inside `committing`; it is not a
   *  state of its own. */
  get flipped(): boolean {
    const u = this.committing
    return u !== null && this.owner.transport === u.to
  }

  /** Sends are gated from the barrier's emission until its COMMITTED lands. */
  get gatesSends(): boolean {
    const u = this.committing
    return u !== null && !u.committed
  }

  /** READY came, so the barrier goes next: a RECONCILE too, and no other goes out till the handoff. */
  get ready(): boolean {
    return this.state.tag === 'staging' && this.state.ready
  }

  /** The connection left its open state, and the upgrade in it. */
  forget(): void {
    this.state = { tag: 'none' }
  }

  maybeStart(): void {
    if (this.disabled) return
    if (!this.owner.connected || this.state.tag !== 'none') return
    // Settled-reconcile gate; a flush above may have just re-armed `reconciling` — the next
    // RECONCILED retries via `handleReconciled`.
    if (this.exchange.reconciling) return
    // Only SSE can carry a barrier as its last frame, and only SSE has anywhere to upgrade to.
    const { transport } = this.owner
    if (transport.type !== CHANNEL_TRANSPORT.SSE) return
    if (!this.allowsWebSocket) return
    if (!this.serverTransports?.includes(CHANNEL_TRANSPORT.WS)) return
    void this.probeAndUpgrade(transport as UpgradeSource)
  }

  /** False where `ctrl` answers another upgrade: a RECONCILED that does not echo the committing attempt's id. */
  onReconciled(ctrl: ReconciledPayload): boolean {
    const committing = this.committing
    if (committing === null || committing.committed) return true
    if (ctrl.upgradeId !== committing.upgradeId) return false
    committing.committed = true
    return true
  }

  /** An attempt not flipped yet stops: its wire went. */
  abortAttempt(): void {
    if (this.state.tag !== 'none' && !this.flipped) this.state.attempt.abort()
  }

  /** Tears down whichever upgrade phase is in flight. Returns the join's record when the flip had
   *  already happened — its buffered frames are then the caller's to drain — and null otherwise. */
  teardown(): CommittingUpgrade | null {
    const u = this.state
    if (u.tag === 'none') return null
    if (u.tag === 'committing' && this.owner.transport === u.to) {
      const record = this.exitCommitting()
      retireOldWire(record.from)
      return record
    }
    u.attempt.abort()
    this.exitAttempt(u.attempt)
    return null
  }

  /** The upgrade is given up for good: what the join held of the old wire is handed on, and none starts again. */
  abandon(): void {
    const abandoned = this.teardown()
    if (abandoned) {
      for (const entry of abandoned.buffer.old) this.owner.dispatchFrame(entry.frame)
      this.exchange.closeUnacknowledged(abandoned.deferredOmitted)
    }
    this.disabled = true
  }

  /** Takes every frame that arrives during the handoff, and is the one place that counts it against the handoff's
   *  budget. Before the flip the probe is nobody's wire, so even a join limb waits: acting on one would settle a swap
   *  that hasn't happened. After it, limbs apply and the rest waits, split by the wire it came from. */
  ingestDuringHandoff(frame: DecodedFrame, source: 'old' | 'new', byteLength: number): void {
    const u = this.committing
    if (u === null) return
    const flipped = this.owner.transport === u.to
    if (flipped && this.applyJoinLimb(frame)) return
    ;(flipped ? u.buffer[source] : u.heldBeforeFlip).push({ frame, byteLength })
    u.bufferedFrames += 1
    u.bufferedBytes += byteLength
    // Checked after the push, so the frame that trips the budget is still in the prefix the
    // fallback delivers. Pre-flip nothing is committed yet, so the attempt can simply be dropped.
    if (u.bufferedFrames <= UPGRADE_HANDOFF_BUFFER_FRAMES && u.bufferedBytes <= UPGRADE_HANDOFF_BUFFER_BYTES) return
    if (flipped) this.owner.fallbackToSse(new NetworkError('Upgrade handoff buffer limit exceeded', true))
    else u.attempt.abort()
  }

  onProbePong(): void {
    const u = this.state
    if (u.tag !== 'none') u.probeHeartbeat?.resetPong()
  }

  onProbeFrame(frame: DecodedFrame, byteLength: number): void {
    if (frame.tag !== TAG.READY) return this.ingestDuringHandoff(frame, 'new', byteLength)
    if (this.state.tag === 'staging') this.state.settleReady?.(frame.payload)
  }

  onProbeClosed(): void {
    if (this.state.tag !== 'none') this.state.attempt.abort()
  }

  handleFin(): void {
    const u = this.committing
    if (u === null) return
    u.finReceived = true
    this.tryComplete()
  }

  tryComplete(): void {
    const u = this.committing
    if (u === null || this.owner.transport !== u.to) return
    if (!u.finReceived || !u.committed) return
    const { from, buffer, deferredOmitted } = this.exitCommitting()
    retireOldWire(from)
    for (const entry of buffer.old) this.owner.dispatchFrame(entry.frame)
    this.exchange.closeUnacknowledged(deferredOmitted)
    for (const entry of buffer.new) this.owner.dispatchFrame(entry.frame)
    for (const { channel, state } of this.channels.values()) if (state.tag === 'open') channel._onUpgradeJoined()
    this.owner.startTtlIfIdle()
  }

  // ── State transitions: every `this.state =` write goes through these. ──

  private enterStaging(attempt: AbortController): void {
    assert(this.owner.connected && this.state.tag === 'none')
    this.state = { tag: 'staging', attempt, deadline: null, probeHeartbeat: null, settleReady: null, ready: false }
  }

  private enterReady(attempt: AbortController): void {
    const u = this.state
    assert(u.tag === 'staging' && u.attempt === attempt)
    u.ready = true
  }

  private armAttemptDeadline(attempt: AbortController): void {
    const u = this.state
    assert(u.tag === 'staging' && u.attempt === attempt && u.deadline === null)
    u.deadline = setTimeout(() => attempt.abort(), UPGRADE_ATTEMPT_TIMEOUT_MS)
  }

  private enterCommitting(
    from: UpgradeSource,
    to: UpgradeTarget,
    session: ProbeSession,
    attempt: AbortController,
  ): void {
    const u = this.state
    assert(u.tag === 'staging' && u.attempt === attempt)
    this.state = {
      tag: 'committing',
      attempt,
      deadline: u.deadline,
      from,
      to,
      probeHeartbeat: session.probeHeartbeat,
      upgradeId: session.upgradeId,
      finReceived: false,
      committed: false,
      heldBeforeFlip: [],
      buffer: { old: [], new: [] },
      bufferedBytes: 0,
      bufferedFrames: 0,
      deferredOmitted: [],
      joinTimer: null,
    }
  }

  private exitAttempt(attempt: AbortController): void {
    const u = this.state
    if (u.tag === 'none' || u.attempt !== attempt) return
    if (u.tag === 'committing' && this.owner.transport === u.to) return
    if (u.deadline) clearTimeout(u.deadline)
    this.state = { tag: 'none' }
  }

  private exitCommitting(): CommittingUpgrade {
    const u = this.state
    assert(u.tag === 'committing')
    if (u.deadline) clearTimeout(u.deadline)
    if (u.joinTimer) clearTimeout(u.joinTimer)
    this.state = { tag: 'none' }
    return u
  }

  // ── The attempt ──

  private async probeAndUpgrade(from: UpgradeSource): Promise<void> {
    const { sessionId } = this.exchange
    if (sessionId === null) return
    const attempt = new AbortController()
    this.enterStaging(attempt)
    try {
      const to = this.owner.createWebSocket()
      const session = await this.stageProbe(to, sessionId, attempt)
      if (!session) return
      await this.commitBarrier(from, to, session, attempt)
    } finally {
      this.exitAttempt(attempt)
      this.exchange.flushPendingRegisterReconcile()
    }
  }

  private async stageProbe(
    to: UpgradeTarget,
    sessionId: string,
    attempt: AbortController,
  ): Promise<ProbeSession | null> {
    const probe = await to.probe(this)
    if (attempt.signal.aborted || !probe) {
      probe?.close()
      return null
    }
    const staging = this.state
    assert(staging.tag === 'staging' && staging.attempt === attempt)
    const probeHeartbeat = this.keepProbeAlive(probe, attempt)
    staging.probeHeartbeat = probeHeartbeat

    const upgradeId = randomUuid()
    const readyP = new Promise<ReadyPayload | null>((resolve) => {
      staging.settleReady = resolve
      attempt.signal.addEventListener('abort', () => resolve(null), { once: true })
    })
    this.armAttemptDeadline(attempt)
    probe.send(encode.prepare({ upgradeId, sessionId }))
    const ready = await readyP
    if (!ready || ready.upgradeId !== upgradeId || attempt.signal.aborted) {
      attempt.abort()
      if (!this.exchange.reconcileOwed) this.owner.drainBufferedFramesToWire()
      return null
    }
    this.enterReady(attempt)
    return { upgradeId, sessionId, probeHeartbeat, uplink: probe.uplink }
  }

  /** The probe is nobody's transport yet, so it has no heartbeat of its own: this one keeps it
   *  alive until the flip adopts it, and any death — its own or the attempt's — closes both. */
  private keepProbeAlive(probe: ProbeWire, attempt: AbortController): Heartbeat {
    const intervalMs = this.liveness.pingInterval
    const heartbeat = new Heartbeat({
      intervalMs,
      pongTimeoutMs: pingDeadline(intervalMs),
      send: () => probe.ping(),
      onDead: () => attempt.abort(),
    })
    heartbeat.start(true)
    attempt.signal.addEventListener(
      'abort',
      () => {
        heartbeat.stop()
        probe.close()
      },
      { once: true },
    )
    return heartbeat
  }

  private async commitBarrier(
    from: UpgradeSource,
    to: UpgradeTarget,
    session: ProbeSession,
    attempt: AbortController,
  ): Promise<void> {
    // After the RECONCILED of the one in flight, which may name a channel the barrier lists.
    await this.exchange.reconcileSettled(attempt.signal)
    if (attempt.signal.aborted) return
    this.enterCommitting(from, to, session, attempt)
    const emission = await from.emitBarrier(() => this.exchange.buildBarrierFrame(session), attempt.signal)

    if (emission === 'not-emitted') {
      attempt.abort()
      if (!this.exchange.reconcileOwed) this.owner.drainBufferedFramesToWire()
      // An attempt that ended without its barrier goes again once the outbox drained.
      void from.drained().then(() => this.maybeStart())
      return
    }
    if (attempt.signal.aborted) {
      this.owner.fallbackToSse(new NetworkError('Upgrade barrier attempt timed out', true))
      return
    }
    this.flip()
  }

  // ── The join ──

  private flip(): void {
    const u = this.committing
    assert(u !== null)
    if (u.deadline) {
      clearTimeout(u.deadline)
      u.deadline = null
    }
    u.probeHeartbeat.stop()
    this.owner.adoptTransport(u.to)
    u.to.adoptProbe()
    u.joinTimer = setTimeout(() => this.onJoinTimeout(), UPGRADE_HANDOFF_JOIN_TIMEOUT_MS)
    // What the probe delivered before the swap can be acted on now. The buffer is made whole
    // first, so the COMMITTED that may be among them cannot complete a join over a partial one.
    const held = u.heldBeforeFlip.splice(0)
    for (const { frame, byteLength } of held) if (!isJoinLimb(frame)) u.buffer.new.push({ frame, byteLength })
    for (const { frame } of held) if (isJoinLimb(frame)) this.applyJoinLimb(frame)
    this.tryComplete()
  }

  /** True if the frame was one of the join's limbs, and has been applied. */
  private applyJoinLimb(frame: DecodedFrame): boolean {
    if (frame.tag === TAG.FIN) {
      this.handleFin()
      return true
    }
    if (frame.tag === TAG.RECONCILED) {
      this.owner.handleReconciled(frame.payload)
      return true
    }
    return false
  }

  private onJoinTimeout(): void {
    const u = this.committing
    if (u === null) return
    u.joinTimer = null
    // The FIN comes behind what the old wire still delivers: given up on once that wire went quiet for the timeout.
    const quiet = u.finReceived ? Infinity : (u.from.heartbeat?.quietFor() ?? Infinity)
    if (quiet < UPGRADE_HANDOFF_JOIN_TIMEOUT_MS) {
      u.joinTimer = setTimeout(() => this.onJoinTimeout(), UPGRADE_HANDOFF_JOIN_TIMEOUT_MS - quiet)
      return
    }
    const waitingFor = u.finReceived ? 'RECONCILED' : 'FIN'
    this.owner.fallbackToSse(new NetworkError(`Upgrade handoff timed out waiting for ${waitingFor}`, true))
  }
}

function retireOldWire(transport: ClientChannelTransport): void {
  transport.detachHeartbeat()
  transport.abandonActiveTransport()
  transport.dispose()
}
