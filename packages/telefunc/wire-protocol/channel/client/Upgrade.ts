export { Upgrade }
export type {
  AttemptEnd,
  AttemptHooks,
  BarrierEmission,
  HandoffFrame,
  JoinRelease,
  ProbeOwner,
  ProbeSession,
  ProbeWire,
  UpgradeOwner,
  UpgradeSource,
  UpgradeTarget,
}

import type { ClientChannelTransport, OutboundFrame } from './ClientConnection.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { DecodedFrame } from '../../wire/frame.js'
import type { ReadyPayload, ReconciledPayload } from '../../wire/payloads.js'
import {
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

/** What an attempt asks of the connection between its steps. */
type AttemptHooks = {
  /** Settles once the RECONCILE in flight has its RECONCILED, which may name a channel the barrier lists. */
  reconcileSettled: (signal: AbortSignal) => Promise<void>
  /** The barrier's frame, built as the old wire sends it. */
  barrier: (session: ProbeSession) => OutboundFrame
  /** `to` becomes the connection's transport. */
  adopt: (to: UpgradeTarget) => void
}

/** How an attempt ended: `'aborted'` before its barrier, its probe gone or never opened; `'not-ready'` without a
 *  matching READY; `'not-emitted'` with its barrier never sent, so the old wire stays usable; `'stranded'` aborted
 *  after its barrier went; `'flipped'` onto the new wire, with the completion frames the probe delivered before, in
 *  order, for the connection to apply before it calls `tryComplete`. */
type AttemptEnd =
  | { tag: 'aborted' | 'not-ready' | 'not-emitted' | 'stranded' }
  | { tag: 'flipped'; completion: DecodedFrame[] }

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
      /** The connection adopted `to` (`flip`): from here the join runs, and frames from either wire wait for it. */
      flipped: boolean
      probeHeartbeat: Heartbeat
      upgradeId: string
      /** The join's two completion frames, one per wire. Both must land before dispatching resumes. */
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

/** FIN (old wire) and RECONCILED (new wire) complete the join; everything else is payload. */
function isUpgradeCompletionFrame(frame: DecodedFrame): boolean {
  return frame.tag === TAG.FIN || frame.tag === TAG.RECONCILED
}

/** What a frame a wire delivers is to the connection: `'dispatch'` it outside a flipped handoff; inside one, `'held'`
 *  for the join, `'complete'` to apply as one of the join's completion frames, or `'fallback'` once the handoff's buffer
 *  went past its budget. */
type HandoffFrame = 'dispatch' | 'held' | 'complete' | 'fallback'

/** What a join releases, in the order the server wrote it: the old wire's frames, then the new wire's. The channels its
 *  RECONCILED left out close unacknowledged between them. */
type JoinRelease = { old: DecodedFrame[]; deferredOmitted: number[]; new: DecodedFrame[] }

/** The connection an upgrade moves from its SSE wire to a WebSocket. */
interface UpgradeOwner {
  /** The join didn't complete in time: the connection goes back to SSE. */
  onHandoffFailed(err: Error): void
}

type UpgradeOptions = {
  owner: UpgradeOwner
}

/** An upgrade: a probe of the next rung staged beside the wire, the barrier that ends the old wire, and the join
 *  of both wires' frames once the connection flipped to the new one. The connection runs an attempt with `run`. */
class Upgrade implements ProbeOwner {
  private readonly owner: UpgradeOwner
  /** Only while the connection is open: it goes with the open state it was in (see `forget`). */
  private state: UpgradeState = { tag: 'none' }
  /** Set by a permanent upgrade abort, and kept through every state change until dispose. */
  private disabled = false

  constructor({ owner }: UpgradeOptions) {
    this.owner = owner
  }

  private get committing(): CommittingUpgrade | null {
    return this.state.tag === 'committing' ? this.state : null
  }

  /** Whether the connection's transport is already the new wire. The flip happens inside `committing`; it is not a
   *  state of its own. */
  get flipped(): boolean {
    return this.committing?.flipped === true
  }

  /** Sends are gated from the barrier's emission until its RECONCILED lands. */
  get gatesSends(): boolean {
    const u = this.committing
    return u !== null && !u.committed
  }

  /** READY came, so the barrier goes next: a RECONCILE too, and no other goes out till the handoff. */
  get ready(): boolean {
    return this.state.tag === 'staging' && this.state.ready
  }

  /** From READY to the handoff's end, no RECONCILE but the upgrade's goes out. */
  get holdsReconcile(): boolean {
    return this.state.tag === 'committing' || this.ready
  }

  /** From the barrier's emission until its join ends, or its attempt does before the flip. */
  get handingOff(): boolean {
    return this.committing !== null
  }

  /** Keeps the channels the handoff's RECONCILED left out, to close unacknowledged once its join ends. */
  deferOmitted(ixes: number[]): void {
    const u = this.committing
    if (u === null) return
    for (const ix of ixes) u.deferredOmitted.push(ix)
  }

  /** Whether `transport` is the wire a flipped handoff left. */
  isLeftWire(transport: ClientChannelTransport): boolean {
    return this.flipped && transport === this.committing!.from
  }

  /** Takes a frame a wire delivered: what the connection does with it (see `HandoffFrame`). */
  takeHandoffFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number): HandoffFrame {
    if (!this.flipped) return 'dispatch'
    return this.ingestDuringHandoff(frame, source === this.committing!.from ? 'old' : 'new', byteLength)
  }

  /** No attempt is in flight, and none gave the upgrade up for good. */
  get canStart(): boolean {
    return !this.disabled && this.state.tag === 'none'
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
  private teardown(): CommittingUpgrade | null {
    const u = this.state
    if (u.tag === 'none') return null
    if (u.tag === 'committing' && u.flipped) {
      const record = this.exitCommitting()
      retireOldWire(record.from)
      return record
    }
    u.attempt.abort()
    this.endAttempt(u.attempt)
    return null
  }

  /** The upgrade is given up for good, and none starts again. Returns what a flipped join held of the old wire, for
   *  the connection to deliver, and the channels its RECONCILED left out; null before a flip. */
  abandon(): Omit<JoinRelease, 'new'> | null {
    const abandoned = this.teardown()
    this.disabled = true
    if (abandoned === null) return null
    return { old: abandoned.buffer.old.map(({ frame }) => frame), deferredOmitted: abandoned.deferredOmitted }
  }

  /** Takes every frame that arrives during the handoff, and is the one place that counts it against the handoff's
   *  budget. Before the flip the probe is nobody's wire, so even a completion frame waits: acting on one would settle a
   *  swap that hasn't happened. After it, completion frames apply and the rest waits, split by the wire it came from. */
  private ingestDuringHandoff(frame: DecodedFrame, source: 'old' | 'new', byteLength: number): HandoffFrame {
    const u = this.committing
    if (u === null) return 'held'
    const { flipped } = u
    if (flipped && isUpgradeCompletionFrame(frame)) return 'complete'
    ;(flipped ? u.buffer[source] : u.heldBeforeFlip).push({ frame, byteLength })
    u.bufferedFrames += 1
    u.bufferedBytes += byteLength
    // Checked after the push, so the frame that trips the budget is still in the prefix the
    // fallback delivers. Pre-flip nothing is committed yet, so the attempt can simply be dropped.
    if (u.bufferedFrames <= UPGRADE_HANDOFF_BUFFER_FRAMES && u.bufferedBytes <= UPGRADE_HANDOFF_BUFFER_BYTES)
      return 'held'
    if (flipped) return 'fallback'
    u.attempt.abort()
    return 'held'
  }

  onProbePong(): void {
    const u = this.state
    if (u.tag !== 'none') u.probeHeartbeat?.resetPong()
  }

  /** The probe delivers only before the flip, which hands its socket to the transport: what it delivers waits for it. */
  onProbeFrame(frame: DecodedFrame, byteLength: number): void {
    if (frame.tag !== TAG.READY) {
      this.ingestDuringHandoff(frame, 'new', byteLength)
      return
    }
    if (this.state.tag === 'staging') this.state.settleReady?.(frame.payload)
  }

  onProbeClosed(): void {
    if (this.state.tag !== 'none') this.state.attempt.abort()
  }

  /** The old wire's FIN came: what the join releases, if that completed it. */
  handleFin(): JoinRelease | null {
    const u = this.committing
    if (u === null) return null
    u.finReceived = true
    return this.tryComplete()
  }

  /** What the join releases once both its completion frames landed after the flip; null till then. */
  tryComplete(): JoinRelease | null {
    const u = this.committing
    if (u === null || !u.flipped) return null
    if (!u.finReceived || !u.committed) return null
    const { from, buffer, deferredOmitted } = this.exitCommitting()
    retireOldWire(from)
    return {
      old: buffer.old.map(({ frame }) => frame),
      deferredOmitted,
      new: buffer.new.map(({ frame }) => frame),
    }
  }

  // ── State transitions: every `this.state =` write goes through these. ──

  private enterStaging(attempt: AbortController): void {
    assert(this.state.tag === 'none')
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
      flipped: false,
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

  /** The attempt ended, however: its state goes, unless it flipped and its join is under way. */
  private endAttempt(attempt: AbortController): void {
    const u = this.state
    if (u.tag === 'none' || u.attempt !== attempt) return
    if (u.tag === 'committing' && u.flipped) return
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

  /** The connection left its open state, and the upgrade in it. */
  forget(): void {
    this.state = { tag: 'none' }
  }

  // ── The attempt ──

  /** Runs one attempt from `from` to `to`, its steps in order: the probe staged, the RECONCILE in flight settled, the
   *  barrier emitted, then the flip. A `to` it didn't hand to `adopt` it disposes. */
  async run(
    from: UpgradeSource,
    to: UpgradeTarget,
    { sessionId, pingInterval }: { sessionId: string; pingInterval: number },
    hooks: AttemptHooks,
  ): Promise<AttemptEnd> {
    const attempt = this.start()
    let adopted = false
    try {
      const session = await this.stage(to, sessionId, pingInterval, attempt)
      if (session === 'no-probe') return { tag: 'aborted' }
      if (session === 'not-ready') return { tag: 'not-ready' }
      await hooks.reconcileSettled(attempt.signal)
      if (attempt.signal.aborted) return { tag: 'aborted' }
      // The barrier goes next, on `from`: from here sends wait for its RECONCILED.
      this.enterCommitting(from, to, session, attempt)
      const emission = await from.emitBarrier(() => hooks.barrier(session), attempt.signal)
      if (emission === 'not-emitted') return { tag: 'not-emitted' }
      if (attempt.signal.aborted) return { tag: 'stranded' }
      hooks.adopt(to)
      adopted = true
      return { tag: 'flipped', completion: this.flip() }
    } finally {
      // A target it didn't adopt goes with it: its probe closes with the attempt, then its page listener.
      if (!adopted) {
        attempt.abort()
        to.dispose()
      }
      this.endAttempt(attempt)
    }
  }

  /** Starts an attempt; its signal aborts on any failure of it. */
  private start(): AbortController {
    const attempt = new AbortController()
    this.enterStaging(attempt)
    return attempt
  }

  /** Opens `to`'s probe and exchanges PREPARE/READY on it: the session its barrier commits, or why not.
   *  `'no-probe'`: no probe opened, or the attempt aborted meanwhile; `'not-ready'`: no matching READY came, and the
   *  attempt is aborted. */
  private async stage(
    to: UpgradeTarget,
    sessionId: string,
    pingInterval: number,
    attempt: AbortController,
  ): Promise<ProbeSession | 'no-probe' | 'not-ready'> {
    const probe = await to.probe(this)
    if (attempt.signal.aborted || !probe) {
      probe?.close()
      return 'no-probe'
    }
    const staging = this.state
    assert(staging.tag === 'staging' && staging.attempt === attempt)
    const probeHeartbeat = this.keepProbeAlive(probe, attempt, pingInterval)
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
      return 'not-ready'
    }
    this.enterReady(attempt)
    return { upgradeId, sessionId, probeHeartbeat, uplink: probe.uplink }
  }

  /** The probe is nobody's transport yet, so it has no heartbeat of its own: this one keeps it
   *  alive until the flip adopts it, and any death — its own or the attempt's — closes both. */
  private keepProbeAlive(probe: ProbeWire, attempt: AbortController, intervalMs: number): Heartbeat {
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

  // ── The join ──

  /** The barrier went and the connection adopted the new wire: the probe becomes it, and the join begins. Returns the
   *  completion frames the probe delivered before, in order, for the connection to apply; then it calls `tryComplete`.
   *  The buffer is made whole first, so a RECONCILED among them cannot complete a join over a partial one. */
  private flip(): DecodedFrame[] {
    const u = this.committing
    assert(u !== null)
    if (u.deadline) {
      clearTimeout(u.deadline)
      u.deadline = null
    }
    u.probeHeartbeat.stop()
    u.flipped = true
    u.to.adoptProbe()
    u.joinTimer = setTimeout(() => this.onJoinTimeout(), UPGRADE_HANDOFF_JOIN_TIMEOUT_MS)
    const held = u.heldBeforeFlip.splice(0)
    for (const { frame, byteLength } of held)
      if (!isUpgradeCompletionFrame(frame)) u.buffer.new.push({ frame, byteLength })
    return held.map(({ frame }) => frame).filter(isUpgradeCompletionFrame)
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
    this.owner.onHandoffFailed(new NetworkError(`Upgrade handoff timed out waiting for ${waitingFor}`, true))
  }
}

function retireOldWire(transport: ClientChannelTransport): void {
  transport.detachHeartbeat()
  transport.abandonActiveTransport()
  transport.dispose()
}
