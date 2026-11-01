export { AttachExchange }
export type { AttachExchangeOwner, ReconcileBatch }

import type { ChannelRecord, ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport, MuxChannel, OutboundFrame } from './connection.js'
import type { SendGate } from './SendGate.js'
import type { ProbeSession } from './Upgrade.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { ReconcileOpenEntry, ReconcilePayload, ReconciledPayload } from '../../wire/payloads.js'
import { RECONCILE_TIMEOUT_MS } from '../../constants.js'
import { ERROR_REASON, encode } from '../../wire/frame.js'
import { replayLossError } from '../channelErrors.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { settledOrAborted } from '../../../utils/settledOrAborted.js'

type ReconcileOutcome = {
  frames: OutboundFrame[]
  channelsToOpen: MuxChannel[]
  reconcileComplete: boolean
}

/** The RECONCILE in flight. */
type ReconcileInFlight = {
  /** The channels it lists, and whether as `initial`. */
  ixes: Map<number, boolean>
  /** The BDP probes it carries, by channel; its RECONCILED answers each one no BDP_PING_ACK answered. */
  probes: Map<number, number>
  /** Per channel, the first seq its wire carries behind it: the RECONCILED replays only the frames before it. */
  carriedFrom: Map<number, number>
  /** Its channels' ATTACH_RESULT frames from before its RECONCILED, applied with it: the server may build it first. */
  attachResults: Map<number, number | null>
}

function reconcileInFlight(): ReconcileInFlight {
  return { ixes: new Map(), probes: new Map(), carriedFrom: new Map(), attachResults: new Map() }
}

type ReconcileBatch = {
  reconcileFrame: OutboundFrame
  movedBufferedFrames: OutboundFrame[]
}

/** The connection, whose wire carries the RECONCILEs. */
interface AttachExchangeOwner {
  readonly transport: ClientChannelTransport
  readonly closed: boolean
  /** Advances each time the connection moves to another wire: what went out on the one before may not have arrived. */
  readonly wire: number
  /** The wire can't take a RECONCILE now: it isn't open, or an upgrade is between its READY and its handoff. */
  readonly reconcileHeld: boolean
  releaseChannel(record: ChannelRecord): void
  dropWire(transport: ClientChannelTransport): void
}

type AttachExchangeOptions = {
  owner: AttachExchangeOwner
  channels: ChannelTable
  gate: SendGate
}

/** The page's side of attaching its channels to the server: each RECONCILE names them, its RECONCILED says which the
 *  server attached and how far it has what they sent, and an ATTACH_RESULT answers for one the server awaited. */
class AttachExchange {
  private readonly owner: AttachExchangeOwner
  private readonly channels: ChannelTable
  private readonly gate: SendGate
  /** A RECONCILE is out and its RECONCILED hasn't come. On SSE it is set while connecting too: the open POST's body
   *  carries the first RECONCILE. Only `enterReconciling` and `exitReconciling` write it, so its deadline
   *  (`reconcileTimer`) never outlives it. */
  private isReconciling = false
  private reconcileTimer: ReturnType<typeof setTimeout> | null = null
  /** Resolved once no RECONCILE is in flight. */
  private reconcileSettledWaiters: (() => void)[] = []
  private inFlight = reconcileInFlight()
  /** A channel registered since the last RECONCILE, which owes the server one naming it. */
  private isReconcileOwed = false
  private registerReconcileTimer: ReturnType<typeof setTimeout> | null = null
  private session: string | null = null
  /** The wire the last RECONCILED came on. */
  private reconciledWire = -1

  constructor({ owner, channels, gate }: AttachExchangeOptions) {
    this.owner = owner
    this.channels = channels
    this.gate = gate
  }

  get reconciling(): boolean {
    return this.isReconciling
  }

  get reconcileOwed(): boolean {
    return this.isReconcileOwed
  }

  /** The server's, from the last RECONCILED. */
  get sessionId(): string | null {
    return this.session
  }

  /** Till its first RECONCILED a wire's RECONCILE deadline bounds it: what precedes may outlast the pong deadline. */
  get boundsWire(): boolean {
    return this.isReconciling && this.reconciledWire !== this.owner.wire
  }

  /** Coalesces sync-burst registrations into one RECONCILE round-trip. */
  scheduleRegisterReconcile(): void {
    if (this.isReconcileOwed) return
    this.isReconcileOwed = true
    this.registerReconcileTimer = setTimeout(() => {
      this.registerReconcileTimer = null
      this.flushPendingRegisterReconcile()
    }, 0)
  }

  /** Sends the owed RECONCILE on the live wire; no-op when none is owed. While the wire can't take it (connecting,
   *  awaiting a RECONCILED, from an upgrade's READY until its handoff) it stays owed: `reconcileOwed` keeps `sendsOpen`
   *  false, so data frames buffer behind it, and `onTransportOpen` and an upgrade attempt's exit call this again. A
   *  RECONCILED, the handoff's too, names what registered meanwhile in its follow-up. */
  flushPendingRegisterReconcile(): void {
    if (!this.isReconcileOwed) return
    if (this.owner.reconcileHeld) return
    if (this.isReconciling) return
    this.sendReconcileBatch(this.stageReconcileBatch())
  }

  cancelRegisterReconcile(): void {
    this.isReconcileOwed = false
    if (this.registerReconcileTimer === null) return
    clearTimeout(this.registerReconcileTimer)
    this.registerReconcileTimer = null
  }

  reconcileSettled(signal: AbortSignal): Promise<void> {
    if (!this.isReconciling) return Promise.resolve()
    return settledOrAborted(new Promise<void>((resolve) => this.reconcileSettledWaiters.push(resolve)), signal)
  }

  /** The server stops awaiting channels as their wire goes, and the next wire's RECONCILE lists them again. */
  forgetWire(): void {
    this.channels.stopAwaiting()
    this.inFlight = reconcileInFlight()
    this.exitReconciling()
  }

  stageReconcileBatch(): ReconcileBatch {
    const reconcileFrame = this.stageReconcile()
    return { reconcileFrame, movedBufferedFrames: this.drainBufferedFramesForReconcile() }
  }

  /** The batch an SSE wire's opening request carries. */
  stageInitialReconcileBatch(): ReconcileBatch {
    const reconcileFrame = this.stageReconcile()
    // After a wire that may have lost frames, the replay goes first: a newer frame ahead gets an older one dropped.
    const replayFirst = this.session !== null || this.sentBefore()
    return { reconcileFrame, movedBufferedFrames: replayFirst ? [] : this.drainBufferedFramesForReconcile() }
  }

  sendReconcileBatch(reconcileBatch: ReconcileBatch): void {
    const { transport } = this.owner
    transport.sendFrame(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) transport.sendFrame(frame)
  }

  /** The old wire's last frame; a channel no RECONCILE named yet is left out, to reconcile after the handoff. */
  buildBarrierFrame({ sessionId, upgradeId, uplink }: ProbeSession): OutboundFrame {
    this.enterReconciling()
    // Its entries attach on the WebSocket, `uplink`'s wire, the one after this one.
    const open = this.declareOpenEntries(this.channels.values(), { skipUnnamed: true, uplink })
    // A new wire's RECONCILE releases what it leaves out.
    this.channels.takeReleased()
    return { kind: 'reconcile', frame: encode.barrier({ sessionId, upgradeId, open }) }
  }

  /** `deferredOmitted` collects, during an upgrade's handoff, the channels it leaves out; `upgradeReady`: an upgrade's
   *  READY came, so its barrier, a RECONCILE too, goes next. */
  applyReconciled(
    ctrl: ReconciledPayload,
    { deferredOmitted, upgradeReady }: { deferredOmitted: number[] | null; upgradeReady: boolean },
  ): ReconcileOutcome {
    this.reconciledWire = this.owner.wire
    this.session = ctrl.sessionId
    const serverMap = new Map<number, number>()
    for (const channel of ctrl.open) serverMap.set(channel.ix, channel.lastSeq)
    const { ixes, probes, attachResults, carriedFrom } = this.inFlight
    this.inFlight = reconcileInFlight()
    for (const [ix, probe] of probes) this.channels.get(ix)?.channel._onProbeAnswered(probe)
    for (const [ix, lastSeq] of attachResults) if (lastSeq !== null && !serverMap.has(ix)) serverMap.set(ix, lastSeq)
    // An initial channel left out is one the server awaits, until its ATTACH_RESULT.
    for (const [ix, initial] of ixes) {
      const record = this.channels.get(ix)
      if (record !== undefined) record.awaited = initial && !serverMap.has(ix) && !attachResults.has(ix)
    }
    const releaseFrames: OutboundFrame[] = []
    const channelsToOpen: MuxChannel[] = []
    // Channels registered after this RECONCILE was built wait for the next one.
    const hasNewChannels = this.channels.hasUnnamed

    for (const ix of ixes.keys()) {
      const record = this.channels.get(ix)
      if (record === undefined || record.awaited) continue
      const replay = this.attachRecord(
        record,
        serverMap.get(ix),
        (carriedFrom.get(ix) ?? Infinity) - 1,
        deferredOmitted,
      )
      if (replay === null) continue
      for (const frame of replay) releaseFrames.push(frame)
      if (record.state.tag !== 'closed') channelsToOpen.push(record.channel)
    }

    // What the server has: the channels this RECONCILE named that it attached, and those the ones before named.
    const attached = (ix: number): boolean => {
      if (ixes.has(ix)) return serverMap.has(ix)
      const record = this.channels.get(ix)
      return record !== undefined && !this.channels.isUnnamed(record) && !record.awaited
    }
    for (const frame of this.gate.drain(attached)) releaseFrames.push(frame)

    if (hasNewChannels && !upgradeReady) {
      const reconcileBatch = this.stageReconcileBatch()
      appendReconcileBatch(releaseFrames, reconcileBatch)
    } else {
      this.exitReconciling()
      // The barrier leaves out what the follow-up would name.
      if (hasNewChannels) this.scheduleRegisterReconcile()
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
  }

  /** An awaited channel's own RECONCILED entry: `lastSeq` if the server attached it, null if not. False where it
   *  answers no channel the server awaited, or waits for the RECONCILED in flight. */
  handleAttachResult(ix: number, lastSeq: number | null): boolean {
    if (this.inFlight.ixes.has(ix)) {
      this.inFlight.attachResults.set(ix, lastSeq)
      return false
    }
    const record = this.channels.get(ix)
    if (record === undefined || !record.awaited) return false
    record.awaited = false
    if (lastSeq === null) {
      this.closeUnacknowledged([ix])
      return true
    }
    const opened = record.state.tag === 'pending'
    if (opened) this.channels.open(record)
    else if (record.state.tag === 'closed') this.channels.serverHas(record, record.state, lastSeq)
    // As `applyReconciled` does: its replay, then what it queued.
    const { transport } = this.owner
    for (const frame of this.replayTo(record, lastSeq)) transport.sendFrame(frame)
    for (const frame of this.gate.drain((i) => i === ix)) transport.sendFrame(frame)
    if (opened && record.state.tag === 'open') {
      record.channel._onTransportOpen(transport.batched, transport.uplink, transport.downlink)
    }
    return true
  }

  closeUnacknowledged(ixes: number[]): void {
    for (const ix of ixes) {
      const record = this.channels.get(ix)
      if (!record) continue
      const err = new NetworkError('Channel not acknowledged by server after reconnect', true)
      this.owner.releaseChannel(record)
      record.channel._onEnded(err)
    }
  }

  // ── The await-RECONCILED window ──

  /** Opens the wait for a new RECONCILE's RECONCILED and arms its deadline. A follow-up RECONCILE for late
   *  registrations opens it again, restarting the deadline. */
  private enterReconciling(): void {
    this.isReconciling = true
    this.inFlight = reconcileInFlight()
    this.armReconcileDeadline(RECONCILE_TIMEOUT_MS)
  }

  private armReconcileDeadline(ms: number): void {
    if (this.reconcileTimer) clearTimeout(this.reconcileTimer)
    this.reconcileTimer = setTimeout(() => this.onReconcileTimeout(), ms)
  }

  /** Closes the wait: the RECONCILED settled, the wire was lost, or the connection was disposed. */
  private exitReconciling(): void {
    this.isReconciling = false
    if (this.reconcileTimer) {
      clearTimeout(this.reconcileTimer)
      this.reconcileTimer = null
    }
    for (const resolve of this.reconcileSettledWaiters.splice(0)) resolve()
  }

  /** The RECONCILED didn't come by the deadline. Unless the wire delivered something meanwhile, it is dropped as on a
   *  missed pong, and `handleTransportLoss` reconnects. */
  private onReconcileTimeout(): void {
    if (this.owner.closed || !this.isReconciling) return
    const { transport } = this.owner
    // A RECONCILE waits behind what the page sent before: the wire is dead once it delivered nothing for the deadline.
    const quiet = transport.heartbeat?.quietFor() ?? Infinity
    if (quiet < RECONCILE_TIMEOUT_MS) return this.armReconcileDeadline(RECONCILE_TIMEOUT_MS - quiet)
    this.owner.dropWire(transport)
  }

  // ── The RECONCILE ──

  private buildReconcileFrame(): OutboundFrame {
    this.enterReconciling()
    const { uplink } = this.owner.transport
    // The wire that has the session holds what the RECONCILEs before named: one there names what changed since.
    const changes = this.session !== null && this.reconciledWire === this.owner.wire
    const records = changes ? this.channels.changed() : this.channels.values()
    const open = this.declareOpenEntries(records, {
      skipUnnamed: false,
      uplink,
    })
    const released = this.channels.takeReleased()
    const reconcile: ReconcilePayload = { open }
    if (this.session) reconcile.sessionId = this.session
    if (changes) reconcile.released = released
    return { kind: 'reconcile', frame: encode.reconcile(reconcile) }
  }

  private declareOpenEntries(
    records: Iterable<ChannelRecord>,
    {
      skipUnnamed,
      uplink,
    }: {
      skipUnnamed: boolean
      uplink: Uplink
    },
  ): ReconcileOpenEntry[] {
    const open: ReconcileOpenEntry[] = []
    const subscribing = new Set<number>()
    for (const record of records) {
      const { ix } = record
      // The server has all a delivered one sent it, or ended it: leaving it out lets it go there too.
      if (record.state.tag === 'closed' && record.state.delivered) {
        this.owner.releaseChannel(record)
        continue
      }
      const isInitial = record.state.tag !== 'open' && record.state.initial
      if (skipUnnamed && isInitial && !record.awaited) continue
      this.channels.name(record)
      this.inFlight.ixes.set(ix, isInitial)
      const payloadEntry: ReconcileOpenEntry = {
        id: record.channel.id,
        ix,
        lastSeq: record.lastSeq,
      }
      if (isInitial) payloadEntry.initial = true
      const state = record.channel._attachState(uplink)
      if (state.probe !== undefined) this.inFlight.probes.set(ix, state.probe)
      Object.assign(payloadEntry, state)
      if (state.broadcast) subscribing.add(ix)
      open.push(payloadEntry)
    }
    // The declared subscriptions supersede the SUB/UNSUB frames queued before them.
    if (subscribing.size > 0) this.gate.dropSubscriptionFrames(subscribing)
    return open
  }

  /** Builds the RECONCILE, which names every channel registered since the last: it covers any pending registration. */
  private stageReconcile(): OutboundFrame {
    this.cancelRegisterReconcile()
    return this.buildReconcileFrame()
  }

  private drainBufferedFramesForReconcile(): OutboundFrame[] {
    if (!this.owner.transport.carriesReconcileInOpen) return []
    // A channel the server awaits is left out: its replay waits for its ATTACH_RESULT.
    return this.gate.drainForReconcile(this.inFlight.carriedFrom)
  }

  /** Whether a replay holds a frame the page sent. */
  private sentBefore(): boolean {
    for (const { replay } of this.channels.values()) if (replay.length > 0) return true
    return false
  }

  // ── The RECONCILED ──

  /** What a channel the RECONCILE named replays to the server, its state brought to the server's; null if let go. */
  private attachRecord(
    record: ChannelRecord,
    lastSeq: number | undefined,
    throughSeq: number,
    deferredOmitted: number[] | null,
  ): OutboundFrame[] | null {
    if (record.state.tag === 'closed') {
      // One the server no longer has needs nothing more from the page.
      if (lastSeq === undefined) {
        this.owner.releaseChannel(record)
        return null
      }
      this.channels.serverHas(record, record.state, lastSeq)
    }
    if (lastSeq === undefined) {
      if (deferredOmitted) deferredOmitted.push(record.ix)
      else this.closeUnacknowledged([record.ix])
      return null
    }
    if (record.state.tag === 'pending') this.channels.open(record)
    return this.replayTo(record, lastSeq, throughSeq)
  }

  /** What the server lacks through `throughSeq`: the replay, or an ERROR ending the channel if the replay lost some. */
  private replayTo(record: ChannelRecord, lastSeq: number, throughSeq = Infinity): OutboundFrame[] {
    record.replay.acknowledge(lastSeq)
    const missed = record.replay.getAfter(lastSeq, throughSeq)
    if (missed !== null) return missed.map((frame) => ({ kind: 'released', frame }))
    // One the server ended needs nothing more.
    if (record.state.tag === 'closed' && record.state.delivered) return []
    return [this.loseChannel(record)]
  }

  /** Ends the channel on both ends, dropping its queue; the ERROR isn't replayed, as the next reconcile sends one. */
  private loseChannel({ ix, replay, channel }: ChannelRecord): OutboundFrame {
    this.gate.dropQueue(ix)
    const frame = encode.error(ix, ERROR_REASON.LOST, replay.nextSeq())
    channel._onEnded(replayLossError('client'))
    return { kind: 'control', frame }
  }
}

function appendReconcileBatch(target: OutboundFrame[], reconcileBatch: ReconcileBatch): void {
  target.push(reconcileBatch.reconcileFrame)
  for (const frame of reconcileBatch.movedBufferedFrames) target.push(frame)
}
