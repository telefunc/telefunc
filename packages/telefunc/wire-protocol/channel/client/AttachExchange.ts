export { AttachExchange }
export type { AttachExchangeOwner, ReconcileBatch }

import type { ChannelRecord, ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport, MuxChannel, OutboundFrame } from './ClientConnection.js'
import type { SendGate } from './SendGate.js'
import type { ProbeSession } from './Upgrade.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import { MAX_U32, type ReconcileOpenEntry, type ReconcilePayload, type ReconciledPayload } from '../../wire/payloads.js'
import { CHANNEL_TRANSPORT, RECONCILE_FLOOR_BYTES_PER_MS, RECONCILE_TIMEOUT_MS } from '../../constants.js'
import { ERROR_REASON, encode } from '../../wire/frame.js'
import { readU16 } from '../../wire/header.js'
import { isSequencedTag } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { settledOrAborted } from '../../../utils/settledOrAborted.js'
import { assert } from '../../../utils/assert.js'
import { randomUuid } from '../../../utils/randomUuid.js'

type ReconcileOutcome = {
  frames: OutboundFrame[]
  channelsToOpen: MuxChannel[]
  /** With `deferOmitted`, the channels the RECONCILED left out, for the caller to close unacknowledged; else none. */
  omitted: number[]
  reconcileComplete: boolean
}

/** The wait for the RECONCILED of the RECONCILE out. */
type ReconciledWait = {
  /** On a WebSocket, a RECONCILE's deadline runs from its receipt, the server's RECONCILE_TAKEN: `'awaited'` till it
   *  comes, while the deadline runs from the last time the server took more of the page's bytes, then `'taken'`.
   *  `'none'` for a RECONCILE on SSE and a BARRIER, whose deadline runs from the start. */
  receipt: 'none' | 'awaited' | 'taken'
  /** The deadline, from its start or the answer to the request that carried it. */
  timeoutMs: number
  timer: ReturnType<typeof setTimeout>
  /** A later RECONCILE's batch POST is out unanswered: what the event stream delivers meanwhile doesn't excuse it. */
  carrierOut: boolean
  /** What the RECONCILE asked, till its RECONCILED is applied (`takeInFlight`). */
  inFlight: ReconcileInFlight | null
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

/** The connection, whose wire carries the RECONCILEs, and which decides when one may go. */
interface AttachExchangeOwner {
  readonly transport: ClientChannelTransport
  /** Advances each time the connection moves to another wire: what went out on the one before may not have arrived. */
  readonly wire: number
  /** A registration owes a RECONCILE, the task that registered it over: it goes now if the wire can take one. */
  onReconcileDue(): void
  /** The RECONCILED didn't come by the deadline, and the wire delivered nothing meanwhile. */
  onReconcileLate(transport: ClientChannelTransport): void
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
  /** Set while a RECONCILE is out and its RECONCILED hasn't come. On SSE it is set while connecting too: the open
   *  POST's body carries the first RECONCILE. Only `enterReconciling` and `exitReconciling` write it, so its deadline
   *  never outlives it. */
  private reconciledWait: ReconciledWait | null = null
  /** Resolved once no RECONCILE is in flight. */
  private reconcileSettledWaiters: (() => void)[] = []
  /** A channel registered since the last RECONCILE, which owes the server one naming it. */
  private isReconcileOwed = false
  private registerReconcileTimer: ReturnType<typeof setTimeout> | null = null
  private session: string | null = null
  /** The id the page names its session by until a RECONCILED gives it the server's: the page makes it, so that the
   *  server keeps the session's end under it even when its first RECONCILED never reaches the page. */
  private readonly ownId = randomUuid()
  /** The wires the connection opened, probes included: each one's first RECONCILE or PREPARE carries its number, which
   *  orders the page's attempts at the server. */
  private attempts = 0
  /** The wire the last RECONCILED came on. */
  private reconciledWire = -1
  /** From a RECONCILED until what it releases is handed to the transport (`batchHanded`). */
  private committing = false

  constructor({ owner, channels, gate }: AttachExchangeOptions) {
    this.owner = owner
    this.channels = channels
    this.gate = gate
  }

  get reconciling(): boolean {
    return this.reconciledWait !== null
  }

  get reconcileOwed(): boolean {
    return this.isReconcileOwed
  }

  /** The server's, from the last RECONCILED. */
  get sessionId(): string | null {
    return this.session
  }

  /** While a RECONCILE is out, the wire takes new frames of the channels its session has: it has had a RECONCILED, and
   *  what that released went. */
  get liveWire(): boolean {
    return this.reconciledWire === this.owner.wire && !this.committing
  }

  /** Whether `liveWire` takes `record`'s new frames: the server has its channel, and no RECONCILE out names it. */
  takesLive(record: ChannelRecord): boolean {
    const { ix } = record
    return (
      this.liveWire &&
      record.state.tag !== 'pending' &&
      !record.awaited &&
      !this.channels.isUndeclared(ix) &&
      this.reconciledWait?.inFlight?.ixes.has(ix) !== true
    )
  }

  /** What the RECONCILED released went to the transport: new frames may go past what waits. */
  batchHanded(): void {
    this.committing = false
  }

  /** The number of a wire the connection opens now. Every wire is an attempt the reconnect delay paces, so it can't
   *  pass a u32. */
  nextAttempt(): number {
    assert(this.attempts < MAX_U32)
    return ++this.attempts
  }

  /** Till its first RECONCILED a wire's RECONCILE deadline bounds it: what precedes may outlast the pong deadline. */
  get boundsWire(): boolean {
    return this.reconciledWait !== null && this.reconciledWire !== this.owner.wire
  }

  /** Whether that deadline runs from the start, so the heartbeat leaves the wire to it. A RECONCILE with a receipt
   *  waits on the heartbeat until the receipt: its server answers the page's PINGs while the page's bytes arrive. */
  get timesWire(): boolean {
    return this.boundsWire && this.reconciledWait?.receipt === 'none'
  }

  /** Coalesces sync-burst registrations into one RECONCILE round-trip. */
  scheduleRegisterReconcile(): void {
    if (this.isReconcileOwed) return
    this.isReconcileOwed = true
    this.registerReconcileTimer = setTimeout(() => {
      this.registerReconcileTimer = null
      this.owner.onReconcileDue()
    }, 0)
  }

  cancelRegisterReconcile(): void {
    this.isReconcileOwed = false
    if (this.registerReconcileTimer === null) return
    clearTimeout(this.registerReconcileTimer)
    this.registerReconcileTimer = null
  }

  reconcileSettled(signal: AbortSignal): Promise<void> {
    if (this.reconciledWait === null) return Promise.resolve()
    return settledOrAborted(new Promise<void>((resolve) => this.reconcileSettledWaiters.push(resolve)), signal)
  }

  /** The server stops awaiting channels as their wire goes, and the next wire's RECONCILE lists them again. */
  forgetWire(): void {
    this.channels.stopAwaiting()
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
    const inFlight = this.enterReconciling(false)
    // Its entries attach on the WebSocket, `uplink`'s wire, the one after this one.
    const open = this.declareOpenEntries(this.channels.values(), inFlight, { skipUndeclared: true, uplink })
    // A new wire's RECONCILE releases what it leaves out.
    this.channels.takeReleased()
    return { kind: 'reconcile', frame: encode.barrier({ sessionId, upgradeId, open }) }
  }

  /** `deferOmitted`: in an upgrade's handoff, the channels the RECONCILED left out close once its join ends, so they are
   *  returned, not closed; `holdsReconcile`: an upgrade is between its READY and its join's end, so a follow-up waits
   *  for that end, as a receipt the join holds back would be taken for the follow-up's. */
  applyReconciled(
    ctrl: ReconciledPayload,
    { deferOmitted, holdsReconcile }: { deferOmitted: boolean; holdsReconcile: boolean },
  ): ReconcileOutcome {
    this.reconciledWire = this.owner.wire
    this.committing = true
    this.session = ctrl.sessionId
    // From here on no RECONCILE out names its channels: what `closeUnacknowledged` sends goes as the wire takes it.
    const inFlight = this.takeInFlight() ?? reconcileInFlight()
    const serverHas = this.settleAnswers(ctrl, inFlight)
    // Channels registered after this RECONCILE was built wait for the next one.
    const hasNewChannels = this.channels.hasUndeclared
    const { frames: replays, channelsToOpen, omitted } = this.attachNamed(inFlight, serverHas)
    // Before the drain: what their close sends queues behind the replays.
    if (!deferOmitted) this.closeUnacknowledged(omitted)

    // What the server has: the channels this RECONCILE named that it attached, and those the ones before named.
    const attached = (ix: number): boolean => {
      if (inFlight.ixes.has(ix)) return serverHas.has(ix)
      const record = this.channels.get(ix)
      return record !== undefined && !this.channels.isUndeclared(ix) && !record.awaited
    }
    const queued = this.gate.drain(attached)
    // The data of channels the server hasn't confirmed since a loss goes after the others', the one dated last first,
    // each channel's own order kept (the server orders each channel alone, and the sort is stable): what may not get
    // through doesn't hold up what may.
    const frames = [...replays, ...queued]
      .map((frame) => ({ frame, datedAt: this.datedAt(frame) }))
      .sort((a, b) => (a.datedAt === b.datedAt ? 0 : a.datedAt > b.datedAt ? -1 : 1))
      .map(({ frame }) => frame)

    if (hasNewChannels && !holdsReconcile) {
      appendReconcileBatch(frames, this.stageReconcileBatch())
    } else {
      this.exitReconciling()
      // The barrier leaves out what the follow-up would name.
      if (hasNewChannels) this.scheduleRegisterReconcile()
    }

    return { frames, channelsToOpen, omitted: deferOmitted ? omitted : [], reconcileComplete: !hasNewChannels }
  }

  /** Since when the server hasn't confirmed the data of `frame`'s channel, `Infinity` where it has it all or `frame` is
   *  no channel's sequenced data. */
  private datedAt({ frame }: OutboundFrame): number {
    const tag = frame[0]
    if (tag === undefined || !isSequencedTag(tag)) return Number.POSITIVE_INFINITY
    return this.channels.get(readU16(frame, 1))?.replay.stall?.since ?? Number.POSITIVE_INFINITY
  }

  /** Settles what `inFlight` asked with the RECONCILED that answers it: its probes, the ATTACH_RESULT frames that came
   *  first, and which initial channels the server awaits. Returns the last seq the server has of each channel it
   *  attached. */
  private settleAnswers(
    ctrl: ReconciledPayload,
    { ixes, probes, attachResults }: ReconcileInFlight,
  ): Map<number, number> {
    const serverHas = new Map<number, number>()
    for (const channel of ctrl.open) serverHas.set(channel.ix, channel.lastSeq)
    for (const [ix, probe] of probes) this.channels.get(ix)?.channel._attach.receiveCredit.onProbeAnswered(probe)
    for (const [ix, lastSeq] of attachResults) if (lastSeq !== null && !serverHas.has(ix)) serverHas.set(ix, lastSeq)
    // An initial channel left out is one the server awaits, until its ATTACH_RESULT.
    for (const [ix, initial] of ixes) {
      const record = this.channels.get(ix)
      if (record !== undefined) record.awaited = initial && !serverHas.has(ix) && !attachResults.has(ix)
    }
    return serverHas
  }

  /** Brings each channel `inFlight` named and the server attached to the server's state: the frames that replays, the
   *  channels that open on the wire, and those the server no longer has. */
  private attachNamed(
    { ixes, carriedFrom }: ReconcileInFlight,
    serverHas: Map<number, number>,
  ): { frames: OutboundFrame[]; channelsToOpen: MuxChannel[]; omitted: number[] } {
    const replays: Replayed[] = []
    const channelsToOpen: MuxChannel[] = []
    const omitted: number[] = []
    for (const ix of ixes.keys()) {
      const record = this.channels.get(ix)
      if (record === undefined || record.awaited) continue
      const lastSeq = serverHas.get(ix)
      if (lastSeq === undefined) {
        // One the server no longer has needs nothing more from the page, if closed; else it closes unacknowledged.
        if (record.state.tag === 'closed') this.gate.release(record)
        else omitted.push(ix)
        continue
      }
      // The replay stops before the first seq the wire carries behind the RECONCILE.
      const carriedSeq = carriedFrom.get(ix) ?? Infinity
      for (const replayed of this.attachRecord(record, lastSeq, carriedSeq - 1)) replays.push(replayed)
      if (record.state.tag !== 'closed') channelsToOpen.push(record.channel)
    }
    // Across its channels, what the page sent goes again in the order it was sent.
    replays.sort((a, b) => a.handed - b.handed)
    return { frames: replays.map(({ frame }) => frame), channelsToOpen, omitted }
  }

  /** An awaited channel's own RECONCILED entry: `lastSeq` if the server attached it, null if not. False where it
   *  answers no channel the server awaited, or waits for the RECONCILED in flight. */
  handleAttachResult(ix: number, lastSeq: number | null): boolean {
    const inFlight = this.reconciledWait?.inFlight
    if (inFlight?.ixes.has(ix)) {
      inFlight.attachResults.set(ix, lastSeq)
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
    // As `applyReconciled` does: its replay, then what it queued, in order with what other channels queued meanwhile.
    const { transport } = this.owner
    for (const { frame } of this.replayTo(record, lastSeq)) transport.sendFrame(frame)
    this.gate.sendRunnable()
    if (opened && record.state.tag === 'open') {
      record.channel._onTransportOpen(transport.batched, transport.uplink, transport.downlink)
    }
    return true
  }

  closeUnacknowledged(ixes: number[]): void {
    for (const ix of ixes) {
      const record = this.channels.get(ix)
      if (!record) continue
      this.gate.release(record)
      record.channel._onConnectionLost(new NetworkError('Channel not acknowledged by server after reconnect', true))
    }
  }

  // ── The await-RECONCILED window ──

  /** Opens the wait for a new RECONCILE's RECONCILED and arms its deadline, which, where `receipted`, runs till the
   *  receipt from what the server takes. A follow-up RECONCILE for late registrations opens it again, restarting it.
   *  Returns what the RECONCILE asks, which it fills as it is built. */
  private enterReconciling(receipted: boolean): ReconcileInFlight {
    if (this.reconciledWait !== null) clearTimeout(this.reconciledWait.timer)
    const inFlight = reconcileInFlight()
    this.reconciledWait = {
      receipt: receipted ? 'awaited' : 'none',
      timeoutMs: RECONCILE_TIMEOUT_MS,
      timer: setTimeout(() => this.onReconcileTimeout(), RECONCILE_TIMEOUT_MS),
      carrierOut: false,
      inFlight,
    }
    return inFlight
  }

  /** What the RECONCILE out asked, which its RECONCILED answers: the wait keeps its deadline, no longer the ask. */
  private takeInFlight(): ReconcileInFlight | null {
    const wait = this.reconciledWait
    if (wait === null) return null
    const { inFlight } = wait
    wait.inFlight = null
    return inFlight
  }

  /** The RECONCILE's deadline runs from now: the server answered the request that carried it. */
  restartReconcileDeadline(): void {
    const wait = this.reconciledWait
    if (wait === null) return
    wait.carrierOut = false
    if (wait.receipt === 'none') this.armReconcileDeadline(wait, wait.timeoutMs)
  }

  /** A later RECONCILE went in a batch POST of `bytes`: its deadline runs from now, with the time that POST takes. */
  reconcileCarried(bytes: number): void {
    const wait = this.reconciledWait
    if (wait === null || wait.receipt !== 'none' || this.boundsWire) return
    wait.carrierOut = true
    wait.timeoutMs = RECONCILE_TIMEOUT_MS + (2 * bytes) / RECONCILE_FLOOR_BYTES_PER_MS
    this.armReconcileDeadline(wait, wait.timeoutMs)
  }

  /** The server took the RECONCILE whole: its RECONCILED comes within the deadline. */
  takeReceipt(): void {
    const wait = this.reconciledWait
    if (wait === null || wait.receipt !== 'awaited') return
    wait.receipt = 'taken'
    this.armReconcileDeadline(wait, RECONCILE_TIMEOUT_MS)
  }

  /** A piece of the RECONCILED came: the rest comes within the deadline. */
  reconciledPieceArrived(): void {
    const wait = this.reconciledWait
    if (wait !== null && wait.receipt === 'taken') this.armReconcileDeadline(wait, RECONCILE_TIMEOUT_MS)
  }

  private armReconcileDeadline(wait: ReconciledWait, ms: number): void {
    clearTimeout(wait.timer)
    wait.timer = setTimeout(() => this.onReconcileTimeout(), ms)
  }

  private reconcileDeadlineIn(ms: number): ReturnType<typeof setTimeout> {
    return setTimeout(() => this.onReconcileTimeout(), ms)
  }

  /** Closes the wait: the RECONCILED settled, the wire was lost, or the connection was disposed. */
  private exitReconciling(): void {
    if (this.reconciledWait !== null) {
      clearTimeout(this.reconciledWait.timer)
      this.reconciledWait = null
    }
    for (const resolve of this.reconcileSettledWaiters.splice(0)) resolve()
  }

  /** The RECONCILED didn't come by the deadline. Unless, awaiting its receipt, the server took more of the page's bytes,
   *  or, a later RECONCILE without one and no batch POST of it out unanswered, the wire delivered something meanwhile, it
   *  is dropped as on a missed pong, and `handleTransportLoss` reconnects. A wire's first RECONCILE has nothing of the
   *  page's ahead of it, so what else the wire says (an SSE comment, a PONG) doesn't answer it. */
  private onReconcileTimeout(): void {
    const wait = this.reconciledWait
    if (wait === null) return
    const { transport } = this.owner
    const { heartbeat } = transport
    // A later one waits behind what the page sent before: the wire is dead once it delivered nothing for the deadline.
    let quiet =
      wait.receipt === 'none' && !this.boundsWire && !wait.carrierOut ? (heartbeat?.quietFor() ?? Infinity) : Infinity
    // Till its receipt, it crosses while the server takes more of the page's bytes.
    if (wait.receipt === 'awaited') quiet = heartbeat?.unconfirmedFor() ?? Infinity
    if (quiet < RECONCILE_TIMEOUT_MS) return this.armReconcileDeadline(wait, RECONCILE_TIMEOUT_MS - quiet)
    this.owner.onReconcileLate(transport)
  }

  // ── The RECONCILE ──

  private buildReconcileFrame(): OutboundFrame {
    const inFlight = this.enterReconciling(this.owner.transport.type === CHANNEL_TRANSPORT.WS)
    const { uplink } = this.owner.transport
    // The wire that has the session holds what the RECONCILEs before named: one there names what changed since.
    const changes = this.session !== null && this.reconciledWire === this.owner.wire
    const records = changes ? this.channels.changed() : this.channels.values()
    const open = this.declareOpenEntries(records, inFlight, {
      skipUndeclared: false,
      uplink,
    })
    const released = this.channels.takeReleased()
    const reconcile: ReconcilePayload = { sessionId: this.session ?? this.ownId, open }
    // What changed goes on the wire that holds the session; a wire's first RECONCILE numbers it.
    if (changes) reconcile.released = released
    else reconcile.attempt = this.nextAttempt()
    const frame = encode.reconcile(reconcile)
    // With no receipt, nothing says how far it crossed: it has the time it and its RECONCILED take at the floor too.
    const wait = this.reconciledWait
    if (wait !== null && wait.receipt === 'none') {
      wait.timeoutMs = RECONCILE_TIMEOUT_MS + (2 * frame.byteLength) / RECONCILE_FLOOR_BYTES_PER_MS
      this.armReconcileDeadline(wait, wait.timeoutMs)
    }
    return { kind: 'reconcile', frame }
  }

  private declareOpenEntries(
    records: Iterable<ChannelRecord>,
    inFlight: ReconcileInFlight,
    {
      skipUndeclared,
      uplink,
    }: {
      skipUndeclared: boolean
      uplink: Uplink
    },
  ): ReconcileOpenEntry[] {
    const open: ReconcileOpenEntry[] = []
    const wanting = new Set<number>()
    for (const record of records) {
      const { ix } = record
      // The server has all a delivered one sent it, or ended it: leaving it out lets it go there too.
      if (record.state.tag === 'closed' && record.state.delivered) {
        this.gate.release(record)
        continue
      }
      const isInitial = this.channels.isInitial(record)
      if (skipUndeclared && isInitial && !record.awaited) continue
      this.channels.markDeclared(record)
      inFlight.ixes.set(ix, isInitial)
      const payloadEntry: ReconcileOpenEntry = {
        id: record.channel.id,
        ix,
        lastSeq: record.lastSeq,
      }
      if (isInitial) payloadEntry.initial = true
      const state = record.channel._attach.state(uplink)
      if (state.probe !== undefined) inFlight.probes.set(ix, state.probe)
      Object.assign(payloadEntry, state)
      if (state.want) wanting.add(ix)
      open.push(payloadEntry)
    }
    // The declared wants supersede the WANT frames queued before them.
    if (wanting.size > 0) this.gate.dropWantFrames(wanting)
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
    const inFlight = this.reconciledWait?.inFlight
    assert(inFlight)
    return this.gate.drainForReconcile(inFlight.carriedFrom)
  }

  /** Whether a replay holds a frame the page sent. */
  private sentBefore(): boolean {
    for (const { replay } of this.channels.values()) if (replay.length > 0) return true
    return false
  }

  // ── The RECONCILED ──

  /** What a channel the RECONCILE named and the server attached replays to it, its state brought to the server's. */
  private attachRecord(record: ChannelRecord, lastSeq: number, throughSeq: number): Replayed[] {
    if (record.state.tag === 'closed') this.channels.serverHas(record, record.state, lastSeq)
    if (record.state.tag === 'pending') this.channels.open(record)
    return this.replayTo(record, lastSeq, throughSeq)
  }

  /** What the server lacks through `throughSeq`: the replay, or an ERROR ending the channel if the replay lost some,
   *  which goes ahead of any replay. */
  private replayTo(record: ChannelRecord, lastSeq: number, throughSeq = Infinity): Replayed[] {
    record.replay.acknowledge(lastSeq)
    const missed = record.replay.getAfter(lastSeq, throughSeq)
    if (missed !== null)
      return missed.map(({ kind, frame, handed }) => ({ frame: { kind, frame, released: true }, handed }))
    // One the server ended needs nothing more.
    if (record.state.tag === 'closed' && record.state.delivered) return []
    return [{ frame: this.loseChannel(record), handed: 0 }]
  }

  /** Ends the channel on both ends, dropping its queue; the ERROR isn't replayed, as the next reconcile sends one. */
  private loseChannel({ ix, replay, channel }: ChannelRecord): OutboundFrame {
    this.gate.dropQueue(ix)
    const frame = encode.error(ix, ERROR_REASON.LOST, replay.nextSeq())
    channel._onEnded(END_CAUSE.lost.error())
    return { kind: 'control', frame }
  }
}

/** A frame a replay sends again, with when the page first handed it among all its channels' frames. */
type Replayed = { frame: OutboundFrame; handed: number }

function appendReconcileBatch(target: OutboundFrame[], reconcileBatch: ReconcileBatch): void {
  target.push(reconcileBatch.reconcileFrame)
  for (const frame of reconcileBatch.movedBufferedFrames) target.push(frame)
}
