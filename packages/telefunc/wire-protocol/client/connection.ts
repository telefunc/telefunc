export { ClientConnection }
export type {
  BarrierEmission,
  MuxChannel,
  MuxConnection,
  OutboundFrame,
  OutboundFrameKind,
  ProbeWire,
  UpgradeSource,
  UpgradeTarget,
}

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { ChannelOverflowError, replayLossError } from '../channel-errors.js'
import { NetworkError } from '../../shared/NetworkError.js'
import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MIN_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
  MAX_CHANNELS_PER_CONNECTION,
  UPGRADE_HANDOFF_BUFFER_BYTES,
  UPGRADE_HANDOFF_BUFFER_FRAMES,
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS,
  UPGRADE_ATTEMPT_TIMEOUT_MS,
  type ChannelTransport,
  type ChannelTransports,
  TIMER_DELAY_MAX_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../constants.js'
import { replayWindow, type ByteWindowKind } from '../flow-control/flow-control.js'
import { ReplayBuffer } from '../replay-buffer.js'
import {
  ACK_STATUS,
  ERROR_REASON,
  TAG,
  encode,
  isSequencedFrame,
  chargedBytes,
  seqNear,
  seqThrough,
} from '../shared-ws.js'
import type {
  AckResultStatus,
  BdpPingAck,
  ChannelFrame,
  DecodedFrame,
  PingEntry,
  PongEntry,
  ReadyPayload,
  ReattachState,
  ReconcileOpenEntry,
  ReconcilePayload,
  ReconciledPayload,
} from '../shared-ws.js'
import { randomUuid } from '../../utils/randomUuid.js'
import { Fifo } from '../../utils/Fifo.js'
import { settledOrAborted } from '../../utils/settledOrAborted.js'
import { Heartbeat } from './heartbeat.js'
import type { Uplink } from './uplink.js'
import type { Downlink } from './downlink.js'
import { WsTransport } from './ws.js'
import { SseTransport } from './sse.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}
type SequencedFrame = BufferedFrame & { seq: number }

/** Probe wire returned by `WsTransport.probe`. Liveness is the consumer's responsibility
 *  until the swap commits — typically driven via a transient Heartbeat. */
type ProbeWire = {
  ping: () => void
  onPong: (cb: () => void) => void
  onClose: (cb: () => void) => void
  /** `send`/`onFrame` exchange PREPARE/READY without the probe becoming the transport — adopting it
   *  early would put a sessionless wire in `this.transport`. PONG stays on `onPong`. */
  send: (frame: Uint8Array<ArrayBuffer>) => void
  onFrame: (cb: (frame: DecodedFrame, byteLength: number) => void) => void
  close: () => void
  /** The uplink of the wire it opens. */
  uplink: Uplink
}

type ProbeSession = {
  upgradeId: string
  sessionId: string
  probeHeartbeat: Heartbeat
  uplink: Uplink
}

/** Kept apart, as recovery differs: `emitted` may have reached the server, `not-emitted` leaves the wire usable. */
type BarrierEmission = 'emitted' | 'not-emitted'

type OutboundFrameKind = 'reconcile' | 'released' | 'control' | ByteWindowKind | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
}

interface MuxChannel {
  readonly id: string
  readonly isClosed: boolean
  /** The largest frame its server takes. */
  readonly _maxFrameBytes: number
  /** It opened on `uplink`'s wire: the one of the channel's last attach means that same wire, which lost nothing. */
  _onTransportOpen(batched: boolean, uplink: Uplink, downlink: Downlink): void
  /** Its frames go in batch POSTs from now on: the wire's streaming upload failed after the channel opened. */
  _onTransportBatched(): void
  /** Entry point for every per-channel wire frame (data + per-channel ctrl). The
   *  channel splits ctrl vs data internally. Connection-level frames (PING/PONG/
   *  FIN/RECONCILED), channel-termination ctrls (ABORT/ERROR) and ATTACH_RESULT stay with the
   *  connection — they involve connection-side cleanup. */
  _dispatchFrame(frame: ChannelFrame): void
  _onTransportClose(err?: Error): void
  /** Its RECONCILE entry on each (re)attach to `uplink`'s wire; `batched`: its flow-control frames wait for a POST. */
  _reattachState(batched: boolean, uplink: Uplink): ReattachState
  /** Its RECONCILE entry's `probe` was answered by the RECONCILED. */
  _onProbeAnswered(probe: number): void
  /** The largest windows the replay buffers allow: the one the page grants, and the one the server grants it. */
  _fitReplays(window: number, peerWindow: number): void
  /** At each heartbeat, a WINDOW for what arrived since the last: the server's replay lets it go while quiet. */
  _acknowledge(): void
  /** It answered the newest frame the server sent it, which acknowledges every frame before it too. */
  _answered(): void
}

interface MuxConnection {
  send(channel: MuxChannel, data: string): number
  sendPublishAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void
  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void
  sendTextAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void
  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void
  sendBinary(channel: MuxChannel, data: Uint8Array): number
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel, abortValue: string): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendByteWindowUpdate(channel: MuxChannel, limit: number, kind: ByteWindowKind): void
  sendMsgWindowUpdate(channel: MuxChannel, limit: number, urgent: boolean): void
  sendBdpPing(channel: MuxChannel, probe: number): void
  sendBdpPingAck(channel: MuxChannel, ack: BdpPingAck): void
  /** Bytes the wire holds that haven't gone out. */
  bufferedAmount(): number
  /** The bytes the wire delivered in the `ms` before it last delivered: before a frame handled as it arrives. */
  deliveredBeforeLast(ms: number): number
  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void
  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void
  unregister(channel: MuxChannel): void
  reconnectWindow(): number
}

type ReconcileOutcome = {
  frames: OutboundFrame[]
  channelsToOpen: MuxChannel[]
  reconcileComplete: boolean
}

/** The RECONCILE in flight. */
type ReconcileInFlight = {
  /** The channels it lists, and whether as `initial`. */
  ixes: Map<number, boolean>
  /** The probes it carries, by channel: its RECONCILED answers each a BDP_PING_ACK didn't. */
  probes: Map<number, number>
  /** Per channel, the first seq its wire carries behind it: the RECONCILED replays only the frames before it. */
  carriedFrom: Map<number, number>
  /** Applied with its RECONCILED, which the server may build before them (an SSE POST's, once its body is read). */
  attachResults: Map<number, number | null>
}

function reconcileInFlight(): ReconcileInFlight {
  return { ixes: new Map(), probes: new Map(), carriedFrom: new Map(), attachResults: new Map() }
}

type ReconcileBatch = {
  reconcileFrame: OutboundFrame
  movedBufferedFrames: OutboundFrame[]
}

type ReconcileBufferedFramesMode = 'batch-on-reconcile' | 'release-after-reconciled'

type ClientConnectionOptions = {
  transports: ChannelTransports
  fetchImpl: typeof fetch
  /** Client-side cache-key extension — distinct values get distinct `ClientConnection` instances. Never sent on the wire. */
  connectionKey?: string
  /** User headers (config.headers + per-call `withContext({ headers })`) merged into every transport fetch. */
  headers?: Record<string, string>
  /** Override the idle-close delay after all channels close. Default: 60 000 ms. Pass 0 to dispose immediately. */
  idleTimeout?: number
}

type ClientChannelTransport = {
  readonly type: ChannelTransport
  readonly sendReconcileOnOpen: boolean
  readonly reconcileMode: ReconcileBufferedFramesMode
  /** True iff client→server frames are per-POST batched instead of pushed onto
   *  one streaming body — signals the channel to use a larger initial window. */
  readonly batched: boolean
  /** The uplink and the downlink of the wire it carries, or opens next. */
  readonly uplink: Uplink
  readonly downlink: Downlink
  start(): void
  hasWire(): boolean
  isConnecting(): boolean
  /** Send a connection-level ping on this wire. Heartbeat's send callback calls this. */
  sendPing(frame: Uint8Array<ArrayBuffer>): void
  sendFrame(frame: OutboundFrame): void
  /** The bytes of data frames it takes now, `Infinity` where nothing bounds them: the others go at once. */
  sendRoom(): number
  /** Bytes of the frames it was handed that haven't gone out to the network. */
  bufferedAmount(): number
  abandonActiveTransport(): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: ReconciledPayload): void
  /** Its wire's, which tracks what the wire delivers. */
  readonly heartbeat: Heartbeat | null
  /** Connection constructs the Heartbeat (with the funnel-bound onDead) and hands it over.
   *  Transport's frame receive path routes PONG to it directly (`heartbeat?.resetPong()`). */
  attachHeartbeat(hb: Heartbeat): void
  detachHeartbeat(): void
  /** Settles once the frames it was handed have left it, or its wire is gone. */
  drained(): Promise<void>
  dispose(): void
}

/** The wire an upgrade moves FROM: it has to carry the barrier as its last frame. */
type UpgradeSource = ClientChannelTransport & {
  emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission>
}

/** The wire an upgrade moves TO: it has to be openable without becoming the transport, and
 *  adoptable once the barrier commits. */
type UpgradeTarget = ClientChannelTransport & {
  probe(): Promise<ProbeWire | null>
  adoptProbe(): void
}

type ConnectionState =
  | { tag: 'fresh' }
  | { tag: 'open'; upgrade: UpgradeState }
  | {
      tag: 'reconnecting'
      attempt: number
      startedAt: number
      timer: ReturnType<typeof setTimeout>
    }
  | { tag: 'closed' }

type UpgradeState =
  | { tag: 'none' }
  | {
      tag: 'staging'
      attempt: AbortController
      deadline: ReturnType<typeof setTimeout> | null
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

/** `closed`: unregistered, and still listed till released, as a RECONCILE leaving it out ends it on the server. */
type ChannelState =
  | { tag: 'pending'; initial: boolean }
  | { tag: 'open' }
  | {
      tag: 'closed'
      initial: boolean
      /** The server has all the page sent on it, or ended it. */
      delivered: boolean
      /** A PING named it. */
      reported: boolean
    }

type ClosedState = Extract<ChannelState, { tag: 'closed' }>

type ChannelEntry = {
  channel: MuxChannel
  state: ChannelState
}

class ClientConnection implements MuxConnection {
  private static cache = new Map<string, ClientConnection>()

  static getOrCreate(telefuncUrl: string, channel: MuxChannel, options: ClientConnectionOptions): ClientConnection {
    // `connectionKey` opts callers out of the shared connection without the server seeing it.
    const key = `${options.transports.join(',')}:${telefuncUrl}|${options.connectionKey ?? ''}`
    let connection = ClientConnection.cache.get(key)
    // Wire indexes are u16 and never reused, so a new channel starts a fresh connection once they run out.
    if (!connection || connection.closed || connection.nextIndex > 0xffff) {
      connection = new ClientConnection(telefuncUrl, options, key)
      ClientConnection.cache.set(key, connection)
    }
    connection.register(channel)
    return connection
  }

  private readonly cacheKey: string
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  private transport: ClientChannelTransport

  private state: ConnectionState = { tag: 'fresh' }
  /** Sticky after a permanent upgrade abort — survives every state transition until dispose. */
  private upgradeDisabled = false
  /** Server-allowed transports from the last settled RECONCILED. Kept so `maybeStartUpgrade`
   *  can run from both `handleReconciled` and `_onTransportOpen` — transport-open and the
   *  first RECONCILED can arrive in either order on the SSE path. */
  private serverTransports: ReconciledPayload['transports'] | null = null
  /** RECONCILE sent, awaiting RECONCILED. SSE sets it true during connecting since the
   *  initial reconcile is baked into the openStream POST body. Bounded by `reconcileTimer`:
   *  written only via `enterReconciling`/`exitReconciling` so the deadline can never outlive
   *  the state it guards. */
  private reconciling = false
  private reconcileTimer: ReturnType<typeof setTimeout> | null = null
  /** Resolved once no RECONCILE is in flight. */
  private reconcileSettledWaiters: (() => void)[] = []
  private ttl: ReturnType<typeof setTimeout> | null = null
  private get closed(): boolean {
    return this.state.tag === 'closed'
  }
  private get connected(): boolean {
    return this.state.tag === 'open'
  }
  private get committing(): CommittingUpgrade | null {
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'committing') return null
    return this.state.upgrade
  }

  /** The flip is an EVENT inside `committing`, not a transition out of it: `flipped ≡ transport === to`. */
  private get flipped(): boolean {
    const u = this.committing
    return u !== null && this.transport === u.to
  }

  /** Sends are gated from the barrier's emission until its COMMITTED lands. */
  private get upgradeGatesSends(): boolean {
    const u = this.committing
    return u !== null && !u.committed
  }

  private sessionId: string | null = null
  /** Advances each time the connection moves to another wire: what went out on the one before may not have arrived. */
  private wire = 0
  /** The wire the last RECONCILED came on. */
  private reconciledWire = -1
  private nextIndex = 0
  private inFlight = reconcileInFlight()
  /** Initial channels a RECONCILED left out, till their ATTACH_RESULT: what they queue waits, so replays go first. */
  private awaitedIxes = new Set<number>()
  private channels = new Map<number, ChannelEntry>()
  /** The channels not closed: `unregister` takes one out while `channels` still lists it. */
  private channelIndex = new Map<MuxChannel, number>()
  /** Frames held while sends are, or for their channel's ATTACH_RESULT. */
  private sendBuffer: BufferedFrame[] = []
  /** Sequenced frames waiting only for the transport's room, in order: a channel's next send waits behind them. */
  private heldForRoom = new Fifo<SequencedFrame>()
  private heldForRoomBytes = 0
  /** Channels whose queued frames go, all in one pass when they're next read (see `settleQueue`). */
  private droppedQueues = new Set<number>()
  private lastSeqByChannel = new Map<number, number>()
  private replayBuffers = new Map<number, ReplayBuffer>()
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs: number
  private pingIntervalMs = CHANNEL_PING_INTERVAL_MS
  /** The heartbeat last installed is the provisional one before a first RECONCILED (see `beatFromReconcile`). */
  private provisionalHeartbeat = false
  private clientReplayBufferBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BYTES
  private clientReplayBufferBinaryBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES
  /** The windows the replay buffers allow, the page's and the server's, as the last RECONCILED says them. */
  private replayWindows: { window: number; peerWindow: number } | null = null
  /** The largest frame the server takes, as the last RECONCILED says it. */
  private maxFrameBytes = WIRE_MAX_RAW_FRAME_BYTES
  private constructor(telefuncUrl: string, options: ClientConnectionOptions, cacheKey: string) {
    this.cacheKey = cacheKey
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.idleTimeoutMs = options.idleTimeout ?? CHANNEL_IDLE_TIMEOUT_MS
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
  }

  // ── State transitions: every `this.state =` write goes through these. ──

  private enterOpen(): void {
    if (this.state.tag === 'open') return
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
  }

  /** Owns the reconnect timer's lifecycle so callers can't forget to cancel a prior one. */
  private enterReconnecting(attempt: number, startedAt: number, delay: number): void {
    if (this.state.tag === 'reconnecting') clearTimeout(this.state.timer)
    const timer = setTimeout(() => this.transport.start(), delay)
    this.state = { tag: 'reconnecting', attempt, startedAt, timer }
  }

  private enterClosed(): void {
    this.state = { tag: 'closed' }
  }

  private enterUpgradeStaging(attempt: AbortController): void {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'none')
    this.state = { tag: 'open', upgrade: { tag: 'staging', attempt, deadline: null, ready: false } }
  }

  private enterUpgradeReady(attempt: AbortController): void {
    assert(this.state.tag === 'open')
    const u = this.state.upgrade
    assert(u.tag === 'staging' && u.attempt === attempt)
    u.ready = true
  }

  /** READY came, so the barrier goes next: a RECONCILE too, and no other goes out till the handoff. */
  private get upgradeReady(): boolean {
    return this.state.tag === 'open' && this.state.upgrade.tag === 'staging' && this.state.upgrade.ready
  }

  private armAttemptDeadline(attempt: AbortController): void {
    assert(this.state.tag === 'open')
    const u = this.state.upgrade
    assert(u.tag === 'staging' && u.attempt === attempt && u.deadline === null)
    u.deadline = setTimeout(() => attempt.abort(), UPGRADE_ATTEMPT_TIMEOUT_MS)
  }

  private enterUpgradeCommitting(
    from: UpgradeSource,
    to: UpgradeTarget,
    session: ProbeSession,
    attempt: AbortController,
  ): void {
    assert(this.state.tag === 'open')
    const u = this.state.upgrade
    assert(u.tag === 'staging' && u.attempt === attempt)
    this.state = {
      tag: 'open',
      upgrade: {
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
      },
    }
  }

  private exitUpgradeAttempt(attempt: AbortController): void {
    if (this.state.tag !== 'open') return
    const u = this.state.upgrade
    if (u.tag === 'none' || u.attempt !== attempt) return
    if (u.tag === 'committing' && this.transport === u.to) return
    if (u.deadline) clearTimeout(u.deadline)
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
  }

  private exitUpgradeCommitting(): CommittingUpgrade {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'committing')
    const u = this.state.upgrade
    if (u.deadline) clearTimeout(u.deadline)
    if (u.joinTimer) clearTimeout(u.joinTimer)
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
    return u
  }

  private get sendsOpen(): boolean {
    return this.connected && !this.reconciling && !this.upgradeGatesSends && this.registerReconcileTimer === null
  }

  private canSendImmediately(ix: number): boolean {
    return this.sendsOpen && !this.awaitedIxes.has(ix)
  }

  // ── Per-channel state transitions: every `entry.state =` write goes through these. ──

  private enterChannelPending(ix: number, channel: MuxChannel, initial: boolean): void {
    this.channels.set(ix, { channel, state: { tag: 'pending', initial } })
    this.channelIndex.set(channel, ix)
  }

  private enterChannelOpen(ix: number): void {
    const entry = this.channels.get(ix)
    assert(entry && entry.state.tag === 'pending')
    entry.state = { tag: 'open' }
  }

  private enterChannelClosed(ix: number): void {
    const entry = this.channels.get(ix)
    assert(entry && entry.state.tag !== 'closed')
    const initial = entry.state.tag === 'pending' && entry.state.initial
    entry.state = { tag: 'closed', initial, delivered: false, reported: false }
  }

  private register(channel: MuxChannel): void {
    // Every RECONCILE lists every channel, the closed ones too, so the oldest the server has all of makes way.
    if (this.channels.size === MAX_CHANNELS_PER_CONNECTION) {
      for (const [ix, entry] of this.channels) {
        if (entry.state.tag !== 'closed' || !entry.state.delivered) continue
        this.releaseChannel(ix, entry.channel)
        break
      }
    }
    assertUsage(
      this.channels.size < MAX_CHANNELS_PER_CONNECTION,
      `Too many channels on one connection (${MAX_CHANNELS_PER_CONNECTION} max) — open another with \`connectionKey\``,
    )
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    const ix = this.nextIndex++
    this.enterChannelPending(ix, channel, true)
    this.replayBuffers.set(ix, new ReplayBuffer(this.clientReplayBufferBytes, this.clientReplayBufferBinaryBytes))
    this.fitReplays(channel)

    if (!this.transport.hasWire() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    this.scheduleRegisterReconcile()
  }

  /** Fits the replays' windows once a RECONCILED says them; till then the defaults' initial window fits any budget. */
  private fitReplays(channel: MuxChannel): void {
    if (this.replayWindows === null) return
    channel._fitReplays(this.replayWindows.window, this.replayWindows.peerWindow)
  }

  /** The transport may have room for data again: what waited for it goes. */
  _onTransportRoom(transport: ClientChannelTransport): void {
    if (transport === this.transport && this.sendsOpen) this.sendHeldForRoom()
  }

  /** How long a gone server is still held: until its loss is noticed at the pong deadline, then for `reconnectTimeout`. */
  reconnectWindow(): number {
    return Math.min(TIMER_DELAY_MAX_MS, 2 * this.pingIntervalMs + this.reconnectTimeoutMs)
  }

  private registerReconcileTimer: ReturnType<typeof setTimeout> | null = null
  /** Coalesces sync-burst registrations into one RECONCILE round-trip. */
  private scheduleRegisterReconcile(): void {
    if (this.registerReconcileTimer !== null) return
    this.registerReconcileTimer = setTimeout(() => this.flushPendingRegisterReconcile(), 0)
  }

  /** Send the queued RECONCILE on the live wire. No-op when nothing's queued. While the wire
   *  can't take it (connecting, awaiting RECONCILED, from an upgrade's READY until its handoff)
   *  the obligation is KEPT, not consumed — `registerReconcileTimer` stays non-null as the marker (which also keeps
   *  `canSendImmediately` false so data frames buffer behind the RECONCILE), and the
   *  transitions that make the wire sendable re-invoke this: `_onTransportOpen`, a settled
   *  RECONCILED, upgrade-attempt exit, and handoff completion. */
  private flushPendingRegisterReconcile(): void {
    if (this.registerReconcileTimer === null) return
    if (this.state.tag !== 'open' || this.state.upgrade.tag === 'committing' || this.upgradeReady) return
    if (this.reconciling) return
    this.sendReconcileBatch(this.stageReconcileBatch(false))
  }

  private cancelRegisterReconcileTimer(): void {
    if (this.registerReconcileTimer === null) return
    clearTimeout(this.registerReconcileTimer)
    this.registerReconcileTimer = null
  }

  /** The channel sends nothing more. */
  unregister(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.channelIndex.delete(channel)
    this.enterChannelClosed(ix)
    this.startTtlIfIdle()
  }

  send(channel: MuxChannel, data: string): number {
    const frame = this.sendSequenced(channel, 'data', (ix, seq) => encode.text(ix, data, seq))
    return frame ? chargedBytes(frame) : 0
  }

  sendBinary(channel: MuxChannel, data: Uint8Array): number {
    const frame = this.sendSequenced(channel, 'data', (ix, seq) => encode.binary(ix, data, seq))
    return frame ? chargedBytes(frame) : 0
  }

  sendPublishAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void {
    this.sendSequenced(channel, 'ack', (ix, seq) => encode.publishAckReq(ix, data, seq), onQueued)
  }

  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void {
    this.sendSequenced(channel, 'ack', (ix, seq) => encode.publishBinaryAckReq(ix, data, seq), onQueued)
  }

  sendTextAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void {
    this.sendSequenced(channel, 'ack', (ix, seq) => encode.textAckReq(ix, data, seq), onQueued)
  }

  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void {
    this.sendSequenced(channel, 'ack', (ix, seq) => encode.binaryAckReq(ix, data, seq), onQueued)
  }

  /** Encodes at the next seq, then takes it, so a refused frame takes none; `onQueued` runs before the wire has it. */
  private sendSequenced(
    channel: MuxChannel,
    kind: 'data' | 'ack',
    buildFrame: (ix: number, seq: number) => Uint8Array<ArrayBuffer>,
    onQueued?: (seq: number) => void,
  ): Uint8Array<ArrayBuffer> | undefined {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const frame = buildFrame(ix, replay.seq + 1)
    const maxFrameBytes = this.maxFrameBytesOf(channel)
    // The server ends the wire a larger frame arrives on, and the one its replay arrives on next.
    assertUsage(
      frame.byteLength <= maxFrameBytes,
      `Channel message too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`,
    )
    const seq = replay.nextSeq()
    onQueued?.(seq)
    this.sendOrQueue(ix, seq, frame, kind)
    return frame
  }

  /** Sends, or queues while sends are held or the transport lacks room; one sent at once passes the settle queue. */
  private sendOrQueue(ix: number, seq: number, frame: Uint8Array<ArrayBuffer>, kind: OutboundFrameKind): void {
    if (!this.canSendImmediately(ix)) {
      this.settleQueue().push({ frame, channelIx: ix, seq })
      return
    }
    // What it held goes first.
    this.sendHeldForRoom()
    if (this.heldForRoom.length === 0 && this.transport.sendRoom() > 0) {
      this.replayBuffers.get(ix)!.push(seq, frame)
      this.transport.sendFrame({ kind, frame })
      return
    }
    this.holdForRoom({ frame, channelIx: ix, seq })
  }

  private holdForRoom(entry: SequencedFrame): void {
    this.heldForRoom.push(entry)
    this.heldForRoomBytes += entry.frame.byteLength
  }

  /** What waits for room goes, from the front, while the transport has room. */
  private sendHeldForRoom(): void {
    if (this.heldForRoom.length === 0) return
    this.dropQueues()
    let room = this.transport.sendRoom()
    while (room > 0 && this.heldForRoom.length > 0) {
      const { frame, channelIx, seq } = this.heldForRoom.shift()!
      this.heldForRoomBytes -= frame.byteLength
      room -= frame.byteLength
      this.replayBuffers.get(channelIx)!.push(seq, frame)
      this.transport.sendFrame({ kind: 'released', frame })
    }
    this.startTtlIfIdle()
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const seq = this.replayBuffers.get(ix)!.nextSeq()
    let frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    const maxFrameBytes = this.maxFrameBytesOf(channel)
    // The server ends the wire a larger frame arrives on, so the send it answers gets an error instead.
    if (frame.byteLength > maxFrameBytes) {
      const error = `Channel ack answer too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`
      frame = encode.ackRes(ix, seq, ackedSeq, error, ACK_STATUS.ERROR)
    }
    this.sendOrQueue(ix, seq, frame, 'ack')
    if (ackedSeq === this.lastSeqByChannel.get(ix)) channel._answered()
  }

  /** The largest frame of `channel`'s the server takes: the channel's own limit, or its connection's once smaller. */
  private maxFrameBytesOf(channel: MuxChannel): number {
    return Math.min(channel._maxFrameBytes, this.maxFrameBytes)
  }

  sendAbort(channel: MuxChannel, abortValue: string): void {
    this.sendClosingFrame(channel, (ix, seq) => encode.abort(ix, abortValue, seq))
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    this.sendClosingFrame(channel, (ix, seq) => encode.close(ix, timeoutMs, seq))
  }

  sendCloseAck(channel: MuxChannel): void {
    this.sendClosingFrame(channel, (ix, seq) => encode.closeAck(ix, seq))
  }

  /** Sequenced as data is, so a closing frame a dead wire lost replays. */
  private sendClosingFrame(
    channel: MuxChannel,
    buildFrame: (ix: number, seq: number) => Uint8Array<ArrayBuffer>,
  ): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    this.sendOrQueue(ix, seq, buildFrame(ix, seq), 'control')
  }

  sendByteWindowUpdate(channel: MuxChannel, limit: number, kind: ByteWindowKind): void {
    this.sendUnsequenced(channel, kind, (ix) => encode.window(ix, limit, this.lastSeqByChannel.get(ix) ?? 0))
  }

  sendMsgWindowUpdate(channel: MuxChannel, limit: number, urgent: boolean): void {
    this.sendUnsequenced(channel, urgent ? 'urgent-flow-control' : 'flow-control', (ix) => encode.msgWindow(ix, limit))
  }

  sendBdpPing(channel: MuxChannel, probe: number): void {
    this.sendUnsequenced(channel, 'flow-control', (ix) => encode.bdpPing(ix, probe))
  }

  sendBdpPingAck(channel: MuxChannel, { probe, backlog, pathRtt }: BdpPingAck): void {
    this.sendUnsequenced(channel, 'flow-control', (ix) => encode.bdpPingAck(ix, probe, backlog, pathRtt))
  }

  bufferedAmount(): number {
    return this.transport.bufferedAmount() + this.heldForRoomBytes
  }

  deliveredBeforeLast(ms: number): number {
    return this.transport.heartbeat?.deliveredBefore(ms) ?? 0
  }

  /** Sends what `buildFrame` builds, or queues it while its channel waits: a limit is cumulative, so stays right. */
  private sendUnsequenced(
    channel: MuxChannel,
    kind: OutboundFrameKind,
    buildFrame: (ix: number) => Uint8Array<ArrayBuffer>,
  ): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = buildFrame(ix)
    if (!this.canSendImmediately(ix)) {
      this.settleQueue().push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind, frame })
  }

  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void {
    this.sendUnsequenced(channel, 'control', (ix) => encode.broadcastSub(ix, binary))
  }

  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void {
    this.sendUnsequenced(channel, 'control', (ix) => encode.broadcastUnsub(ix, binary))
  }

  _onTransportOpen(transport: ClientChannelTransport): void {
    if (this.closed) return
    if (transport !== this.transport) return
    this.enterOpen()
    if (this.transport.sendReconcileOnOpen) {
      this.sendReconcileBatch(this.stageReconcileBatch(false))
      this.beatFromReconcile(transport)
      return
    }
    if (!this.reconciling) {
      // A register-reconcile queued during the connecting window sends its RECONCILE here and
      // carries the buffered frames after it, emptying the buffer; with none queued, flush
      this.flushPendingRegisterReconcile()
      this.drainBufferedFramesToWire()
    }
    this.maybeStartUpgrade()
  }

  /** The wire's upload fell back to batch POSTs, which a channel may have opened before (see `_onTransportOpen`). */
  _onTransportBatched(transport: ClientChannelTransport): void {
    if (transport !== this.transport) return
    for (const { channel, state } of this.channels.values()) if (state.tag !== 'closed') channel._onTransportBatched()
  }

  _onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void {
    // A wire the page gave up still delivers, but its RECONCILED answers a RECONCILE the next wire's replaces.
    if (abandoned && frame.tag === TAG.RECONCILED) return
    const u = this.committing
    if (u !== null && this.transport === u.to) {
      this.ingestDuringHandoff(frame, source === u.from ? 'old' : 'new', byteLength)
    } else {
      this.dispatchFrame(frame)
    }
  }

  private dispatchFrame(frame: DecodedFrame): void {
    // Track seq for ALL sequenced frames, ACK_RES and the closing ones too; otherwise reconciles under-report lastSeq.
    if (isSequencedFrame(frame)) {
      const seq = this.trackSeq(frame.index, frame.seq)
      if (seq === null) return
      frame.seq = seq
    }
    if (frame.tag === TAG.WINDOW) this.serverHasThrough(frame.index, this.sentThrough(frame.index, frame.lastSeq))
    if (frame.tag === TAG.ACK_RES) this.serverHasThrough(frame.index, frame.ackedSeq)
    // ATTACH_RESULT and the connection-level and closing ctrls need connection bookkeeping; the rest is per-channel.
    switch (frame.tag) {
      case TAG.FIN:
        this.handleUpgradeFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
      case TAG.ATTACH_RESULT:
        this.handleAttachResult(
          frame.index,
          frame.lastSeq === null ? null : this.sentThrough(frame.index, frame.lastSeq),
        )
        return
      case TAG.ABORT:
        this.closeRemoteChannel(frame.index, makeAbortError(parse(frame.abortValue)))
        this.startTtlIfIdle()
        return
      case TAG.ERROR:
        this.closeRemoteChannel(frame.index, channelErrorFor(frame.reason))
        this.startTtlIfIdle()
        return
    }
    // PING/PONG/PIECE/PIECES_ACK/ANNOUNCE/RECONCILE/STREAM_REQUEST_OPEN_ACK never reach `dispatchFrame` —
    // transports peel them off in their own receive paths. Everything that lands
    // here is per-channel and carries `index`.
    const channelFrame = frame as ChannelFrame
    const entry = this.channels.get(channelFrame.index)
    // A closed channel stays listed only for what it still has to tell the server.
    if (entry && entry.state.tag !== 'closed') entry.channel._dispatchFrame(channelFrame)
  }

  /** Every frame arriving mid-handoff lands here — the one charge site. Before the flip the probe
   *  is nobody's wire, so even a join limb waits: acting on one would settle a swap that has not
   *  happened. After it, limbs apply and the rest waits, partitioned by the wire it came from. */
  private ingestDuringHandoff(frame: DecodedFrame, source: 'old' | 'new', byteLength: number): void {
    const u = this.committing
    if (u === null) return
    const flipped = this.transport === u.to
    if (flipped && this.applyJoinLimb(frame)) return
    ;(flipped ? u.buffer[source] : u.heldBeforeFlip).push({ frame, byteLength })
    u.bufferedFrames += 1
    u.bufferedBytes += byteLength
    // Checked after the push, so the frame that trips the budget is still in the prefix the
    // fallback delivers. Pre-flip nothing is committed yet, so the attempt can simply be dropped.
    if (u.bufferedFrames <= UPGRADE_HANDOFF_BUFFER_FRAMES && u.bufferedBytes <= UPGRADE_HANDOFF_BUFFER_BYTES) return
    if (flipped) this.fallbackToSse(new NetworkError('Upgrade handoff buffer limit exceeded', true))
    else u.attempt.abort()
  }

  /** True if the frame was one of the join's limbs, and has been applied. */
  private applyJoinLimb(frame: DecodedFrame): boolean {
    if (frame.tag === TAG.FIN) {
      this.handleUpgradeFin()
      return true
    }
    if (frame.tag === TAG.RECONCILED) {
      this.handleReconciled(frame.payload)
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
    this.fallbackToSse(new NetworkError(`Upgrade handoff timed out waiting for ${waitingFor}`, true))
  }

  /** An SSE wire's RECONCILE went out with the request that opens it, before the wire opens (see `beatFromReconcile`). */
  _onTransportReconcileSent(transport: ClientChannelTransport): void {
    if (this.closed || transport !== this.transport) return
    this.beatFromReconcile(transport)
  }

  /** From the RECONCILE on: a RECONCILED comes behind what the server sends as it attaches, however long that takes. */
  private beatFromReconcile(transport: ClientChannelTransport): void {
    if (this.sessionId !== null) this.installHeartbeat(transport, this.pingIntervalMs, false)
    else this.installHeartbeat(transport, CHANNEL_PING_INTERVAL_MIN_MS, true)
  }

  /** Idempotent once a RECONCILED set the interval; the next install replaces a provisional one. */
  private installHeartbeat(transport: ClientChannelTransport, intervalMs: number, provisional: boolean): void {
    if (transport.heartbeat !== null && !this.provisionalHeartbeat && this.pingIntervalMs === intervalMs) return
    transport.detachHeartbeat()
    if (!provisional) this.pingIntervalMs = intervalMs
    this.provisionalHeartbeat = provisional
    const hb = new Heartbeat(
      intervalMs,
      intervalMs * 2,
      () => this.beat(transport),
      () => this.handlePongTimeout(transport),
    )
    transport.attachHeartbeat(hb)
    hb.start(!provisional)
  }

  /** Each open channel acknowledges what arrived since its last WINDOW, then the PING goes. */
  private beat(transport: ClientChannelTransport): void {
    for (const { channel, state } of this.channels.values()) if (state.tag !== 'closed') channel._acknowledge()
    transport.sendPing(this.buildPing())
  }

  /** Names each closed channel the server attached; one a PING named, with nothing to replay or send, is let go. */
  private buildPing(): Uint8Array<ArrayBuffer> {
    const ended: PingEntry[] = []
    let queued: Set<number> | undefined
    for (const [ix, entry] of this.channels) {
      const state = entry.state
      if (state.tag !== 'closed' || state.initial) continue
      if (state.reported && this.replayBuffers.get(ix)!.length === 0) {
        queued ??= this.queuedIxes()
        if (!queued.has(ix)) {
          this.releaseChannel(ix, entry.channel)
          continue
        }
      }
      state.reported = true
      ended.push({ ix, lastSeq: this.lastSeqByChannel.get(ix) ?? 0 })
    }
    return encode.ping(ended)
  }

  /** How far the server has what the page sent on each channel named, or that it no longer holds it. */
  _onTransportPong(ended: PongEntry[]): void {
    for (const { ix, lastSeq } of ended) {
      const entry = this.channels.get(ix)
      if (entry === undefined || entry.state.tag !== 'closed') continue
      if (lastSeq === null) this.releaseChannel(ix, entry.channel)
      else this.serverHas(ix, entry.state, this.sentThrough(ix, lastSeq))
    }
  }

  /** The wire carries a seq's low 32 bits: the server's acknowledgement is the latest seq with them the page sent. */
  private sentThrough(ix: number, lastSeq: number): number {
    return seqThrough(lastSeq, this.replayBuffers.get(ix)?.seq ?? 0)
  }

  /** A closed channel the server attached has what the page sent on it through `lastSeq`. */
  private serverHas(ix: number, state: ClosedState, lastSeq: number): void {
    state.initial = false
    this.serverHasThrough(ix, lastSeq)
    if (lastSeq >= this.replayBuffers.get(ix)!.seq) state.delivered = true
  }

  /** The server has what the page sent on the channel through `lastSeq`, which its replay lets go. */
  private serverHasThrough(ix: number, lastSeq: number): void {
    this.replayBuffers.get(ix)?.acknowledge(lastSeq)
  }

  private dropWire(transport: ClientChannelTransport): void {
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    this._onTransportClosed(transport)
  }

  /** A batch POST has been out longer than its upload takes at the slowest rate a wire is held to. */
  _onTransportPostLate(transport: ClientChannelTransport): void {
    this.handlePongTimeout(transport)
  }

  /** Till its first RECONCILED a wire's RECONCILE deadline bounds it: what precedes may outlast the pong deadline. */
  private handlePongTimeout(transport: ClientChannelTransport): void {
    if (this.reconciling && this.reconciledWire !== this.wire) return
    this.dropWire(transport)
  }

  /** Enter the await-RECONCILED window and arm its liveness bound. A follow-up RECONCILE for
   *  late registrations re-enters and restarts the deadline from scratch. */
  private enterReconciling(): void {
    this.reconciling = true
    this.armReconcileDeadline(RECONCILE_TIMEOUT_MS)
  }

  private armReconcileDeadline(ms: number): void {
    if (this.reconcileTimer) clearTimeout(this.reconcileTimer)
    this.reconcileTimer = setTimeout(() => this.onReconcileTimeout(), ms)
  }

  /** Leave the await-RECONCILED window: RECONCILED settled, the wire was lost, or disposed. */
  private exitReconciling(): void {
    this.reconciling = false
    if (this.reconcileTimer) {
      clearTimeout(this.reconcileTimer)
      this.reconcileTimer = null
    }
    for (const resolve of this.reconcileSettledWaiters.splice(0)) resolve()
  }

  private reconcileSettled(signal: AbortSignal): Promise<void> {
    if (!this.reconciling) return Promise.resolve()
    return settledOrAborted(new Promise<void>((resolve) => this.reconcileSettledWaiters.push(resolve)), signal)
  }

  /** RECONCILED never arrived on a silently-stalled wire — same outcome as a missed pong:
   *  drop the wire and let `handleTransportLoss` reconnect. */
  private onReconcileTimeout(): void {
    if (this.closed || !this.reconciling) return
    // A RECONCILE waits behind what the page sent before: the wire is dead once it delivered nothing for the deadline.
    const quiet = this.transport.heartbeat?.quietFor() ?? Infinity
    if (quiet < RECONCILE_TIMEOUT_MS) return this.armReconcileDeadline(RECONCILE_TIMEOUT_MS - quiet)
    this.dropWire(this.transport)
  }

  private handleUpgradeFin(): void {
    const u = this.committing
    if (u === null) return
    u.finReceived = true
    this.tryCompleteUpgrade()
  }

  private tryCompleteUpgrade(): void {
    const u = this.committing
    if (u === null || this.transport !== u.to) return
    if (!u.finReceived || !u.committed) return
    const { from, buffer, deferredOmitted } = this.exitUpgradeCommitting()
    this.retireOldWire(from)
    for (const entry of buffer.old) this.dispatchFrame(entry.frame)
    this.closeUnacknowledged(deferredOmitted)
    for (const entry of buffer.new) this.dispatchFrame(entry.frame)
    this.flushPendingRegisterReconcile()
    this.startTtlIfIdle()
  }

  /** Tears down whichever upgrade phase is in flight. Returns the join's record when the flip had
   *  already happened — its buffered frames are then the caller's to drain — and null otherwise. */
  private teardownUpgrade(): CommittingUpgrade | null {
    if (this.state.tag !== 'open' || this.state.upgrade.tag === 'none') return null
    const u = this.state.upgrade
    if (u.tag === 'committing' && this.transport === u.to) {
      const record = this.exitUpgradeCommitting()
      this.retireOldWire(record.from)
      return record
    }
    u.attempt.abort()
    this.exitUpgradeAttempt(u.attempt)
    return null
  }

  private retireOldWire(transport: ClientChannelTransport): void {
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    transport.dispose()
  }

  _onTransportClosed(transport: ClientChannelTransport, { rejectedByServer = false } = {}): void {
    if (this.closed) return
    transport.detachHeartbeat()
    if (transport !== this.transport) {
      if (this.flipped && transport === this.committing!.from) {
        this.fallbackToSse(new NetworkError('Connection dropped', true))
      }
      return
    }
    if (this.state.tag === 'open' && this.state.upgrade.tag !== 'none' && !this.flipped) {
      this.state.upgrade.attempt.abort()
    }
    const err = new NetworkError(
      rejectedByServer
        ? `Server rejected ${this.transport.type === CHANNEL_TRANSPORT.SSE ? 'SSE' : 'WebSocket'} connection`
        : 'Connection dropped',
      true,
    )
    this.handleTransportLoss(err, rejectedByServer)
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    const committing = this.committing
    if (committing !== null && !committing.committed) {
      // A RECONCILED that does not echo this attempt's id belongs to someone else's upgrade.
      if (ctrl.upgradeId !== committing.upgradeId) return
      committing.committed = true
    }
    this.transport.applyReconciledSettings(ctrl)
    this.reconciledWire = this.wire
    const deferredOmitted = committing?.deferredOmitted ?? null
    const outcome = this.applyReconciled(ctrl, deferredOmitted)
    this.installHeartbeat(this.transport, ctrl.pingInterval, false)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) {
      channel._onTransportOpen(this.transport.batched, this.transport.uplink, this.transport.downlink)
    }
    this.tryCompleteUpgrade()
    if (outcome.reconcileComplete) {
      this.serverTransports = ctrl.transports
      this.startTtlIfIdle()
      this.flushPendingRegisterReconcile()
      this.maybeStartUpgrade()
    }
  }

  // ── SSE→WS upgrade ──

  private maybeStartUpgrade(): void {
    if (this.upgradeDisabled) return
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'none') return
    // Settled-reconcile gate; a flush above may have just re-armed `reconciling` — the next
    // RECONCILED retries via `handleReconciled`.
    if (this.reconciling) return
    const nextTransport = UPGRADE_PATH[this.transport.type]
    if (!nextTransport) return
    if (!this.isTransportUpgradeAllowed(nextTransport)) return
    if (!this.serverTransports?.includes(nextTransport)) return
    // Only SSE can carry a barrier as its last frame, and only SSE has anywhere to upgrade to.
    if (this.transport.type !== CHANNEL_TRANSPORT.SSE) return
    void this.probeAndUpgrade(this.transport as UpgradeSource, nextTransport)
  }

  private fallbackToSse(err: Error): void {
    if (this.closed) return
    const abandoned = this.teardownUpgrade()
    if (abandoned) {
      for (const entry of abandoned.buffer.old) this.dispatchFrame(entry.frame)
      this.closeUnacknowledged(abandoned.deferredOmitted)
    }
    this.upgradeDisabled = true
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(err, false)
  }

  private isTransportUpgradeAllowed(nextTransport: ChannelTransport): boolean {
    return this.connectionOptions.transports.includes(nextTransport)
  }

  private async probeAndUpgrade(from: UpgradeSource, targetTransport: UpgradeTargetTransport): Promise<void> {
    this.flushPendingRegisterReconcile()
    const sessionId = this.sessionId
    if (sessionId === null) return
    const attempt = new AbortController()
    this.enterUpgradeStaging(attempt)
    try {
      const to = UPGRADE_TARGET_REGISTRY[targetTransport](this.telefuncUrl, this)
      const session = await this.stageProbe(to, sessionId, attempt)
      if (!session) return
      await this.commitBarrier(from, to, session, attempt)
    } finally {
      this.exitUpgradeAttempt(attempt)
      this.flushPendingRegisterReconcile()
    }
  }

  private async stageProbe(
    to: UpgradeTarget,
    sessionId: string,
    attempt: AbortController,
  ): Promise<ProbeSession | null> {
    const probe = await to.probe()
    if (attempt.signal.aborted || !probe) {
      probe?.close()
      return null
    }
    const probeHeartbeat = this.keepProbeAlive(probe, attempt)

    const upgradeId = randomUuid()
    let onReady: ((payload: ReadyPayload) => void) | null = null
    probe.onFrame((frame, byteLength) => {
      if (frame.tag === TAG.READY) {
        onReady?.(frame.payload)
        return
      }
      this.ingestDuringHandoff(frame, 'new', byteLength)
    })

    const readyP = new Promise<ReadyPayload | null>((resolve) => {
      onReady = resolve
      attempt.signal.addEventListener('abort', () => resolve(null), { once: true })
    })
    this.armAttemptDeadline(attempt)
    probe.send(encode.prepare({ upgradeId, sessionId }))
    const ready = await readyP
    if (!ready || ready.upgradeId !== upgradeId || attempt.signal.aborted) {
      attempt.abort()
      if (this.registerReconcileTimer === null) this.drainBufferedFramesToWire()
      return null
    }
    this.enterUpgradeReady(attempt)
    return { upgradeId, sessionId, probeHeartbeat, uplink: probe.uplink }
  }

  /** The probe is nobody's transport yet, so it has no heartbeat of its own: this one keeps it
   *  alive until the flip adopts it, and any death — its own or the attempt's — closes both. */
  private keepProbeAlive(probe: ProbeWire, attempt: AbortController): Heartbeat {
    const heartbeat = new Heartbeat(
      this.pingIntervalMs,
      this.pingIntervalMs * 2,
      () => probe.ping(),
      () => attempt.abort(),
    )
    probe.onPong(() => heartbeat.resetPong())
    probe.onClose(() => attempt.abort())
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
    await this.reconcileSettled(attempt.signal)
    if (attempt.signal.aborted) return
    this.enterUpgradeCommitting(from, to, session, attempt)
    const emission = await from.emitBarrier(() => this.buildBarrierFrame(session), attempt.signal)

    if (emission === 'not-emitted') {
      attempt.abort()
      if (this.registerReconcileTimer === null) this.drainBufferedFramesToWire()
      // An attempt that ended without its barrier goes again once the outbox drained.
      void from.drained().then(() => this.maybeStartUpgrade())
      return
    }
    if (attempt.signal.aborted) {
      this.fallbackToSse(new NetworkError('Upgrade barrier attempt timed out', true))
      return
    }
    this.flip()
  }

  private flip(): void {
    const u = this.committing
    assert(u !== null)
    if (u.deadline) {
      clearTimeout(u.deadline)
      u.deadline = null
    }
    u.probeHeartbeat.stop()
    this.transport = u.to
    this.wire++
    u.to.adoptProbe()
    u.joinTimer = setTimeout(() => this.onJoinTimeout(), UPGRADE_HANDOFF_JOIN_TIMEOUT_MS)
    // What the probe delivered before the swap can be acted on now. The buffer is made whole
    // first, so the COMMITTED that may be among them cannot complete a join over a partial one.
    const held = u.heldBeforeFlip.splice(0)
    for (const { frame, byteLength } of held) if (!isJoinLimb(frame)) u.buffer.new.push({ frame, byteLength })
    for (const { frame } of held) if (isJoinLimb(frame)) this.applyJoinLimb(frame)
    this.tryCompleteUpgrade()
  }

  private drainBufferedFramesToWire(): void {
    for (const frame of this.drainBufferedFrames(this.isSendable)) this.transport.sendFrame(frame)
    this.startTtlIfIdle()
  }

  private handleTransportLoss(err: Error, rejected: boolean): void {
    if (this.closed) return
    if (this.flipped) {
      this.fallbackToSse(err)
      return
    }
    this.wire++
    // The wire is dying: the queued RECONCILE isn't sent, the reconnect's lists every channel.
    this.cancelRegisterReconcileTimer()
    this.forgetWire()
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }

    const { attempt: prevAttempt, startedAt: prevStartedAt } =
      this.state.tag === 'reconnecting' ? this.state : { attempt: 0, startedAt: 0 }

    if (rejected && prevAttempt === 0) {
      this.closeAll(err instanceof Error ? err : new NetworkError('Connection dropped', true))
      this.dispose()
      return
    }
    // A closed channel the server has all of needs nothing from another wire.
    for (const [ix, entry] of this.channels) {
      if (entry.state.tag === 'closed' && entry.state.delivered) this.releaseChannel(ix, entry.channel)
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    const startedAt = prevStartedAt || Date.now()
    if (Date.now() - startedAt > this.reconnectTimeoutMs) {
      this.closeAll(err instanceof Error ? err : new NetworkError('Connection dropped', true))
      this.dispose()
      return
    }
    const delay = Math.min(CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** prevAttempt, CHANNEL_RECONNECT_MAX_DELAY_MS)
    this.enterReconnecting(prevAttempt + 1, startedAt, delay)
  }

  /** On a live wire with no channel open and nothing queued: what the transport holds leaves before the wire does. */
  private startTtlIfIdle(): void {
    if (
      !this.connected ||
      this.ttl ||
      this.settleQueue().length > 0 ||
      this.heldForRoom.length > 0 ||
      this.channelIndex.size > 0
    ) {
      return
    }
    const ttl = setTimeout(() => {
      void this.transport.drained().then(() => {
        if (this.ttl === ttl && this.channelIndex.size === 0) this.dispose()
      })
    }, this.idleTimeoutMs)
    this.ttl = ttl
  }

  /** The server stops awaiting channels as their wire goes, and the next wire's RECONCILE lists them again. */
  private forgetWire(): void {
    this.awaitedIxes.clear()
    this.inFlight = reconcileInFlight()
    this.exitReconciling()
  }

  private dispose(): void {
    if (this.closed) return
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    if (this.registerReconcileTimer !== null) {
      clearTimeout(this.registerReconcileTimer)
      this.registerReconcileTimer = null
    }
    // Tear down any in-flight phase before transitioning to `closed`.
    if (this.state.tag === 'reconnecting') clearTimeout(this.state.timer)
    this.teardownUpgrade()
    this.enterClosed()
    this.transport.detachHeartbeat()
    this.transport.dispose()
    for (const replayBuffer of this.replayBuffers.values()) replayBuffer.dispose()
    this.channels.clear()
    this.channelIndex.clear()
    this.sendBuffer = []
    this.heldForRoom = new Fifo()
    this.heldForRoomBytes = 0
    this.droppedQueues.clear()
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    this.forgetWire()
    // A fresh connection replaces this one once its indexes run out.
    if (ClientConnection.cache.get(this.cacheKey) === this) ClientConnection.cache.delete(this.cacheKey)
  }

  // ── Protocol internals ──

  buildReconcileFrame(): OutboundFrame {
    const open = this.declareOpenEntries({
      skipUnnamed: false,
      uplink: this.transport.uplink,
      batched: this.transport.batched,
    })
    const reconcile: ReconcilePayload = { open }
    if (this.sessionId) reconcile.sessionId = this.sessionId
    return { kind: 'reconcile', frame: encode.reconcile(reconcile) }
  }

  /** The old wire's last frame; a channel no RECONCILE named yet is left out, to reconcile after the handoff. */
  private buildBarrierFrame({ sessionId, upgradeId, uplink }: ProbeSession): OutboundFrame {
    // Its entries attach on the WebSocket, `uplink`'s wire, the one after this one.
    const open = this.declareOpenEntries({ skipUnnamed: true, uplink, batched: false })
    return { kind: 'reconcile', frame: encode.barrier({ sessionId, upgradeId, open }) }
  }

  private declareOpenEntries({
    skipUnnamed,
    uplink,
    batched,
  }: {
    skipUnnamed: boolean
    uplink: Uplink
    batched: boolean
  }): ReconcileOpenEntry[] {
    this.enterReconciling()
    this.inFlight = reconcileInFlight()
    const open: ReconcileOpenEntry[] = []
    const subscribing = new Set<number>()
    for (const [ix, entry] of this.channels) {
      // The server has all a delivered one sent it, or ended it: leaving it out lets it go there too.
      if (entry.state.tag === 'closed' && entry.state.delivered) {
        this.releaseChannel(ix, entry.channel)
        continue
      }
      const isInitial = entry.state.tag !== 'open' && entry.state.initial
      if (skipUnnamed && isInitial && !this.awaitedIxes.has(ix)) continue
      this.inFlight.ixes.set(ix, isInitial)
      const payloadEntry: ReconcileOpenEntry = {
        id: entry.channel.id,
        ix,
        lastSeq: this.lastSeqByChannel.get(ix) ?? 0,
      }
      if (isInitial) payloadEntry.initial = true
      const state = entry.channel._reattachState(batched, uplink)
      if (state.probe !== undefined) this.inFlight.probes.set(ix, state.probe)
      Object.assign(payloadEntry, state)
      if (state.broadcast) subscribing.add(ix)
      open.push(payloadEntry)
    }
    // The declared subscriptions supersede the SUB/UNSUB frames queued before them.
    if (subscribing.size > 0) {
      this.sendBuffer = this.settleQueue().filter(
        ({ channelIx, frame }) =>
          !subscribing.has(channelIx) || (frame[0] !== TAG.BROADCAST_SUB && frame[0] !== TAG.BROADCAST_UNSUB),
      )
    }
    return open
  }

  drainBufferedFramesForReconcile(isInitialBatch: boolean): OutboundFrame[] {
    if (this.transport.reconcileMode !== 'batch-on-reconcile') return []
    // After a wire that may have lost frames, the replay goes first: a newer frame ahead gets an older one dropped.
    if (isInitialBatch && (this.sessionId !== null || this.sentBefore())) return []
    // A channel the server awaits is left out: its replay waits for its ATTACH_RESULT.
    for (const { channelIx, seq } of this.settleQueue()) {
      if (seq !== undefined && this.isSendable(channelIx) && !this.inFlight.carriedFrom.has(channelIx)) {
        this.inFlight.carriedFrom.set(channelIx, seq)
      }
    }
    return this.drainBufferedFrames(this.isSendable)
  }

  /** Whether a replay holds a frame the page sent. */
  private sentBefore(): boolean {
    for (const replay of this.replayBuffers.values()) if (replay.length > 0) return true
    return false
  }

  private readonly isSendable = (ix: number): boolean => !this.awaitedIxes.has(ix)

  stageReconcileBatch(isInitialBatch: boolean): ReconcileBatch {
    // This batch includes every channel, so it already covers any pending registration.
    this.cancelRegisterReconcileTimer()
    const reconcileFrame = this.buildReconcileFrame()
    const movedBufferedFrames = this.drainBufferedFramesForReconcile(isInitialBatch)
    return { reconcileFrame, movedBufferedFrames }
  }

  private sendReconcileBatch(reconcileBatch: ReconcileBatch): void {
    this.transport.sendFrame(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) this.transport.sendFrame(frame)
  }

  private appendReconcileBatch(target: OutboundFrame[], reconcileBatch: ReconcileBatch): void {
    target.push(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) target.push(frame)
  }

  private applyReconciled(ctrl: ReconciledPayload, deferredOmitted: number[] | null): ReconcileOutcome {
    this.sessionId = ctrl.sessionId
    this.takeServerSettings(ctrl)
    const serverMap = new Map<number, number>()
    for (const channel of ctrl.open) serverMap.set(channel.ix, channel.lastSeq)
    const { ixes, probes, attachResults, carriedFrom } = this.inFlight
    this.inFlight = reconcileInFlight()
    for (const [ix, probe] of probes) this.channels.get(ix)?.channel._onProbeAnswered(probe)
    for (const [ix, lastSeq] of attachResults) if (lastSeq !== null && !serverMap.has(ix)) serverMap.set(ix, lastSeq)
    // An initial channel left out is one the server awaits, until its ATTACH_RESULT.
    for (const [ix, initial] of ixes) {
      this.awaitedIxes.delete(ix)
      if (!initial || serverMap.has(ix) || attachResults.has(ix) || !this.channels.has(ix)) continue
      this.awaitedIxes.add(ix)
    }
    const releaseFrames: OutboundFrame[] = []
    const channelsToOpen: MuxChannel[] = []
    let hasNewChannels = false

    for (const [ix, entry] of this.channels) {
      if (!ixes.has(ix)) {
        // Registered after the RECONCILE we just got back was built — wait for next round.
        if (!serverMap.has(ix)) hasNewChannels = true
        continue
      }
      if (this.awaitedIxes.has(ix)) continue
      const replay = this.reattach(ix, entry, serverMap.get(ix), (carriedFrom.get(ix) ?? Infinity) - 1, deferredOmitted)
      if (replay === null) continue
      for (const frame of replay) releaseFrames.push(frame)
      if (entry.state.tag !== 'closed') channelsToOpen.push(entry.channel)
    }

    for (const frame of this.drainBufferedFrames((ix) => serverMap.has(ix))) releaseFrames.push(frame)

    if (hasNewChannels && !this.upgradeReady) {
      const reconcileBatch = this.stageReconcileBatch(false)
      this.appendReconcileBatch(releaseFrames, reconcileBatch)
    } else {
      this.exitReconciling()
      // The barrier leaves out what the follow-up would name.
      if (hasNewChannels) this.scheduleRegisterReconcile()
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
  }

  /** The timeouts, replay sizes and frame limit the server sets, each replay fitted to them. */
  private takeServerSettings(ctrl: ReconciledPayload): void {
    this.reconnectTimeoutMs = ctrl.reconnectTimeout
    // A runtime's lower limit is its WebSocket's.
    if (this.connectionOptions.transports.includes(CHANNEL_TRANSPORT.WS)) this.maxFrameBytes = ctrl.maxFrameBytes
    // A per-call `idleTimeout` (withContext) is kept over the server's.
    if (this.connectionOptions.idleTimeout === undefined) this.idleTimeoutMs = ctrl.idleTimeout
    this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    this.clientReplayBufferBinaryBytes = ctrl.clientReplayBufferBinary
    this.replayWindows = {
      window: replayWindow(ctrl.serverReplayBuffer, ctrl.serverReplayBufferBinary),
      peerWindow: replayWindow(ctrl.clientReplayBuffer, ctrl.clientReplayBufferBinary),
    }
    // Before this reconcile stores anything: a channel registered before the first RECONCILED was sized with the defaults.
    for (const replay of this.replayBuffers.values()) {
      replay.setLimits(this.clientReplayBufferBytes, this.clientReplayBufferBinaryBytes)
    }
    for (const { channel } of this.channels.values()) this.fitReplays(channel)
  }

  /** What a channel the RECONCILE named replays to the server, its state brought to the server's; null if let go. */
  private reattach(
    ix: number,
    entry: ChannelEntry,
    lastSeq: number | undefined,
    throughSeq: number,
    deferredOmitted: number[] | null,
  ): OutboundFrame[] | null {
    if (entry.state.tag === 'closed') {
      // One the server no longer has needs nothing more from the page.
      if (lastSeq === undefined) {
        this.releaseChannel(ix, entry.channel)
        return null
      }
      this.serverHas(ix, entry.state, lastSeq)
    }
    if (lastSeq === undefined) {
      if (deferredOmitted) deferredOmitted.push(ix)
      else this.closeUnacknowledged([ix])
      return null
    }
    if (entry.state.tag === 'pending') this.enterChannelOpen(ix)
    return this.replayTo(ix, entry, lastSeq, throughSeq)
  }

  private closeUnacknowledged(ixes: number[]): void {
    for (const ix of ixes) {
      const entry = this.channels.get(ix)
      if (!entry) continue
      const err = new NetworkError('Channel not acknowledged by server after reconnect', true)
      this.releaseChannel(ix, entry.channel)
      entry.channel._onTransportClose(err)
    }
  }

  /** An awaited channel's own RECONCILED entry: `lastSeq` if the server attached it, null if not. */
  private handleAttachResult(ix: number, lastSeq: number | null): void {
    if (this.inFlight.ixes.has(ix)) {
      this.inFlight.attachResults.set(ix, lastSeq)
      return
    }
    if (!this.awaitedIxes.delete(ix)) return
    const entry = this.channels.get(ix)!
    if (lastSeq === null) {
      this.closeUnacknowledged([ix])
    } else {
      const opened = entry.state.tag === 'pending'
      if (opened) this.enterChannelOpen(ix)
      else if (entry.state.tag === 'closed') this.serverHas(ix, entry.state, lastSeq)
      // As `applyReconciled` does: its replay, then what it queued.
      for (const frame of this.replayTo(ix, entry, lastSeq)) this.transport.sendFrame(frame)
      for (const frame of this.drainBufferedFrames((i) => i === ix)) this.transport.sendFrame(frame)
      if (opened && entry.state.tag === 'open') {
        entry.channel._onTransportOpen(this.transport.batched, this.transport.uplink, this.transport.downlink)
      }
    }
    this.startTtlIfIdle()
    this.maybeStartUpgrade()
  }

  /** What the server lacks through `throughSeq`: the replay, or an ERROR ending the channel if the replay lost some. */
  private replayTo(ix: number, entry: ChannelEntry, lastSeq: number, throughSeq = Infinity): OutboundFrame[] {
    this.serverHasThrough(ix, lastSeq)
    const missed = this.replayBuffers.get(ix)!.getAfter(lastSeq, throughSeq)
    if (missed !== null) return missed.map((frame) => ({ kind: 'released', frame }))
    // One the server ended needs nothing more.
    if (entry.state.tag === 'closed' && entry.state.delivered) return []
    return [this.loseChannel(ix, entry)]
  }

  /** Ends the channel on both ends, dropping its queue; the ERROR isn't replayed, as the next reconcile sends one. */
  private loseChannel(ix: number, entry: ChannelEntry): OutboundFrame {
    this.droppedQueues.add(ix)
    const frame = encode.error(ix, ERROR_REASON.LOST, this.replayBuffers.get(ix)!.nextSeq())
    entry.channel._onTransportClose(replayLossError('client'))
    return { kind: 'control', frame }
  }

  /** The server ended it, so it needs nothing more the page sent on it. */
  private closeRemoteChannel(ix: number, err?: Error): void {
    const entry = this.channels.get(ix)
    if (!entry) return
    this.unregister(entry.channel)
    assert(entry.state.tag === 'closed')
    entry.state.delivered = true
    entry.channel._onTransportClose(err)
  }

  private closeAll(err: Error): void {
    for (const [, entry] of this.channels) {
      entry.channel._onTransportClose(err)
    }
    this.dispose()
  }

  /** The seq of a frame of channel `ix` with low bits `bits`, or null to drop a duplicate or one of a let-go one. */
  private trackSeq(ix: number, bits: number): number | null {
    if (!this.channels.has(ix)) return null
    const prev = this.lastSeqByChannel.get(ix) ?? 0
    const seq = seqNear(bits, prev + 1)
    if (seq <= prev) return null
    this.lastSeqByChannel.set(ix, seq)
    return seq
  }

  /** Takes out what `releasable` channels queued; past the room, their sequenced frames go behind `heldForRoom`'s. */
  private drainBufferedFrames(releasable: (ix: number) => boolean): OutboundFrame[] {
    const frames: OutboundFrame[] = []
    const sendBuffer = this.settleQueue()
    let room = this.transport.sendRoom()
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const frame = entry.frame
      const channelIx = entry.channelIx
      const seq = entry.seq
      if (!releasable(channelIx)) {
        sendBuffer[writeIx++] = entry
        continue
      }
      if (seq !== undefined) {
        if (room <= 0 || this.heldForRoom.length > 0) {
          this.holdForRoom({ frame, channelIx, seq })
          continue
        }
        room -= frame.byteLength
        this.replayBuffers.get(channelIx)?.push(seq, frame)
      }
      frames.push({ kind: 'released', frame })
    }
    sendBuffer.length = writeIx
    return frames
  }

  /** Drops all the channel holds, its ATTACH_RESULT wait too, as a RECONCILE leaving it out ends that on the server. */
  private releaseChannel(ix: number, channel: MuxChannel): void {
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.awaitedIxes.delete(ix)
    this.lastSeqByChannel.delete(ix)
    const replayBuffer = this.replayBuffers.get(ix)
    replayBuffer?.dispose()
    this.replayBuffers.delete(ix)
    if (this.sendBuffer.length > 0 || this.heldForRoom.length > 0) this.droppedQueues.add(ix)
  }

  /** sendBuffer, with what waited for room in front of it once sends are held: a reconcile takes them all in order. */
  private settleQueue(): BufferedFrame[] {
    this.dropQueues()
    if (!this.sendsOpen && this.heldForRoom.length > 0) {
      this.sendBuffer = [...this.heldForRoom, ...this.sendBuffer]
      this.heldForRoom = new Fifo()
      this.heldForRoomBytes = 0
    }
    return this.sendBuffer
  }

  /** The channels whose frames wait to go. */
  private queuedIxes(): Set<number> {
    const ixes = new Set<number>()
    for (const { channelIx } of this.settleQueue()) ixes.add(channelIx)
    for (const { channelIx } of this.heldForRoom) ixes.add(channelIx)
    return ixes
  }

  private dropQueues(): void {
    if (this.droppedQueues.size === 0) return
    const dropped = this.droppedQueues
    this.sendBuffer = this.sendBuffer.filter(({ channelIx }) => !dropped.has(channelIx))
    if (this.heldForRoom.length > 0) {
      const kept = new Fifo<SequencedFrame>()
      for (const entry of this.heldForRoom) {
        if (dropped.has(entry.channelIx)) this.heldForRoomBytes -= entry.frame.byteLength
        else kept.push(entry)
      }
      this.heldForRoom = kept
    }
    this.droppedQueues = new Set()
  }
}

// ── Transport registry ──

/** Maps each ChannelTransport to a factory that creates the corresponding ClientChannelTransport. */
const TRANSPORT_REGISTRY: Record<
  ChannelTransport,
  (telefuncUrl: string, options: ClientConnectionOptions, owner: ClientConnection) => ClientChannelTransport
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, _options, owner) => new WsTransport(telefuncUrl, owner),
  [CHANNEL_TRANSPORT.SSE]: (telefuncUrl, options, owner) =>
    new SseTransport(telefuncUrl, options.fetchImpl, options.headers, owner),
}

/** Defines which transport can upgrade to which. */
type UpgradeTargetTransport = typeof CHANNEL_TRANSPORT.WS

const UPGRADE_PATH: Partial<Record<ChannelTransport, UpgradeTargetTransport>> = {
  [CHANNEL_TRANSPORT.SSE]: CHANNEL_TRANSPORT.WS,
}

const UPGRADE_TARGET_REGISTRY: Record<
  UpgradeTargetTransport,
  (telefuncUrl: string, owner: ClientConnection) => UpgradeTarget
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, owner) => new WsTransport(telefuncUrl, owner),
}

/** What a channel the server ended with an ERROR of `reason` closes with. */
function channelErrorFor(reason: number): Error {
  switch (reason) {
    case ERROR_REASON.OVERFLOW:
      return new ChannelOverflowError(
        'Broadcast closed: this client fell further behind than the server holds for a client',
      )
    case ERROR_REASON.LOST:
      return replayLossError('server')
    default:
      return makeBugError()
  }
}
