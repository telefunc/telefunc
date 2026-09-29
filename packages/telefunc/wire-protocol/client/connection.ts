export { ClientConnection }
export type { MuxChannel, MuxConnection }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { ChannelOverflowError, replayLossError } from '../channel-errors.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { base64urlToUint8Array } from '../base64url.js'
import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
  CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_RECONCILE_DEADLINE_MS,
  STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS,
  MAX_CHANNELS_PER_CONNECTION,
  UPGRADE_HANDOFF_BUFFER_BYTES,
  UPGRADE_HANDOFF_BUFFER_FRAMES,
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS,
  UPGRADE_ATTEMPT_TIMEOUT_MS,
  UPGRADE_DRAIN_TIMEOUT_MS,
  WS_PROBE_TIMEOUT_MS,
  type ChannelTransport,
  type ChannelTransports,
  TIMER_DELAY_MAX_MS,
} from '../constants.js'
import { encodeU32, encodeLengthPrefixedFrames } from '../frame.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { replayWindow } from '../flow-control/flow-control.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../request-kind.js'
import {
  ACK_STATUS,
  ERROR_REASON,
  TAG,
  decode,
  encode,
  isReplayLoss,
  isSequencedFrame,
  payloadBytes,
} from '../shared-ws.js'
import type {
  AckResultStatus,
  ChannelFrame,
  DecodedFrame,
  PingEntry,
  PongEntry,
  ReadyPayload,
  ReattachState,
  ReconcileOpenEntry,
  ReconcilePayload,
  ReconciledPayload,
  ReplayLoss,
  SeqReader,
} from '../shared-ws.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../sse-request.js'
import { DeadlineScheduler } from './deadlineScheduler.js'
import { randomUuid } from '../../utils/randomUuid.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}

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
}

type ProbeSession = {
  upgradeId: string
  probeHeartbeat: Heartbeat
}

/** Ping-then-pong-deadline loop. Each transport owns one for its wire; the upgrade probe
 *  flow constructs a transient instance for the probed wire until the swap commits. */
class Heartbeat {
  private pingTimer: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  /** When the wire last delivered a frame, or the pong deadline was last set. */
  private lastReceivedAt = 0

  constructor(
    private readonly intervalMs: number,
    private readonly pongTimeoutMs: number,
    private readonly send: () => void,
    private readonly onDead: () => void,
  ) {}

  start(): void {
    if (this.pingTimer) return
    this.send()
    this.resetPong()
    this.pingTimer = setInterval(this.send, this.intervalMs)
  }

  resetPong(): void {
    this.lastReceivedAt = performance.now()
    this.armPongDeadline(this.pongTimeoutMs)
  }

  noteReceived(): void {
    this.lastReceivedAt = performance.now()
  }

  private armPongDeadline(ms: number): void {
    if (this.pongTimer) clearTimeout(this.pongTimer)
    this.pongTimer = setTimeout(this.onPongDeadline, ms)
  }

  // A PONG arrives behind what the server queued before it, as much as a window on a slow link: the wire is dead once it
  // has delivered nothing for the deadline.
  private readonly onPongDeadline = (): void => {
    const quiet = performance.now() - this.lastReceivedAt
    if (quiet < this.pongTimeoutMs) this.armPongDeadline(this.pongTimeoutMs - quiet)
    else this.onDead()
  }

  stop(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
    if (this.pongTimer) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
  }
}

/** Settles on either, discards both outcomes — a caller that needs to know re-reads the signal.
 *  Taking the rejection matters: on an abort-first race nothing else would handle it. */
function settledOrAborted(promise: Promise<unknown>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise<void>((resolve) => {
    const settle = (): void => {
      signal.removeEventListener('abort', settle)
      resolve()
    }
    signal.addEventListener('abort', settle, { once: true })
    promise.then(settle, settle)
  })
}

/** The verdict IS the recovery decision, so the three cases must stay distinct: `emitted` may have
 *  reached the server (trust no wire, reconcile afresh, upgrades sticky-disabled), `not-emitted`
 *  leaves the wire usable, `wedged` wrote nothing but a never-settling POST owns the flush gate. */
type BarrierEmission = 'emitted' | 'not-emitted' | 'wedged'

type OutboundFrameKind = 'reconcile' | 'control' | 'flow-control' | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
}

interface MuxChannel {
  readonly id: string
  readonly isClosed: boolean
  /** `wire` numbers the connection's wire: the number of the channel's last attach means that same wire, which lost
   *  nothing. */
  _onTransportOpen(batched: boolean, wire: number): void
  /** Entry point for every per-channel wire frame (data + per-channel ctrl). The
   *  channel splits ctrl vs data internally. Connection-level frames (PING/PONG/
   *  FIN/RECONCILED), channel-termination ctrls (ABORT/ERROR) and ATTACH_RESULT stay with the
   *  connection — they involve connection-side cleanup. */
  _dispatchFrame(frame: ChannelFrame): void
  _onTransportClose(err?: Error): void
  /** What this channel declares in its RECONCILE entry on every (re)attach, on `wire`, whose flow-control frames wait
   *  for a batched POST when `batched`. */
  _reattachState?(wire: number, batched: boolean): ReattachState
  /** The largest windows the replay buffers allow: the one the page grants, and the one the server grants it. */
  _fitReplays?(window: number, peerWindow: number): void
  /** At each heartbeat: a WINDOW for what arrived since the last, so the server's replay lets it go while the channel is
   *  quiet. */
  _acknowledge?(): void
}

interface MuxConnection {
  send(channel: MuxChannel, data: string): number
  sendPublishAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void
  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void
  sendTextAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void
  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void
  sendBinary(channel: MuxChannel, data: Uint8Array): void
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendByteWindowUpdate(channel: MuxChannel, limit: number): void
  sendMsgWindowUpdate(channel: MuxChannel, limit: number): void
  sendBdpPing(channel: MuxChannel, probe: number): void
  sendBdpPingAck(channel: MuxChannel, probe: number, starved: boolean): void
  /** Bytes the wire holds that haven't gone out. */
  bufferedAmount(): number
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
  start(): void
  hasWire(): boolean
  isConnecting(): boolean
  /** Send a connection-level ping on this wire. Heartbeat's send callback calls this. */
  sendPing(frame: Uint8Array<ArrayBuffer>): void
  sendFrame(frame: OutboundFrame): void
  /** Bytes of the frames it was handed that haven't gone out to the network. */
  bufferedAmount(): number
  abandonActiveTransport(): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: ReconciledPayload): void
  /** Connection constructs the Heartbeat (with the funnel-bound onDead) and hands it over.
   *  Transport's frame receive path routes PONG to it directly (`heartbeat?.resetPong()`). */
  attachHeartbeat(hb: Heartbeat): void
  detachHeartbeat(): void
  hasHeartbeat(): boolean
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

type OutboxEntry = { frame: Uint8Array<ArrayBuffer>; deadline: number }

type SseInitialBatchStage = {
  initialFrames: OutboundFrame[]
  movedOutbox: OutboxEntry[]
  movedBufferedFrames: OutboundFrame[]
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

/** Partitioned by SOURCE WIRE, not one arrival-ordered list, and `old` drains FIRST: the server wrote all of it before
 *  anything the new wire carries, which repeats only what of it has a seq. */
type UpgradeBuffer = { old: BufferedWireFrame[]; new: BufferedWireFrame[] }

/** FIN (old wire) and RECONCILED (new wire) are the join's two limbs; everything else is payload. */
function isJoinLimb(frame: DecodedFrame): boolean {
  return frame.tag === TAG.FIN || frame.tag === TAG.RECONCILED
}

/** Per-channel lifecycle. `closed` = unregistered, its closing frame sent or queued. It stays listed, since a RECONCILE
 *  leaving it out ends it on the server, and what it sent replays, until the server no longer holds it, it is
 *  `delivered` as a RECONCILE is built or its wire is lost, or a PING finds nothing of it left to replay or send. Once
 *  the server attached it, each PING names it with how far the page has what the server sent on it. `initial`: the
 *  server may not have registered it, until a RECONCILED or ATTACH_RESULT attaches it. `delivered`: the server has all
 *  the page sent on it, or ended it and needs none of it. `reported`: a PING named it. */
type ChannelState =
  | { tag: 'pending'; initial: boolean }
  | { tag: 'open' }
  | { tag: 'closed'; initial: boolean; delivered: boolean; reported: boolean }

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
  private nextIndex = 0
  /** What the RECONCILE in flight lists, and whether as `initial`. */
  private reconcileIxes = new Map<number, boolean>()
  /** Initial channels a RECONCILED left out because the server hadn't registered them, until their ATTACH_RESULT, which
   *  comes on the wire awaiting them, the old one or, after a barrier, the WebSocket. What they queue waits for it, so
   *  their replay goes first. */
  private awaitedIxes = new Set<number>()
  /** The attach results of channels the RECONCILE in flight lists, applied with its RECONCILED, which the server may have
   *  built before them (an SSE batch POST's RECONCILED goes once the POST's body is read). */
  private earlyAttachResults = new Map<number, number | null>()
  /** Per channel, the first seq the RECONCILE in flight carries behind it on its wire. Its RECONCILED replays only what
   *  comes before: the wire delivers the rest after the RECONCILE, or is gone, and the next one's RECONCILE replays it. */
  private carriedFrom = new Map<number, number>()
  private channels = new Map<number, ChannelEntry>()
  private channelIndex = new Map<MuxChannel, number>()
  private sendBuffer: BufferedFrame[] = []
  private lastSeqByChannel = new Map<number, number>()
  private replayBuffers = new Map<number, ReplayBuffer>()
  /** Where the page stands on each channel, from which a frame's seqs are read. */
  readonly seqs: SeqReader = {
    received: (ix) => this.lastSeqByChannel.get(ix) ?? 0,
    sent: (ix) => this.replayBuffers.get(ix)?.seq ?? 0,
  }
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs: number
  private pingIntervalMs = CHANNEL_PING_INTERVAL_MS
  private clientReplayBufferBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BYTES
  private clientReplayBufferBinaryBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES
  private serverReplayBufferBytes = CHANNEL_SERVER_REPLAY_BUFFER_BYTES
  private serverReplayBufferBinaryBytes = CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES
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

  /** READY arrived, so the barrier comes next. It is a RECONCILE too, one at a time, and lists what the server has on
   *  the old wire, so no other RECONCILE goes out until the handoff. */
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

  private canSendImmediately(ix: number): boolean {
    return (
      this.connected &&
      !this.reconciling &&
      !this.upgradeGatesSends &&
      this.registerReconcileTimer === null &&
      !this.awaitedIxes.has(ix)
    )
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
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    assertUsage(
      this.openChannelCount() < MAX_CHANNELS_PER_CONNECTION,
      `Too many channels on one connection (${MAX_CHANNELS_PER_CONNECTION} max) — open another with \`connectionKey\``,
    )
    // Every RECONCILE lists every channel, the closed ones too, so the oldest closed one makes way.
    if (this.channels.size === MAX_CHANNELS_PER_CONNECTION) {
      const [ix, entry] = [...this.channels].find(([, entry]) => entry.state.tag === 'closed')!
      this.releaseChannel(ix, entry.channel)
    }
    const ix = this.nextIndex++
    this.enterChannelPending(ix, channel, true)
    this.replayBuffers.set(
      ix,
      new ReplayBuffer(
        this.clientReplayBufferBytes,
        this.replayMaxAgeMs(this.pingIntervalMs),
        this.clientReplayBufferBinaryBytes,
      ),
    )
    this.fitReplays(channel)

    if (!this.transport.hasWire() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    this.scheduleRegisterReconcile()
  }

  /** Its window fits the server's replay, and the server's fits the page's: as the first RECONCILED says, the defaults
   *  until then. */
  private fitReplays(channel: MuxChannel): void {
    channel._fitReplays?.(
      replayWindow(this.serverReplayBufferBytes, this.serverReplayBufferBinaryBytes),
      replayWindow(this.clientReplayBufferBytes, this.clientReplayBufferBinaryBytes),
    )
  }

  /** How long a gone server is still held: until its loss is noticed at the pong deadline, then for `reconnectTimeout`. */
  reconnectWindow(pingIntervalMs = this.pingIntervalMs): number {
    return Math.min(TIMER_DELAY_MAX_MS, 2 * pingIntervalMs + this.reconnectTimeoutMs)
  }

  /** As the server's, a frame stays replayable through the reconnect window, plus a second for the reconnect itself. */
  private replayMaxAgeMs(pingIntervalMs: number): number {
    return Math.min(TIMER_DELAY_MAX_MS, this.reconnectWindow(pingIntervalMs) + 1_000)
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
    this.sendReconcileBatch(this.stageReconcileBatch())
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
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return 0
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.text(ix, data, seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
    } else {
      replay.push(seq, frame)
      this.transport.sendFrame({ kind: 'data', frame })
    }
    return payloadBytes(frame)
  }

  sendPublishAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.publishAckReq(ix, data, seq), onQueued)
  }

  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.publishBinaryAckReq(ix, data, seq), onQueued)
  }

  sendTextAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.textAckReq(ix, data, seq), onQueued)
  }

  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.binaryAckReq(ix, data, seq), onQueued)
  }

  /** Shared ack-req issuance — encodes via `buildFrame`, invokes `onQueued(seq)` so the
   *  channel registers the pending ack *before* the frame hits the wire (so an
   *  immediate `ACK_RES` can't be lost), then ships or buffers the frame. Mirrors
   *  `IndexedPeer.sendTextAckReq` on the server side. */
  private sendAckReq(
    channel: MuxChannel,
    buildFrame: (ix: number, seq: number) => Uint8Array<ArrayBuffer>,
    onQueued: (seq: number) => void,
  ): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = buildFrame(ix, seq)
    onQueued(seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
  }

  sendBinary(channel: MuxChannel, data: Uint8Array): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.binary(ix, data, seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'data', frame })
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
  }

  sendAbort(channel: MuxChannel): void {
    this.sendClosingFrame(channel, (ix, seq) => encode.close(ix, 0, seq))
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
    const frame = buildFrame(ix, seq)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendByteWindowUpdate(channel: MuxChannel, limit: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.sendFlowControl(ix, encode.window(ix, limit, this.lastSeqByChannel.get(ix) ?? 0))
  }

  sendMsgWindowUpdate(channel: MuxChannel, limit: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.sendFlowControl(ix, encode.msgWindow(ix, limit))
  }

  sendBdpPing(channel: MuxChannel, probe: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.sendFlowControl(ix, encode.bdpPing(ix, probe))
  }

  sendBdpPingAck(channel: MuxChannel, probe: number, starved: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.sendFlowControl(ix, encode.bdpPingAck(ix, probe, starved))
  }

  bufferedAmount(): number {
    return this.transport.bufferedAmount()
  }

  /** Held with the rest while sends are held. A limit is cumulative, so one that waited is still right, where a
   *  dropped one could stall the peer: an upgrade attempt that ends without its barrier lifts the hold with no reattach
   *  to advertise it again. */
  private sendFlowControl(ix: number, frame: Uint8Array<ArrayBuffer>): void {
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'flow-control', frame })
  }

  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.broadcastSub(ix, binary)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.broadcastUnsub(ix, binary)
    if (!this.canSendImmediately(ix)) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  _onTransportOpen(transport: ClientChannelTransport): void {
    if (this.closed) return
    if (transport !== this.transport) return
    this.enterOpen()
    if (this.transport.sendReconcileOnOpen) {
      this.sendReconcileBatch(this.stageReconcileBatch())
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

  _onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number): void {
    const u = this.committing
    if (u !== null && this.transport === u.to) {
      this.ingestDuringHandoff(frame, source === u.from ? 'old' : 'new', byteLength)
    } else {
      this.dispatchFrame(frame)
    }
  }

  private dispatchFrame(frame: DecodedFrame): void {
    // Track seq for ALL sequenced frames, ACK_RES and the closing ones too; otherwise reconciles under-report lastSeq.
    if (isSequencedFrame(frame) && this.trackSeq(frame.index, frame.seq) === 'dup') return
    if (frame.tag === TAG.WINDOW) this.serverHasThrough(frame.index, frame.lastSeq)
    // Connection-level + channel-termination ctrls and ATTACH_RESULT stay here; they involve connection
    // bookkeeping (upgrade state, channel release, TTL). Everything else is per-channel and goes through
    // `channel._dispatchFrame`.
    switch (frame.tag) {
      case TAG.FIN:
        this.handleUpgradeFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
      case TAG.ATTACH_RESULT:
        this.handleAttachResult(frame.index, frame.lastSeq)
        return
      case TAG.ABORT:
        this.closeRemoteChannel(frame.index, makeAbortError(parse(frame.abortValue)))
        this.startTtlIfIdle()
        return
      case TAG.ERROR:
        this.closeRemoteChannel(
          frame.index,
          frame.reason === ERROR_REASON.OVERFLOW
            ? new ChannelOverflowError(
                'Broadcast closed: this client fell further behind than the server holds for a client',
              )
            : isReplayLoss(frame.reason)
              ? replayLossError('server', frame.reason)
              : makeBugError(),
        )
        this.startTtlIfIdle()
        return
    }
    // PING/PONG/RECONCILE/STREAM_REQUEST_OPEN_ACK never reach `dispatchFrame` —
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
    const waitingFor = u.finReceived ? 'RECONCILED' : 'FIN'
    this.fallbackToSse(new NetworkError(`Upgrade handoff timed out waiting for ${waitingFor}`, true))
  }

  /** Idempotent. Detaches first either way so a fresh install can never leak the prior. */
  private installHeartbeat(transport: ClientChannelTransport, intervalMs: number): void {
    if (transport.hasHeartbeat() && this.pingIntervalMs === intervalMs) return
    transport.detachHeartbeat()
    this.pingIntervalMs = intervalMs
    const hb = new Heartbeat(
      intervalMs,
      intervalMs * 2,
      () => this.beat(transport),
      () => this.handlePongTimeout(transport),
    )
    transport.attachHeartbeat(hb)
    hb.start()
  }

  /** Each open channel acknowledges what arrived since its last WINDOW, then the PING goes. */
  private beat(transport: ClientChannelTransport): void {
    for (const { channel, state } of this.channels.values()) if (state.tag !== 'closed') channel._acknowledge?.()
    transport.sendPing(this.buildPing())
  }

  /** Names each closed channel the server attached, with how far the page has what the server sent on it, which the
   *  server answers with how far it has what the page sent on it, or that it no longer holds it. One a PING named
   *  before, with nothing left to replay or to send, is let go instead: nothing it holds can reach the server now. */
  private buildPing(): Uint8Array<ArrayBuffer> {
    const ended: PingEntry[] = []
    for (const [ix, entry] of this.channels) {
      const state = entry.state
      if (state.tag !== 'closed' || state.initial) continue
      if (
        state.reported &&
        this.replayBuffers.get(ix)!.length === 0 &&
        !this.sendBuffer.some(({ channelIx }) => channelIx === ix)
      ) {
        this.releaseChannel(ix, entry.channel)
        continue
      }
      state.reported = true
      ended.push({ ix, lastSeq: this.lastSeqByChannel.get(ix) ?? 0 })
    }
    return encode.ping(ended)
  }

  /** The server's answer to a PING: how far it has what the page sent on each channel named, or that it no longer holds
   *  the channel. */
  _onTransportPong(ended: PongEntry[]): void {
    for (const { ix, lastSeq } of ended) {
      const entry = this.channels.get(ix)
      if (entry === undefined || entry.state.tag !== 'closed') continue
      if (lastSeq === null) this.releaseChannel(ix, entry.channel)
      else this.serverHas(ix, entry.state, lastSeq)
    }
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

  /** Funnel for pong-timeouts. Suppress while reconciling — pings are delayed by the round-trip;
   *  `reconcileTimer` (armed by `enterReconciling`) is the liveness bound for that window. */
  private handlePongTimeout(transport: ClientChannelTransport): void {
    if (this.reconciling) return
    this.dropWire(transport)
  }

  /** Enter the await-RECONCILED window and arm its liveness bound. A follow-up RECONCILE for
   *  late registrations re-enters and restarts the deadline from scratch. */
  private enterReconciling(): void {
    this.reconciling = true
    if (this.reconcileTimer) clearTimeout(this.reconcileTimer)
    this.reconcileTimer = setTimeout(() => this.onReconcileTimeout(), RECONCILE_TIMEOUT_MS)
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
    this.releaseDeferredOmitted(deferredOmitted)
    for (const entry of buffer.new) this.dispatchFrame(entry.frame)
    this.pruneSendBufferForReleasedChannels()
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

  private pruneSendBufferForReleasedChannels(): void {
    const sendBuffer = this.sendBuffer
    if (sendBuffer.length === 0) return
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      if (this.channels.has(entry.channelIx)) sendBuffer[writeIx++] = entry
    }
    sendBuffer.length = writeIx
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
    const deferredOmitted = committing?.deferredOmitted ?? null
    const outcome = this.applyReconciled(ctrl, deferredOmitted)
    this.installHeartbeat(this.transport, ctrl.pingInterval)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) channel._onTransportOpen(this.transport.batched, this.wire)
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
      this.releaseDeferredOmitted(abandoned.deferredOmitted)
    }
    this.upgradeDisabled = true
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(err)
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
      await this.commitBarrier(from, to, session, sessionId, attempt)
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
    return { upgradeId, probeHeartbeat }
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
    heartbeat.start()
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
    sessionId: string,
    attempt: AbortController,
  ): Promise<void> {
    // After the RECONCILED of the one in flight, which may name a channel the barrier lists.
    await this.reconcileSettled(attempt.signal)
    if (attempt.signal.aborted) return
    this.enterUpgradeCommitting(from, to, session, attempt)
    const emission = await from.emitBarrier(() => this.buildBarrierFrame(sessionId, session.upgradeId), attempt.signal)

    if (emission === 'wedged') {
      attempt.abort()
      this.recoverWedgedOldWire(new NetworkError('Upgrade aborted with the old wire stalled', true))
      return
    }
    if (emission === 'not-emitted') {
      attempt.abort()
      if (this.registerReconcileTimer === null) this.drainBufferedFramesToWire()
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

  private recoverWedgedOldWire(err: Error): void {
    const wedged = this.transport
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    wedged.abandonActiveTransport()
    wedged.dispose()
    this.handleTransportLoss(err)
  }

  private drainBufferedFramesToWire(): void {
    for (const frame of this.drainBufferedFrames(this.sendableChannels(), this.awaitedIxes))
      this.transport.sendFrame(frame)
    this.startTtlIfIdle()
  }

  private handleTransportLoss(err: Error, rejected = false): void {
    if (this.closed) return
    if (this.flipped) {
      this.fallbackToSse(err)
      return
    }
    this.wire++
    // The server stops awaiting channels when their wire goes; the next wire's RECONCILE lists them again.
    this.awaitedIxes.clear()
    this.earlyAttachResults.clear()
    // The wire is dying: the queued RECONCILE isn't sent, the reconnect's lists every channel.
    this.cancelRegisterReconcileTimer()
    this.exitReconciling()
    this.reconcileIxes.clear()
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
    for (const [ix, entry] of this.channels)
      if (entry.state.tag === 'closed' && entry.state.delivered) this.releaseChannel(ix, entry.channel)
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

  /** On a live wire once no channel is open and nothing is queued: a closed one's frames went out, and what the
   *  transport holds leaves before the wire does. */
  private startTtlIfIdle(): void {
    if (!this.connected || this.ttl || this.sendBuffer.length > 0 || this.openChannelCount() > 0) return
    const ttl = setTimeout(() => {
      void this.transport.drained().then(() => {
        if (this.ttl === ttl && this.openChannelCount() === 0) this.dispose()
      })
    }, this.idleTimeoutMs)
    this.ttl = ttl
  }

  private openChannelCount(): number {
    let count = 0
    for (const entry of this.channels.values()) if (entry.state.tag !== 'closed') count++
    return count
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
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    this.reconcileIxes.clear()
    this.awaitedIxes.clear()
    this.earlyAttachResults.clear()
    this.exitReconciling()
    // A fresh connection replaces this one once its indexes run out.
    if (ClientConnection.cache.get(this.cacheKey) === this) ClientConnection.cache.delete(this.cacheKey)
  }

  // ── Protocol internals ──

  buildReconcileFrame(): OutboundFrame {
    const open = this.declareOpenEntries({ skipUnnamed: false, wire: this.wire, batched: this.transport.batched })
    const reconcile: ReconcilePayload = { open, ...(this.sessionId ? { sessionId: this.sessionId } : {}) }
    return { kind: 'reconcile', frame: encode.reconcile(reconcile) }
  }

  /** The old wire's last frame. A channel the server awaits is listed, and its await moves to the new wire. One no
   *  RECONCILE has named yet is left out: the server has no record of it, so it reconciles after the handoff. */
  private buildBarrierFrame(sessionId: string, upgradeId: string): OutboundFrame {
    // Its entries attach on the WebSocket, the wire after this one.
    const open = this.declareOpenEntries({ skipUnnamed: true, wire: this.wire + 1, batched: false })
    return { kind: 'reconcile', frame: encode.barrier({ sessionId, upgradeId, open }) }
  }

  private declareOpenEntries({
    skipUnnamed,
    wire,
    batched,
  }: {
    skipUnnamed: boolean
    wire: number
    batched: boolean
  }): ReconcileOpenEntry[] {
    this.enterReconciling()
    this.reconcileIxes = new Map()
    this.carriedFrom = new Map()
    const open: ReconcileOpenEntry[] = []
    for (const [ix, entry] of this.channels) {
      // The server has all a delivered one sent it, or ended it: leaving it out lets it go there too.
      if (entry.state.tag === 'closed' && entry.state.delivered) {
        this.releaseChannel(ix, entry.channel)
        continue
      }
      const isInitial = entry.state.tag !== 'open' && entry.state.initial
      if (skipUnnamed && isInitial && !this.awaitedIxes.has(ix)) continue
      this.reconcileIxes.set(ix, isInitial)
      const payloadEntry: ReconcileOpenEntry = {
        id: entry.channel.id,
        ix,
        lastSeq: this.lastSeqByChannel.get(ix) ?? 0,
      }
      if (isInitial) payloadEntry.initial = true
      const state = entry.channel._reattachState?.(wire, batched)
      Object.assign(payloadEntry, state)
      // The declared subscriptions supersede the SUB/UNSUB frames queued before them.
      if (state?.broadcast)
        this.sendBuffer = this.sendBuffer.filter(
          ({ channelIx, frame }) =>
            channelIx !== ix || (frame[0] !== TAG.BROADCAST_SUB && frame[0] !== TAG.BROADCAST_UNSUB),
        )
      open.push(payloadEntry)
    }
    return open
  }

  drainBufferedFramesForReconcile(isInitialBatch: boolean): OutboundFrame[] {
    if (this.transport.reconcileMode !== 'batch-on-reconcile') return []
    // The hazard is confined to a *reconnect's* initial batch: the previous wire may have died
    // with an unacked frame still only in the replay buffer, which is re-sent after RECONCILED
    // (`applyReconciled`'s `getAfter`). Eager-batching a newer frame into that initial batch
    // would reach the server first, advance its `lastClientSeq` past the older one, and get the
    // older one dup-dropped on replay — silent message loss. Defer to the post-RECONCILED
    // release there (same ordering discipline as the WS 'release-after-reconciled' mode).
    // A connect retried before its first RECONCILED is a reconnect too: its earlier batch's frames are only in the
    // replay buffers. Every other reconcile is on a live wire: a first attempt has sent nothing yet, and a
    // reconcile on an established wire (new-channel registration, chained reconcile) has no
    // in-transit replay frame to jump ahead of — so eager-batch, it saves a round-trip.
    // A channel the server awaits is left out: its replay waits for its ATTACH_RESULT.
    const sentBefore = [...this.replayBuffers.values()].some((replay) => replay.length > 0)
    if (isInitialBatch && (this.sessionId !== null || sentBefore)) return []
    const sendable = this.sendableChannels()
    for (const { channelIx, seq } of this.sendBuffer)
      if (seq !== undefined && sendable.has(channelIx) && !this.carriedFrom.has(channelIx))
        this.carriedFrom.set(channelIx, seq)
    return this.drainBufferedFrames(sendable, this.awaitedIxes)
  }

  private sendableChannels(): Set<number> | Map<number, ChannelEntry> {
    if (this.awaitedIxes.size === 0) return this.channels
    return new Set([...this.channels.keys()].filter((ix) => !this.awaitedIxes.has(ix)))
  }

  stageReconcileBatch(isInitialBatch = false): ReconcileBatch {
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
    this.reconnectTimeoutMs = ctrl.reconnectTimeout
    // A per-call `idleTimeout` (withContext) is kept over the server's.
    if (this.connectionOptions.idleTimeout === undefined) this.idleTimeoutMs = ctrl.idleTimeout
    this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    this.clientReplayBufferBinaryBytes = ctrl.clientReplayBufferBinary
    this.serverReplayBufferBytes = ctrl.serverReplayBuffer
    this.serverReplayBufferBinaryBytes = ctrl.serverReplayBufferBinary
    // Before this reconcile stores anything: a channel registered before the first RECONCILED was sized with the defaults.
    const maxAgeMs = this.replayMaxAgeMs(ctrl.pingInterval)
    for (const replay of this.replayBuffers.values()) {
      replay.setLimits(this.clientReplayBufferBytes, maxAgeMs, this.clientReplayBufferBinaryBytes)
    }
    for (const { channel } of this.channels.values()) this.fitReplays(channel)

    const serverMap = new Map<number, number>()
    for (const channel of ctrl.open) serverMap.set(channel.ix, channel.lastSeq)
    const reconcileIxes = this.reconcileIxes
    this.reconcileIxes = new Map()
    const attachResults = this.earlyAttachResults
    this.earlyAttachResults = new Map()
    const carriedFrom = this.carriedFrom
    this.carriedFrom = new Map()
    for (const [ix, lastSeq] of attachResults) if (lastSeq !== null && !serverMap.has(ix)) serverMap.set(ix, lastSeq)
    // An initial channel left out is one the server awaits, until its ATTACH_RESULT. What one released meanwhile
    // queued goes out now: the server holds it for the attach.
    const releasable = new Set(serverMap.keys())
    for (const [ix, initial] of reconcileIxes) {
      this.awaitedIxes.delete(ix)
      if (!initial || serverMap.has(ix) || attachResults.has(ix)) continue
      this.awaitedIxes.add(ix)
      if (!this.channels.has(ix)) releasable.add(ix)
    }
    const releaseFrames: OutboundFrame[] = []
    const channelsToOpen: MuxChannel[] = []
    let hasNewChannels = false

    for (const [ix, entry] of this.channels) {
      if (!reconcileIxes.has(ix)) {
        // Registered after the RECONCILE we just got back was built — wait for next round.
        if (!serverMap.has(ix)) hasNewChannels = true
        continue
      }
      if (this.awaitedIxes.has(ix)) continue
      if (entry.state.tag === 'closed') {
        const lastSeq = serverMap.get(ix)
        // One the server no longer has needs nothing more from the page.
        if (lastSeq === undefined) {
          this.releaseChannel(ix, entry.channel)
          continue
        }
        this.serverHas(ix, entry.state, lastSeq)
      }
      if (!serverMap.has(ix)) {
        if (deferredOmitted) {
          deferredOmitted.push(ix)
          continue
        }
        const err = new NetworkError('Channel not acknowledged by server after reconnect', true)
        this.releaseChannel(ix, entry.channel)
        entry.channel._onTransportClose(err)
        continue
      }
      if (entry.state.tag === 'pending') this.enterChannelOpen(ix)
      for (const frame of this.replayTo(ix, entry, serverMap.get(ix)!, (carriedFrom.get(ix) ?? Infinity) - 1))
        releaseFrames.push(frame)
      if (entry.state.tag !== 'closed') channelsToOpen.push(entry.channel)
    }

    for (const frame of this.drainBufferedFrames(releasable, this.channels)) releaseFrames.push(frame)

    if (hasNewChannels && !this.upgradeReady) {
      const reconcileBatch = this.stageReconcileBatch()
      this.appendReconcileBatch(releaseFrames, reconcileBatch)
    } else {
      this.exitReconciling()
      // The barrier leaves out what the follow-up would name.
      if (hasNewChannels) this.scheduleRegisterReconcile()
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
  }

  private releaseDeferredOmitted(ixes: number[]): void {
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
    if (this.reconcileIxes.has(ix)) {
      this.earlyAttachResults.set(ix, lastSeq)
      return
    }
    if (!this.awaitedIxes.delete(ix)) return
    const entry = this.channels.get(ix)
    // Nothing for one released meanwhile: the server held its closing frame for the attach, or closed it itself.
    if (lastSeq === null) {
      this.releaseDeferredOmitted([ix])
    } else if (entry) {
      const opened = entry.state.tag === 'pending'
      if (opened) this.enterChannelOpen(ix)
      else if (entry.state.tag === 'closed') this.serverHas(ix, entry.state, lastSeq)
      // As `applyReconciled` does: its replay, then what it queued.
      for (const frame of this.replayTo(ix, entry, lastSeq)) this.transport.sendFrame(frame)
      for (const frame of this.drainBufferedFrames(new Set([ix]), this.channels)) this.transport.sendFrame(frame)
      if (opened && entry.state.tag === 'open') entry.channel._onTransportOpen(this.transport.batched, this.wire)
    }
    this.startTtlIfIdle()
    this.maybeStartUpgrade()
  }

  /** What the server, which attached the channel with `lastSeq`, lacks of it through `throughSeq`: its replay, or, if
   *  the replay no longer holds all of it, the ERROR that ends the channel on both ends in its place. */
  private replayTo(ix: number, entry: ChannelEntry, lastSeq: number, throughSeq = Infinity): OutboundFrame[] {
    this.serverHasThrough(ix, lastSeq)
    const missed = this.replayBuffers.get(ix)!.getAfter(lastSeq, throughSeq)
    if (typeof missed !== 'number') return missed.map((frame) => ({ kind: 'reconcile', frame }))
    // One the server ended needs nothing more.
    if (entry.state.tag === 'closed' && entry.state.delivered) return []
    return [this.loseChannel(ix, entry, missed)]
  }

  /** Ends the channel on both ends. What it queued is dropped, as it would reach the server past the hole. The ERROR
   *  isn't kept in the replay: a later reconcile finds the same hole and sends another. */
  private loseChannel(ix: number, entry: ChannelEntry, loss: ReplayLoss): OutboundFrame {
    this.sendBuffer = this.sendBuffer.filter(({ channelIx }) => channelIx !== ix)
    const frame = encode.error(ix, loss, this.replayBuffers.get(ix)!.nextSeq())
    entry.channel._onTransportClose(replayLossError('client', loss))
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

  /** Dedup against double-delivery. Transports are TCP-ordered and replay sends a contiguous slice
   *  starting at our reported `lastSeq + 1`, so duplicates shouldn't occur in normal operation. One for a channel the
   *  page let go is dropped too. */
  private trackSeq(ix: number, seq: number): 'accept' | 'dup' {
    if (!this.channels.has(ix)) return 'dup'
    const prev = this.lastSeqByChannel.get(ix) ?? 0
    if (seq <= prev) return 'dup'
    this.lastSeqByChannel.set(ix, seq)
    return 'accept'
  }

  private drainBufferedFrames(
    releasableChannels: Set<number> | Map<number, unknown>,
    /** Frames for these channels stay in the buffer. Omitted: nothing is retained. */
    retainedChannels?: Set<number> | Map<number, unknown>,
  ): OutboundFrame[] {
    const frames: OutboundFrame[] = []
    const sendBuffer = this.sendBuffer
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const frame = entry.frame
      const channelIx = entry.channelIx
      const seq = entry.seq
      if (!releasableChannels.has(channelIx)) {
        if (retainedChannels?.has(channelIx)) sendBuffer[writeIx++] = entry
        continue
      }
      if (seq !== undefined) this.replayBuffers.get(channelIx)?.push(seq, frame)
      frames.push({ kind: 'reconcile', frame })
    }
    sendBuffer.length = writeIx
    return frames
  }

  private releaseChannel(ix: number, channel: MuxChannel): void {
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.lastSeqByChannel.delete(ix)
    const replayBuffer = this.replayBuffers.get(ix)
    replayBuffer?.dispose()
    this.replayBuffers.delete(ix)
  }
}

class WsTransport implements UpgradeTarget {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly sendReconcileOnOpen = true
  readonly reconcileMode = 'release-after-reconciled' as const
  readonly batched = false
  private heartbeat: Heartbeat | null = null
  private probedWs: WebSocket | null = null
  private ws: WebSocket | null = null
  private abandonedWs: WebSocket | null = null
  private connecting = false
  private everOpened = false

  private readonly wsUrl: string

  constructor(
    telefuncUrl: string,
    private readonly owner: ClientConnection,
  ) {
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(telefuncUrl, base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    this.wsUrl = url.href
  }

  async probe(): Promise<ProbeWire | null> {
    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      return null
    }
    ws.binaryType = 'arraybuffer'

    let onPong: (() => void) | null = null
    let onClose: (() => void) | null = null
    let onFrame: ((frame: DecodedFrame, byteLength: number) => void) | null = null
    ws.onmessage = ({ data }: MessageEvent) => {
      const raw = new Uint8Array(data as ArrayBuffer)
      let frame: DecodedFrame
      try {
        frame = decode(raw, this.owner.seqs)
      } catch {
        ws.close()
        return
      }
      if (frame.tag === TAG.PONG) {
        onPong?.()
        return
      }
      onFrame?.(frame, raw.byteLength)
    }
    ws.onclose = () => {
      if (this.probedWs === ws) this.probedWs = null
      onClose?.()
    }
    ws.onerror = () => {}
    ws.onopen = () => ws.send(encode.ping())

    // First pong proves the wire is alive — consumer reassigns onPong/onClose after the await.
    const ready = await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), WS_PROBE_TIMEOUT_MS)
      onPong = () => {
        clearTimeout(timer)
        resolve(true)
      }
      onClose = () => {
        clearTimeout(timer)
        resolve(false)
      }
    })
    if (!ready) {
      try {
        ws.close()
      } catch {}
      return null
    }

    this.probedWs = ws
    return {
      ping: () => {
        try {
          ws.send(encode.ping())
        } catch {}
      },
      onPong: (cb) => {
        onPong = cb
      },
      onClose: (cb) => {
        onClose = cb
      },
      send: (frame) => {
        try {
          ws.send(frame)
        } catch {
          // Socket died between the open event and this send — `onClose` aborts the attempt.
        }
      },
      onFrame: (cb) => {
        onFrame = cb
      },
      close: () => {
        if (this.probedWs === ws) this.probedWs = null
        try {
          ws.close()
        } catch {}
      },
    }
  }

  adoptProbe(): void {
    const ws = this.probedWs
    assert(ws !== null)
    this.probedWs = null
    this.ws = ws
    this.everOpened = true
    this.connecting = false
    this.setupHandlers(ws)
  }

  start(): void {
    if (this.connecting || this.hasWire()) return

    this.connecting = true

    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      this.connecting = false
      this.owner._onTransportClosed(this)
      return
    }

    this.ws = ws
    ws.binaryType = 'arraybuffer'

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.handleOpen(ws)
    }

    this.setupHandlers(ws)
  }

  private handleOpen(ws: WebSocket): void {
    if (this.ws !== ws) return
    this.everOpened = true
    this.connecting = false
    this.owner._onTransportOpen(this)
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  hasHeartbeat(): boolean {
    return this.heartbeat !== null
  }

  drained(): Promise<void> {
    // A socket sends what it buffered before its close.
    return Promise.resolve()
  }

  private setupHandlers(ws: WebSocket): void {
    ws.onmessage = ({ data }: MessageEvent) => {
      const raw = new Uint8Array(data as ArrayBuffer)
      let frame: DecodedFrame
      try {
        frame = decode(raw, this.owner.seqs)
      } catch {
        ws.close()
        return
      }
      this.heartbeat?.noteReceived()
      if (frame.tag === TAG.PONG) {
        this.heartbeat?.resetPong()
        this.owner._onTransportPong(frame.ended)
        return
      }
      this.owner._onTransportFrame(frame, this, raw.byteLength)
    }
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.owner._onTransportClosed(this, { rejectedByServer: !this.everOpened })
    }
    ws.onerror = () => {}
  }

  hasWire(): boolean {
    return this.ws !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    const ws = this.ws
    assert(ws)
    ws.send(frame.frame)
  }

  bufferedAmount(): number {
    return this.ws?.bufferedAmount ?? 0
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    this.abandonedWs = ws
    ws.onopen = ws.onerror = ws.onclose = null
  }

  closeAbandonedTransport(): void {
    const ws = this.abandonedWs
    if (!ws) return
    this.abandonedWs = null
    ws.onmessage = ws.onclose = null
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(): void {}

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return
    this.ws.send(frame)
  }

  dispose(): void {
    this.connecting = false
    const wsProbed = this.probedWs
    this.probedWs = null
    if (wsProbed) {
      wsProbed.onopen = wsProbed.onmessage = wsProbed.onerror = wsProbed.onclose = null
      try {
        wsProbed.close(1000)
      } catch {}
    }
    const ws = this.ws
    this.ws = null
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
      try {
        ws.close(1000)
      } catch {}
    }
    this.closeAbandonedTransport()
  }
}

class SseTransport implements UpgradeSource {
  readonly type = CHANNEL_TRANSPORT.SSE
  readonly sendReconcileOnOpen = false
  readonly reconcileMode = 'batch-on-reconcile' as const
  /** Each wire's own, so a POST still in flight for a wire that died isn't dispatched on the next one. */
  private connId = randomUuid()
  get batched(): boolean {
    return this.streamRequest.tag !== 'active'
  }
  private heartbeat: Heartbeat | null = null
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
  private lastPostStartedAt = 0
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
    while (this.flushing) {
      if (!this.hasWire()) return 'not-emitted'
      if (signal.aborted) return 'wedged'
      await settledOrAborted(new Promise<void>((resolve) => this.drainCallbacks.push(resolve)), signal)
    }
    if (!this.hasWire() || signal.aborted) return 'not-emitted'
    this.flushScheduler.cancel()
    const queued = this.outbox.splice(0, this.outbox.length).map((entry) => entry.frame)
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
    if (this.flushing && frame.kind === 'heartbeat') {
      this.schedulePingDuringFlush(frame)
      return
    }
    if (this.streamRequest.tag === 'active') {
      this.streamRequest.body.push(encodeU32(frame.frame.byteLength))
      this.streamRequest.body.push(frame.frame)
      this.streamRequest.unconfirmed?.push(frame.frame)
      return
    }
    const now = Date.now()
    const deadline = this.getFrameDeadline(frame.kind, now)
    this.outbox.push({ frame: frame.frame, deadline })
    this.scheduleFlush()
    if (deadline <= now) void this.flushOutbox()
  }

  private async openStream(): Promise<void> {
    this.connId = randomUuid()
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
    // The duplex:'half' POST ends with its body, or earlier when something on the way cuts it (Node's `requestTimeout`).
    // `fetchEndedP` catches its rejection eagerly so it's always handled even if openStream exits early.
    let fetchEndedP: Promise<'fetch-ended'> | undefined
    let uploadBody: PushReadableStream<Uint8Array<ArrayBuffer>> | undefined
    if (this.streamRequest.tag !== 'failed') {
      const body = createPushReadableStream<Uint8Array<ArrayBuffer>>()
      uploadBody = body
      // Metadata header first — the server classifies the POST by it; `streamRequest: true`
      // makes it emit `reconciled` inline (the body never ends, can't defer to body-end).
      body.push(encodeSseRequestMetadata({ connId: this.connId, streamRequest: true }))
      const fetch = this.openStreamRequest(body, abortController.signal)
      this.streamRequest = { tag: 'active', body, unconfirmed: [] }
      fetchEndedP = (async (): Promise<'fetch-ended'> => {
        try {
          await fetch
        } catch {}
        return 'fetch-ended'
      })()
    }

    const failOpen = (rejectedByServer: boolean): void => {
      this.rollbackInitialBatch(stage)
      this.closeStreamRequest()
      abortController.abort()
      this.transportAbort = null
      this.connecting = false
      this.owner._onTransportClosed(this, { rejectedByServer })
    }

    let response: Response
    try {
      response = await ssePromise
    } catch {
      failOpen(false)
      return
    }

    if (!response.ok || !response.body) {
      failOpen(true)
      return
    }

    const reader = createSseEventStreamReader(
      response.body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
      abortController,
    )

    // Run the SSE loop concurrently with the handshake wait — frames (including the first
    // RECONCILED) can arrive while the open-ack is still in flight and must be dispatched.
    let resolveHandshakeOk!: () => void
    const handshakeOkP = new Promise<'ok'>((resolve) => {
      resolveHandshakeOk = () => resolve('ok')
    })
    ;(async () => {
      try {
        while (true) {
          const raw = await reader.readNextEntry()
          if (!raw) break
          if (raw[0] === TAG.STREAM_REQUEST_OPEN_ACK) {
            resolveHandshakeOk()
            continue
          }
          const frame = decode(raw, this.owner.seqs)
          this.heartbeat?.noteReceived()
          if (frame.tag === TAG.PONG) {
            this.heartbeat?.resetPong()
            this.owner._onTransportPong(frame.ended)
            continue
          }
          this.owner._onTransportFrame(frame, this, raw.byteLength)
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
    })()

    // Race upstream readiness: ack (ok), fetch ended (dead), or timeout. Non-ok → outbox+batch.
    if (fetchEndedP) {
      const timeoutP = new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS),
      )
      const result = await Promise.race([handshakeOkP, timeoutP, fetchEndedP])
      if (result === 'ok') {
        if (this.streamRequest.tag === 'active') this.streamRequest.unconfirmed = null
        // Ended while its body is still ours: nothing written to it reaches the server any more, so this wire ends.
        void fetchEndedP.then(() => {
          if (this.streamRequest.tag === 'active' && this.streamRequest.body === uploadBody) abortController.abort()
        })
      } else {
        // It ended without the open-ack, so the server may not have read what went into its body: resend it, first.
        const unsent =
          result === 'fetch-ended' && this.streamRequest.tag === 'active' ? this.streamRequest.unconfirmed : null
        this.closeStreamRequest()
        this.streamRequest = { tag: 'failed' }
        if (unsent) this.outbox = [...unsent.map((frame) => ({ frame, deadline: Date.now() })), ...this.outbox]
      }
    }

    this.connecting = false
    this.owner._onTransportOpen(this)
    if (this.outbox.length > 0) void this.flushOutbox()
  }

  private stageInitialBatch(): SseInitialBatchStage {
    const reconcileBatch = this.owner.stageReconcileBatch(true)
    const initialFrames: OutboundFrame[] = []
    initialFrames.push(reconcileBatch.reconcileFrame)
    const movedBufferedFrames = reconcileBatch.movedBufferedFrames
    // A dead wire's outbox carries only its window refreshes. This reconcile declares every channel and its
    // subscriptions, and sequenced frames, a close request among them, replay after RECONCILED from the server's
    // lastSeq: sent first, they could overtake older ones a POST still in flight carries, whose frames the server would
    // then drop as duplicates.
    const movedOutbox = this.outbox.filter(isWindowRefresh)
    this.outbox = []
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
  }

  private async flushOutbox(): Promise<void> {
    if (!this.hasWire() || this.flushing || this.outbox.length === 0) return
    assert(this.transportAbort)
    this.flushScheduler.cancel()
    this.flushing = true
    try {
      const now = Date.now()
      const queued = this.outbox.splice(0, this.outbox.length)
      this.lastPostStartedAt = now
      const wire = this.transportAbort

      try {
        const response = await this.post(
          encodeSseRequest(
            { connId: this.connId },
            encodeLengthPrefixedFrames(queued, (entry) => entry.frame),
          ),
          wire.signal,
        )
        if (!response.ok) throw new Error('POST failed')
      } catch {
        // Its wire has ended already: what the POST carried goes the way of that wire's outbox (stageInitialBatch),
        // also when the next wire has reconciled already.
        if (wire !== this.transportAbort) {
          this.outbox = queued.filter(isWindowRefresh).concat(this.outbox)
          return
        }
        this.outbox = queued.concat(this.outbox)
        this.abandonActiveTransport()
        this.owner._onTransportClosed(this)
        return
      }
    } finally {
      this.flushing = false
      if (this.outbox.length > 0) {
        this.scheduleFlush()
      } else {
        const cbs = this.drainCallbacks.splice(0)
        for (const cb of cbs) cb()
      }
    }
  }

  /** Concurrent ping POST while a flush POST is in flight. */
  private schedulePingDuringFlush(frame: OutboundFrame): void {
    const delay = Math.max(0, this.getFrameDeadline(frame.kind) - Date.now())
    setTimeout(() => {
      if (this.flushing) {
        void this.sendStandalonePost([frame.frame])
      } else {
        this.sendFrame(frame)
      }
    }, delay)
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

  /** For frames that cannot wait for the next flush. Failure is swallowed: its two senders are a
   *  heartbeat ping (its own deadline notices a dead wire) and the barrier (delivered either way). */
  private async sendStandalonePost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
    if (!this.hasWire()) return
    assert(this.transportAbort)
    try {
      await this.post(
        encodeSseRequest({ connId: this.connId }, encodeLengthPrefixedFrames(frames)),
        this.transportAbort.signal,
      )
    } catch {}
  }

  /** What a POST under way carries has gone out. */
  bufferedAmount(): number {
    if (this.streamRequest.tag === 'active') return this.streamRequest.body.bufferedAmount
    let bytes = 0
    for (const entry of this.outbox) bytes += entry.frame.byteLength
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
        return now + SSE_RECONCILE_DEADLINE_MS
      case 'control':
        return now
      case 'heartbeat':
        return now + this.heartbeatFlushDelayMs
      case 'flow-control':
      case 'ack':
      case 'data':
        return (
          now +
          (now - this.lastPostStartedAt >= this.flushThrottleMs ? this.postIdleFlushDelayMs : this.flushThrottleMs)
        )
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
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
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
    this.heartbeatFlushDelayMs = Math.floor(ctrl.pingInterval / 2)
  }

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    if (!this.hasWire()) return
    this.sendFrame({ kind: 'heartbeat', frame })
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  hasHeartbeat(): boolean {
    return this.heartbeat !== null
  }

  drained(): Promise<void> {
    if (!this.hasWire() || (!this.flushing && this.outbox.length === 0)) return Promise.resolve()
    return new Promise((resolve) => this.drainCallbacks.push(resolve))
  }

  dispose(): void {
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    this.flushScheduler.cancel()
    this.outbox = []
    this.closeStreamRequest()
    this.transportAbort?.abort()
    this.transportAbort = null
    this.closeAbandonedTransport()
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }

  // ── Persistent client→server stream-request POST (half-duplex streaming body) ──

  /** Half-duplex POST. Resolves on body-end, rejects on fetch error. */
  /** `PushReadableStream` IS-A `ReadableStream` — fetch reads it directly, the producer's
   *  `push(chunk)` lands in the same stream's queue, no async-iterator adapter in between. */
  private openStreamRequest(body: PushReadableStream<Uint8Array<ArrayBuffer>>, signal: AbortSignal): Promise<unknown> {
    return this.post(body, signal, { duplex: 'half' })
  }

  private closeStreamRequest(): void {
    // Only 'active' has a body to close; 'failed' is sticky-terminal so don't regress to 'idle'.
    if (this.streamRequest.tag !== 'active') return
    this.streamRequest.body.close()
    this.streamRequest = { tag: 'idle' }
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

function isWindowRefresh({ frame }: OutboxEntry): boolean {
  return frame[0] === TAG.WINDOW || frame[0] === TAG.MSG_WINDOW
}

function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
): {
  cancel: () => void
  readNextEntry: () => Promise<Uint8Array<ArrayBuffer> | null>
} {
  const decoder = new TextDecoder()
  // Cursor-based incremental parser. `lineBuf` accumulates decoded text; `cursor` is
  // the offset of the first unparsed byte. We walk it line-by-line via `indexOf('\n')`
  // and queue completed events as we go — no full-buffer splits, no re-joins. The
  // prefix gets trimmed amortised once the consumed region exceeds half the buffer.
  let lineBuf = ''
  let cursor = 0
  let pendingData = ''
  const ready: Array<Uint8Array<ArrayBuffer>> = []
  let cancelled = false

  const cancel = () => {
    if (cancelled) return
    cancelled = true
    reader.cancel().catch(() => {})
  }

  abortController.signal.addEventListener('abort', cancel, { once: true })

  const flushEvent = () => {
    if (pendingData !== '') {
      ready.push(base64urlToUint8Array(pendingData))
      pendingData = ''
    }
  }

  const processBufferedLines = () => {
    while (cursor < lineBuf.length) {
      const nl = lineBuf.indexOf('\n', cursor)
      if (nl === -1) break // incomplete tail line — wait for more bytes
      const line = lineBuf.slice(cursor, nl)
      cursor = nl + 1
      if (line.length === 0) {
        flushEvent()
        continue
      }
      if (line.charCodeAt(0) === 58 /* ':' */) continue
      if (line.startsWith('data: ')) {
        pendingData = line.slice(6)
      }
    }
    // Amortised compaction — discard the consumed prefix once it dominates the buffer.
    if (cursor > 16384 && cursor * 2 >= lineBuf.length) {
      lineBuf = lineBuf.slice(cursor)
      cursor = 0
    }
  }

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
      lineBuf += decoder.decode(value!, { stream: true })
      processBufferedLines()
    }
  }

  return { cancel, readNextEntry }
}
