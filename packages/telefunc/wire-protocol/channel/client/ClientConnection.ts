export { ClientConnection }
export type {
  ClientChannelTransport,
  ClientConnectionOptions,
  MuxChannel,
  MuxConnection,
  OutboundFrame,
  OutboundFrameKind,
}

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { type ChannelTransport, type ChannelTransports } from '../../constants.js'
import type { Urgency } from '../../flowControl/credit.js'
import { ACK_STATUS, ERROR_REASON, encode } from '../../wire/frame.js'
import type { AckResultStatus, BdpPingAck, ChannelFrame, SequencedEncoder } from '../../wire/frame.js'
import type { CallWant, ReconciledPayload, Want } from '../../wire/payloads.js'
import type { Heartbeat } from './Heartbeat.js'
import type { AbortReasonLimit } from '../channel.js'
import { HEADER } from '../../wire/header.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import type { FrameAt, SendQueue } from './SendQueue.js'
import type { ChannelAttach } from './ChannelAttach.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { Downlink } from '../../flowControl/client/Downlink.js'
import { WireLifecycle, type WireLifecycleOwner } from './WireLifecycle.js'

type OutboundFrameKind = 'reconcile' | 'control' | Urgency | 'ack' | 'data' | 'heartbeat'

/** A frame and its kind, which it keeps through every wait. `released`: it waited (held back, for headroom, or in the
 *  replay), and goes by the sooner of its kind's deadline and the release's. */
type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
  released?: boolean
  receivedAt?: number
}

interface MuxChannel {
  readonly id: string
  readonly isClosed: boolean
  /** The largest frame the server accepts from this channel on a WebSocket. */
  readonly _maxFrameBytes: number
  /** The channel is open on `uplink`'s wire; called again with the same `uplink`, that wire lost nothing. */
  _onTransportOpen(batched: boolean, uplink: Uplink, downlink: Downlink): void
  /** Entry point for every per-channel wire frame (data + per-channel ctrl). The
   *  channel splits ctrl vs data internally. Wire-level frames (PING/PONG/
   *  FIN/RECONCILED), channel-termination ctrls (ABORT/ERROR) and ATTACH_RESULT stay with the
   *  connection — they involve connection-side cleanup. */
  _dispatchFrame(frame: ChannelFrame): void
  _onEnded(err?: Error): void
  /** Its connection is gone for good. */
  _onConnectionLost(err: Error): void
  /** What it declares each time it attaches: its credits and, for a broadcast, its want. */
  readonly _attach: ChannelAttach
}

interface MuxConnection {
  sendSequenced<T>(
    channel: MuxChannel,
    kind: 'data' | 'ack',
    encodeFrame: SequencedEncoder<T>,
    data: T,
    onQueued?: (seq: number) => void,
  ): number
  /** Puts a newer frame in the place of the channel's frame `seq` while that still waits to go and is the channel's last,
   *  and returns the bytes it adds over that one: null once it went, or once the channel sent a frame after it. */
  replaceWaiting<T>(channel: MuxChannel, seq: number, encodeFrame: SequencedEncoder<T>, data: T): number | null
  /** Throws where `data`, encoded by `encodeFrame`, is larger than the server accepts, as its send would. */
  assertFits<T>(channel: MuxChannel, encodeFrame: SequencedEncoder<T>, data: T): void
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  /** `inPlaceOf`: a lossy send's seq, which the ABORT takes the place of where that still waits and is the channel's
   *  last, so it never goes. */
  sendAbort(channel: MuxChannel, abortValue: string, inPlaceOf?: number): void
  /** The most of an abort reason the server accepts from `channel`: it ends any wire a larger frame arrives on. */
  abortReasonLimit(channel: MuxChannel): AbortReasonLimit
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendCloseDone(channel: MuxChannel): void
  sendCloseTimedOut(channel: MuxChannel): void
  sendByteLimit(channel: MuxChannel, limit: number, urgency: Urgency, receivedAt?: number): void
  sendMessageLimit(channel: MuxChannel, limit: number, urgency: Urgency): void
  sendBdpPing(channel: MuxChannel, probe: number): void
  sendBdpPingAck(channel: MuxChannel, ack: BdpPingAck): void
  /** Bytes the wire holds that haven't gone out. */
  bufferedAmount(): number
  /** Bytes the wire delivered in the `ms` before its latest delivery: the frame being handled, on arrival. */
  deliveredBefore(ms: number): number
  sendWant(channel: MuxChannel, want: Want): void
  /** The server confirmed every want `channel` sent: a call carries none of it. */
  wantSettled(channel: MuxChannel): void
  unregister(channel: MuxChannel): void
  /** The server ended `channel`, which then sends nothing more: the server needs nothing more of what it sent. */
  serverEnded(channel: MuxChannel): void
  reconnectWindow(): number
}

type ClientConnectionOptions = {
  transports: ChannelTransports
  fetchImpl: typeof fetch
  /** Client-side cache-key extension — distinct values get distinct `ClientConnection` instances. Never sent on the wire. */
  connectionKey?: string
  /** User headers (config.headers + per-call `withContext({ headers })`) merged into every transport fetch. The first
   *  channel's headers serve every channel the connection carries. */
  headers?: Record<string, string>
  /** Override the idle-close delay after all channels close. Default: the server's `idleTimeout` once a RECONCILED says
   *  it, 60 000 ms before. Pass 0 to dispose immediately. */
  idleTimeout?: number
}

type ClientChannelTransport = {
  readonly type: ChannelTransport
  /** True if the request that opens the wire carries the RECONCILE and the frames buffered behind it. If false, the
   *  RECONCILE goes once the wire is open, and the buffered frames after its RECONCILED. */
  readonly carriesReconcileInOpen: boolean
  /** True iff client→server frames are per-POST batched instead of pushed onto
   *  one streaming body — signals the channel to use a larger initial window. */
  readonly batched: boolean
  /** The uplink and downlink of the wire this transport carries, or of the one it opens next. */
  readonly uplink: Uplink
  readonly downlink: Downlink
  start(): void
  /** Lets the attempt under way go, opened or not, through its own socket or request: it delivers nothing, and reports
   *  nothing more. */
  cancelAttempt(): void
  hasWire(): boolean
  isConnecting(): boolean
  /** Send a wire-level ping on this wire. Heartbeat's send callback calls this; `now` sends it without the heartbeat's
   *  hold. */
  sendPing(frame: Uint8Array<ArrayBuffer>, now?: boolean): void
  sendFrame(frame: OutboundFrame): void
  /** How many bytes of data frames it accepts now, `Infinity` if nothing bounds them. Other frames go at once. */
  headroom(): number
  /** How long a frame may wait to go before the page counts as behind: past the wire's own wait, its least round trip,
   *  so `Infinity` before that is measured. */
  readonly behindAfter: number
  /** Bytes of the frames it was handed that haven't gone out to the network. */
  bufferedAmount(): number
  abandonActiveTransport(): void
  /** Drops the data frames of channel `ix` it holds and hasn't sent: nothing more of that channel goes. */
  dropQueued(ix: number): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: ReconciledPayload): void
  /** The heartbeat of its current wire, which also tracks what the wire delivers. */
  readonly heartbeat: Heartbeat | null
  /** Liveness builds the Heartbeat and hands it over; the transport's receive path then reports each PONG to it. */
  attachHeartbeat(heartbeat: Heartbeat): void
  detachHeartbeat(): void
  /** Settles once the frames it was handed have left it, or its wire is gone. */
  drained(): Promise<void>
  dispose(): void
}

// One per page, however many copies of Telefunc it loaded: each would otherwise open its own connections.
const globalObject = getGlobalObject<{
  connections: Map<string, ClientConnection>
  /** The page's channels with a want the server hasn't confirmed, by the `telefuncUrl` of the calls that carry it. */
  wanting: Map<string, Set<MuxChannel>>
}>('channel/client/ClientConnection.ts', { connections: new Map(), wanting: new Map() })

/** What a page's channels send through, one connection per server and set of transports: it encodes each channel's
 *  frames and hands them to its send queue; the wire lifecycle carries them. */
class ClientConnection implements MuxConnection, WireLifecycleOwner {
  static getOrCreate(telefuncUrl: string, channel: MuxChannel, options: ClientConnectionOptions): ClientConnection {
    // `connectionKey` opts callers out of the shared connection without the server seeing it.
    const key = `${options.transports.join(',')}:${telefuncUrl}|${options.connectionKey ?? ''}`
    let connection = globalObject.connections.get(key)
    if (!connection || !connection.lifecycle.takesChannels) {
      connection = new ClientConnection({ telefuncUrl, options, cacheKey: key })
      globalObject.connections.set(key, connection)
    }
    connection.lifecycle.register(channel)
    return connection
  }

  private readonly cacheKey: string
  private readonly telefuncUrl: string
  private readonly lifecycle: WireLifecycle
  private readonly sendQueue: SendQueue
  /** The transport its wire runs on now: the lifecycle replaces it on an upgrade, or a fallback to SSE. */
  private get transport(): ClientChannelTransport {
    return this.lifecycle.transport
  }

  private constructor({
    telefuncUrl,
    options,
    cacheKey,
  }: { telefuncUrl: string; options: ClientConnectionOptions; cacheKey: string }) {
    this.cacheKey = cacheKey
    this.telefuncUrl = telefuncUrl
    this.lifecycle = new WireLifecycle({ owner: this, telefuncUrl, options })
    this.sendQueue = this.lifecycle.sendQueue
  }

  /** The wants of the page's broadcasts on `telefuncUrl` that the server hasn't confirmed: a call to it carries them, so
   *  its server applies them before the telefunction runs. Undefined where there are none. */
  static unconfirmedWants(telefuncUrl: string): CallWant[] | undefined {
    const wanting = globalObject.wanting.get(telefuncUrl)
    if (wanting === undefined) return undefined
    const wants: CallWant[] = []
    for (const channel of wanting) {
      const want = channel.isClosed ? null : channel._attach.unconfirmedWant
      if (want !== null) wants.push({ channel: channel.id, ...want })
    }
    // A broadcast closing before its want was confirmed stays here until it sends nothing more.
    return wants.length === 0 ? undefined : wants
  }

  /** The next `getOrCreate` opens a fresh connection; one that already replaced this one stays cached. */
  onDisposed(): void {
    if (globalObject.connections.get(this.cacheKey) === this) globalObject.connections.delete(this.cacheKey)
  }

  /** How long the page waits for a server that went away: until the pong deadline, then `reconnectTimeout` more. */
  reconnectWindow(): number {
    return this.lifecycle.reconnectWindow()
  }

  /** Called once the channel sends nothing more. */
  unregister(channel: MuxChannel): void {
    this.wantSettled(channel)
    this.lifecycle.unregister(channel)
  }

  serverEnded(channel: MuxChannel): void {
    this.lifecycle.serverEnded(channel)
  }

  /** Encodes `data` with `encodeFrame` at the next seq and only then takes the seq, so a refused frame uses none.
   *  `onQueued` runs before the wire has the frame. Returns its bytes, or 0 once the channel sends nothing more. */
  sendSequenced<T>(
    channel: MuxChannel,
    kind: 'data' | 'ack',
    encodeFrame: SequencedEncoder<T>,
    data: T,
    onQueued?: (seq: number) => void,
  ): number {
    const frame = this.sendQueue.sendSequenced(channel, kind, ({ ix }, seq) => encodeFrame(ix, data, seq), onQueued)
    return frame?.byteLength ?? 0
  }

  replaceWaiting<T>(channel: MuxChannel, seq: number, encodeFrame: SequencedEncoder<T>, data: T): number | null {
    return this.sendQueue.replaceWaiting(channel, seq, ({ ix }, at) => encodeFrame(ix, data, at))
  }

  assertFits<T>(channel: MuxChannel, encodeFrame: SequencedEncoder<T>, data: T): void {
    this.sendQueue.assertFits(channel, ({ ix }, seq) => encodeFrame(ix, data, seq))
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    let answersNewest = false
    this.sendQueue.sendSequenced(channel, 'ack', ({ ix, lastSeq }, seq) => {
      answersNewest = ackedSeq === lastSeq
      const frame = encode.ackRes(ix, seq, ackedSeq, result, status)
      const maxFrameBytes = this.lifecycle.maxFrameBytesOf(channel)
      if (frame.byteLength <= maxFrameBytes) return frame
      // The server ends any wire a larger frame arrives on, so the awaiting send gets an error answer instead.
      const error = `Channel ack answer too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`
      return encode.ackRes(ix, seq, ackedSeq, error, ACK_STATUS.ERROR)
    })
    if (answersNewest) channel._attach.receiveCredit.answered()
  }

  abortReasonLimit(channel: MuxChannel): AbortReasonLimit {
    return {
      max: this.lifecycle.maxFrameBytesOf(channel),
      bytesOf: (serialized) => HEADER + utf8ByteLength(serialized),
      what: 'the server accepts in one frame',
    }
  }

  /** Sequenced like data, as CLOSE and CLOSE_ACK are, so a closing frame lost with a dead wire is replayed. */
  sendAbort(channel: MuxChannel, abortValue: string, inPlaceOf?: number): void {
    const abortAt: FrameAt = ({ ix }, seq) => encode.abort(ix, abortValue, seq)
    if (inPlaceOf !== undefined && this.sendQueue.replaceHeld(channel, inPlaceOf, abortAt)) return
    this.sendQueue.sendSequenced(channel, 'control', abortAt)
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    this.sendQueue.sendSequenced(channel, 'control', ({ ix }, seq) => encode.close(ix, timeoutMs, seq))
  }

  sendCloseAck(channel: MuxChannel): void {
    this.sendQueue.sendSequenced(channel, 'control', ({ ix, lastSeq }, seq) => encode.closeAck(ix, lastSeq, seq))
  }

  sendCloseDone(channel: MuxChannel): void {
    this.sendQueue.sendSequenced(channel, 'control', ({ ix, lastSeq }, seq) => encode.closeDone(ix, lastSeq, seq))
  }

  sendCloseTimedOut(channel: MuxChannel): void {
    this.sendQueue.sendSequenced(channel, 'control', ({ ix }, seq) =>
      encode.error(ix, ERROR_REASON.CLOSE_TIMED_OUT, seq),
    )
  }

  sendByteLimit(channel: MuxChannel, limit: number, urgency: Urgency, receivedAt?: number): void {
    this.sendQueue.sendUnsequenced(channel, urgency, ({ ix, lastSeq }) => encode.window(ix, limit, lastSeq), receivedAt)
  }

  sendMessageLimit(channel: MuxChannel, limit: number, urgency: Urgency): void {
    this.sendQueue.sendUnsequenced(channel, urgency, ({ ix }) => encode.msgWindow(ix, limit))
  }

  sendBdpPing(channel: MuxChannel, probe: number): void {
    this.sendQueue.sendUnsequenced(channel, 'flow-control', ({ ix }) => encode.bdpPing(ix, probe))
  }

  sendBdpPingAck(channel: MuxChannel, { probe, backlog, pathRtt }: BdpPingAck): void {
    // On batch POSTs the page's measured round trip leaves out the wait for its POST, so it reports no path RTT.
    const reported = this.transport.batched ? Infinity : pathRtt
    this.sendQueue.sendUnsequenced(channel, 'flow-control', ({ ix }) => encode.bdpPingAck(ix, probe, backlog, reported))
  }

  bufferedAmount(): number {
    return this.transport.bufferedAmount() + this.sendQueue.waitingBytes
  }

  deliveredBefore(ms: number): number {
    return this.transport.heartbeat?.deliveredBefore(ms) ?? 0
  }

  sendWant(channel: MuxChannel, want: Want): void {
    if (!this.sendQueue.sendUnsequenced(channel, 'control', ({ ix }) => encode.want(ix, want))) return
    let wanting = globalObject.wanting.get(this.telefuncUrl)
    if (wanting === undefined) globalObject.wanting.set(this.telefuncUrl, (wanting = new Set()))
    wanting.add(channel)
  }

  wantSettled(channel: MuxChannel): void {
    const wanting = globalObject.wanting.get(this.telefuncUrl)
    if (wanting?.delete(channel) && wanting.size === 0) globalObject.wanting.delete(this.telefuncUrl)
  }
}
