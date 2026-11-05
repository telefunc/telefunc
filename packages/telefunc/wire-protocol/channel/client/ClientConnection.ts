export { ClientConnection }
export type {
  ClientChannelTransport,
  ClientConnectionOptions,
  MuxChannel,
  MuxConnection,
  OutboundFrame,
  OutboundFrameKind,
}

import { assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import {
  CHANNEL_TRANSPORT,
  type ChannelTransport,
  type ChannelTransports,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import type { Urgency } from '../../flowControl/credit.js'
import { ACK_STATUS, encode } from '../../wire/frame.js'
import type { AckResultStatus, BdpPingAck, ChannelFrame, SequencedEncoder } from '../../wire/frame.js'
import type { ReconciledPayload } from '../../wire/payloads.js'
import type { Heartbeat } from './Heartbeat.js'
import type { ChannelTable } from './ChannelTable.js'
import type { SendGate } from './SendGate.js'
import type { ChannelAttach } from './ChannelAttach.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { Downlink } from '../../flowControl/client/Downlink.js'
import { WireLifecycle, type WireLifecycleOwner } from './WireLifecycle.js'

type OutboundFrameKind = 'reconcile' | 'released' | 'control' | Urgency | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
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
  /** Its credits as the connection drives them across wires. */
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
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel, abortValue: string): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendByteLimit(channel: MuxChannel, limit: number, urgency: Urgency): void
  sendMessageLimit(channel: MuxChannel, limit: number, urgency: Urgency): void
  sendBdpPing(channel: MuxChannel, probe: number): void
  sendBdpPingAck(channel: MuxChannel, ack: BdpPingAck): void
  /** Bytes the wire holds that haven't gone out. */
  bufferedAmount(): number
  /** Bytes the wire delivered in the `ms` before its latest delivery: the frame being handled, on arrival. */
  deliveredBefore(ms: number): number
  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void
  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void
  unregister(channel: MuxChannel): void
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
  hasWire(): boolean
  isConnecting(): boolean
  /** Send a wire-level ping on this wire. Heartbeat's send callback calls this. */
  sendPing(frame: Uint8Array<ArrayBuffer>): void
  sendFrame(frame: OutboundFrame): void
  /** How many bytes of data frames it accepts now, `Infinity` if nothing bounds them. Other frames go at once. */
  headroom(): number
  /** Bytes of the frames it was handed that haven't gone out to the network. */
  bufferedAmount(): number
  abandonActiveTransport(): void
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
const globalObject = getGlobalObject<{ connections: Map<string, ClientConnection> }>(
  'channel/client/ClientConnection.ts',
  { connections: new Map() },
)

/** What a page's channels send through, one connection per server and set of transports: it encodes each channel's
 *  frames and hands them to its send gate; the wire lifecycle carries them. */
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
  /** True if its transports include a WebSocket, whose runtime may accept smaller frames than the wire's limit. */
  private readonly usesWebSocket: boolean
  private readonly lifecycle: WireLifecycle
  private readonly channels: ChannelTable
  private readonly gate: SendGate
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
    this.usesWebSocket = options.transports.includes(CHANNEL_TRANSPORT.WS)
    this.lifecycle = new WireLifecycle({ owner: this, telefuncUrl, options })
    this.channels = this.lifecycle.channels
    this.gate = this.lifecycle.gate
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
    this.lifecycle.unregister(channel)
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
    const record = this.channels.of(channel)
    if (record === undefined) return 0
    const { replay } = record
    const frame = encodeFrame(record.ix, data, replay.seq + 1)
    const maxFrameBytes = this.maxFrameBytesOf(channel)
    // The server ends any wire a larger frame arrives on, and the replay would send it again on the next one.
    assertUsage(
      frame.byteLength <= maxFrameBytes,
      `Channel message too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`,
    )
    const seq = replay.nextSeq()
    onQueued?.(seq)
    this.gate.send(record, seq, frame, kind)
    return frame.byteLength
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const { ix } = record
    const seq = record.replay.nextSeq()
    let frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    const maxFrameBytes = this.maxFrameBytesOf(channel)
    // The server ends any wire a larger frame arrives on, so the awaiting send gets an error answer instead.
    if (frame.byteLength > maxFrameBytes) {
      const error = `Channel ack answer too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`
      frame = encode.ackRes(ix, seq, ackedSeq, error, ACK_STATUS.ERROR)
    }
    this.gate.send(record, seq, frame, 'ack')
    if (ackedSeq === record.lastSeq) channel._attach.receiveCredit.answered()
  }

  /** The largest frame the server accepts from `channel`; on a WebSocket, a runtime's lower limit wins. */
  private maxFrameBytesOf(channel: MuxChannel): number {
    if (!this.usesWebSocket) return WIRE_MAX_RAW_FRAME_BYTES
    return Math.min(channel._maxFrameBytes, this.lifecycle.maxFrameBytes)
  }

  /** Sequenced like data, as CLOSE and CLOSE_ACK are, so a closing frame lost with a dead wire is replayed. */
  sendAbort(channel: MuxChannel, abortValue: string): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const seq = record.replay.nextSeq()
    this.gate.send(record, seq, encode.abort(record.ix, abortValue, seq), 'control')
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const seq = record.replay.nextSeq()
    this.gate.send(record, seq, encode.close(record.ix, timeoutMs, seq), 'control')
  }

  sendCloseAck(channel: MuxChannel): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const seq = record.replay.nextSeq()
    this.gate.send(record, seq, encode.closeAck(record.ix, seq), 'control')
  }

  sendByteLimit(channel: MuxChannel, limit: number, urgency: Urgency): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.window(record.ix, limit, record.lastSeq), urgency)
  }

  sendMessageLimit(channel: MuxChannel, limit: number, urgency: Urgency): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.msgWindow(record.ix, limit), urgency)
  }

  sendBdpPing(channel: MuxChannel, probe: number): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.bdpPing(record.ix, probe), 'flow-control')
  }

  sendBdpPingAck(channel: MuxChannel, { probe, backlog, pathRtt }: BdpPingAck): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    // On batch POSTs the page's measured round trip leaves out the wait for its POST, so it reports no path RTT.
    const frame = encode.bdpPingAck(record.ix, probe, backlog, this.transport.batched ? Infinity : pathRtt)
    this.gate.sendUnsequenced(record, frame, 'flow-control')
  }

  bufferedAmount(): number {
    return this.transport.bufferedAmount() + this.gate.waitingBytes
  }

  deliveredBefore(ms: number): number {
    return this.transport.heartbeat?.deliveredBefore(ms) ?? 0
  }

  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.broadcastSub(record.ix, binary), 'control')
  }

  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.broadcastUnsub(record.ix, binary), 'control')
  }
}
