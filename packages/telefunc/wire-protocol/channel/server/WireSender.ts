export { WireSender }

import { LOSSY_CONFIRM_SLACK_MS, WIRE_QUEUE_DELAY_MS, WIRE_SEND_BACKLOG_BASE_BYTES } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { TAG, passesData } from '../../wire/tags.js'
import type { ServerChannel } from './ServerChannel.js'
import type { PeerSender } from './IndexedPeer.js'
import type { ServerTransport, Wire } from './ChannelMux.js'
import type { SessionRegistry } from './SessionRegistry.js'
import type { Deliveries } from '../../flowControl/Deliveries.js'
import type { WireQueue } from '../../flowControl/server/waitingPublishes.js'
import { SocketWait, type SocketWaitOwner } from '../../flowControl/server/SocketWait.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { ServerPieceReceiver } from '../../flowControl/server/ServerPieceReceiver.js'
import type { PieceReceiverOwner } from '../../flowControl/pieces.js'
import { TransportSocket } from './TransportSocket.js'

type WireSenderOptions = {
  wire: Wire
  transport: ServerTransport<unknown>
  /** The wire's ping interval, which sizes the frames its headroom sends whole. */
  pingInterval: number
  /** The POST turn of the page's confirmations on SSE. */
  sseFlushThrottle: number
  /** What the wire delivered. */
  deliveries: Deliveries
  /** The mux's sessions, whose channels on the wire its headroom counts. */
  sessions: SessionRegistry
  /** The channels attaching to the wire in this turn, which its headroom counts too. */
  attaching: ReadonlySet<ServerChannel>
}

/** A wire's headroom and the receiver that acknowledges what the wire took, on a wire that takes pieces. */
type WirePieces = { headroom: ServerHeadroom; receiver: ServerPieceReceiver }

/** Sends a wire's frames in call order, and terminates the wire once its backlog passes what its channels allow; the
 *  peers of its channels send through it, and once the wire closed it drops what they send. */
class WireSender implements PeerSender, SocketWaitOwner, PieceReceiverOwner {
  private readonly wire: Wire
  private readonly transport: ServerTransport<unknown>
  /** Null on a wire that doesn't take pieces (SSE). */
  readonly pieces: WirePieces | null
  readonly queue: WireQueue
  readonly behindAfter: number
  /** How long the frames that go at once wait behind what the socket holds. */
  private readonly wait = new SocketWait({ owner: this })
  private readonly deliveries: Deliveries
  private readonly sessions: SessionRegistry
  private readonly attaching: ReadonlySet<ServerChannel>
  /** The wire closed: it sends nothing more, holds nothing and delivered nothing. */
  private closed = false
  /** What may be written before the backlog is read again against what its channels' flow control allows. */
  private headroom = 0
  /** The largest frame sent on the wire. */
  private largestSent = 0
  /** Its backlog passed that, and the wire is being terminated. */
  private pastBacklog = false
  /** The RECONCILE's probes its RECONCILED hasn't answered: a frame ahead of it goes behind their BDP_PING_ACKs. */
  private probed: { ix: number; probe: number }[] = []

  constructor({ wire, transport, pingInterval, sseFlushThrottle, deliveries, sessions, attaching }: WireSenderOptions) {
    this.wire = wire
    this.transport = transport
    this.pieces = transport.wholeMessages
      ? {
          headroom: new ServerHeadroom({ socket: new TransportSocket({ transport, wire }), pingInterval }),
          receiver: new ServerPieceReceiver({ owner: this }),
        }
      : null
    this.queue = this.pieces ? this.pieces.headroom : transport.queueOf!(wire)
    this.behindAfter = LOSSY_CONFIRM_SLACK_MS + (this.pieces ? WIRE_QUEUE_DELAY_MS : sseFlushThrottle)
    this.deliveries = deliveries
    this.sessions = sessions
    this.attaching = attaching
  }

  send(frame: Uint8Array<ArrayBuffer>): void {
    if (this.closed || this.pastBacklog) return
    if (this.probed.length > 0) this.answerProbes()
    if (frame[0] === TAG.ACK_RES) this.pieces?.receiver.answering()
    if (frame.byteLength > this.largestSent) this.largestSent = frame.byteLength
    if (frame.byteLength > this.headroom) {
      this.headroom = this.measureHeadroom()
      if (this.headroom < 0) {
        this.pastBacklog = true
        queueMicrotask(() => this.transport.terminateWire(this.wire))
        return
      }
    }
    this.headroom -= frame.byteLength
    const at = performance.now()
    this.wait.handed(frame.byteLength, passesData(frame[0]!), at)
    if (this.pieces) this.pieces.headroom.send(frame, at)
    else this.transport.sendNow(this.wire, frame)
  }

  carry(frame: Uint8Array<ArrayBuffer>): void {
    if (this.pieces) this.pieces.headroom.sender.carry(() => frame)
    else this.send(frame)
  }

  /** `PieceReceiverOwner`: its receiver's acknowledgement goes as any frame of the wire does. */
  sendAck(frame: Uint8Array<ArrayBuffer>): void {
    this.send(frame)
  }

  carryAck(take: () => Uint8Array<ArrayBuffer> | null): void {
    this.pieces?.headroom.sender.carry(take)
  }

  sendWait(): SocketWait {
    return this.wait
  }

  deliveredBefore(ms: number): number {
    return this.closed ? 0 : this.deliveries.beforeHandled(ms)
  }

  /** The wire closed: drops what is sent from now on, and stops its headroom. */
  close(): void {
    this.closed = true
    this.pieces?.headroom.close()
  }

  /** Answers the probe ahead of the next frame, unless the RECONCILED goes first. */
  addProbe(ix: number, probe: number): void {
    this.probed.push({ ix, probe })
  }

  /** The RECONCILED going out answers the probes. */
  dropProbes(): void {
    this.probed = []
  }

  /** What its socket holds unsent, and what its piece sender hasn't handed it. */
  bufferedAmount(): number | undefined {
    if (this.closed) return 0
    const socket = this.transport.bufferedAmount(this.wire)
    return socket === undefined ? undefined : socket + (this.pieces?.headroom.sender.unhandedBytes ?? 0)
  }

  /** What a frame that goes at once counts as ahead of it: the transport's `queuedAhead` where it has one. */
  queuedAhead(): number | undefined {
    if (this.closed) return 0
    return this.transport.queuedAhead ? this.transport.queuedAhead(this.wire) : this.bufferedAmount()
  }

  /** Each answer says no credit ran out and names no round trip. */
  private answerProbes(): void {
    const probed = this.probed
    this.probed = []
    for (const { ix, probe } of probed) this.send(encode.bdpPingAck(ix, probe, Infinity, Infinity))
  }

  /** What the wire may hold, less what it holds; `Infinity` if the runtime can't tell. Its data is what its channels hold,
   *  so what passes this is what nothing else bounds: a page flooding it with what it answers at once. */
  private measureHeadroom(): number {
    const backlog = this.bufferedAmount()
    if (backlog === undefined) return Infinity
    let allowed = WIRE_SEND_BACKLOG_BASE_BYTES + this.largestSent
    for (const channel of this.channels()) allowed += channel.heldBytes
    return allowed - backlog
  }

  /** The channels the wire's send headroom counts: its session's, and those attaching in this turn. */
  private channels(): Set<ServerChannel> {
    const sessionId = this.transport.getSessionId(this.wire)
    const session = sessionId === undefined ? undefined : this.sessions.peekSession(sessionId)
    const channels = new Set(this.attaching)
    for (const { channel } of session?.values() ?? []) channels.add(channel)
    return channels
  }
}
