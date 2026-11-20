export { WireSender }

import { LOSSY_CONFIRM_SLACK_MS, WIRE_QUEUE_DELAY_MS, WIRE_SEND_BACKLOG_BASE_BYTES } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { TAG, passesData } from '../../wire/tags.js'
import type { ServerChannel } from './ServerChannel.js'
import type { PeerSender } from './IndexedPeer.js'
import type { ServerTransport, Wire } from './ChannelMux.js'
import type { SessionRegistry } from './SessionRegistry.js'
import type { Deliveries } from '../../flowControl/Deliveries.js'
import {
  countWaitingPublishes,
  droppingFrom,
  droppingPublishes,
  type WireQueue,
} from '../../flowControl/server/waitingPublishes.js'
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
class WireSender implements PeerSender, SocketWaitOwner, PieceReceiverOwner, WireQueue {
  private readonly wire: Wire
  private readonly transport: ServerTransport<unknown>
  /** Null on a wire that doesn't take pieces (SSE). */
  readonly pieces: WirePieces | null
  /** What the transport holds back for the page; what the sender holds for a RECONCILED is newer. */
  private readonly wireQueue: WireQueue
  private readonly sseFlushThrottle: number
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
  /** The RECONCILEDs owed: what is sent meanwhile is held behind the last of them. */
  private owed = 0
  /** What was sent while a RECONCILED was owed, in order: the frames themselves, which a replay also keeps. */
  private readonly held: Uint8Array<ArrayBuffer>[] = []

  constructor({ wire, transport, pingInterval, sseFlushThrottle, deliveries, sessions, attaching }: WireSenderOptions) {
    this.wire = wire
    this.transport = transport
    this.pieces = transport.wholeMessages
      ? {
          headroom: new ServerHeadroom({ socket: new TransportSocket({ transport, wire }), pingInterval }),
          receiver: new ServerPieceReceiver({ owner: this }),
        }
      : null
    this.wireQueue = this.pieces ? this.pieces.headroom : transport.queueOf!(wire)
    this.sseFlushThrottle = sseFlushThrottle
    this.deliveries = deliveries
    this.sessions = sessions
    this.attaching = attaching
  }

  /** A WebSocket page's confirmation waits behind its wire's queues; an SSE page's waits for its POST's turn: the
   *  throttle, or the POST ahead of it, which takes a round trip. */
  behindAfter(roundTrip: number): number {
    const wait = this.pieces ? WIRE_QUEUE_DELAY_MS : Math.max(this.sseFlushThrottle, roundTrip)
    return LOSSY_CONFIRM_SLACK_MS + wait + roundTrip
  }

  send(frame: Uint8Array<ArrayBuffer>): void {
    if (this.owed > 0) this.held.push(frame)
    else this.write(frame)
  }

  /** A RECONCILE is answered: what is sent from now, its replay first, goes behind its RECONCILED (`sendReconciled`),
   *  so the page reads that first on every host and transport. */
  owesReconciled(): void {
    this.owed++
  }

  /** An attach probe's answer, which says no credit ran out and names no round trip: the one frame that goes ahead of
   *  what is held, since a RECONCILED of many channels would read the path as slower than it is. */
  answerProbe(ix: number, probe: number): void {
    this.write(encode.bdpPingAck(ix, probe, Infinity, Infinity))
  }

  /** The RECONCILED goes ahead of what was held; once none is owed, so does that, in order. */
  sendReconciled(reconciled: Uint8Array<ArrayBuffer>): void {
    this.write(reconciled)
    this.owed--
    if (this.owed > 0) return
    // `write` hands the frame to the transport only, which sends nothing back through here meanwhile.
    for (const frame of this.held) this.write(frame)
    this.held.length = 0
  }

  /** What the wire holds back for its page: what its transport holds, then what it holds for a RECONCILED. */
  get queue(): WireQueue {
    return this
  }

  waitingPublishes(ix: number): number {
    return this.wireQueue.waitingPublishes(ix) + countWaitingPublishes(this.held, ix)
  }

  dropWaitingPublishes(ix: number, keep: number): void {
    const queued = this.wireQueue.waitingPublishes(ix)
    this.wireQueue.dropWaitingPublishes(ix, keep)
    this.dropHeld(droppingPublishes(ix, Math.max(0, keep - queued)))
  }

  dropWaitingFrom(ix: number, from: 'publish' | 'start'): void {
    // From a publish the transport holds on, all of the channel's frames held behind it go too.
    const fromStart = from === 'start' || this.wireQueue.waitingPublishes(ix) > 0
    this.wireQueue.dropWaitingFrom(ix, from)
    this.dropHeld(droppingFrom(ix, fromStart ? 'start' : 'publish'))
  }

  /** `drops` is asked in the order the frames were held. */
  private dropHeld(drops: (frame: Uint8Array) => boolean): void {
    let kept = 0
    for (const frame of this.held) if (!drops(frame)) this.held[kept++] = frame
    this.held.length = kept
  }

  private write(frame: Uint8Array<ArrayBuffer>): void {
    if (this.closed || this.pastBacklog) return
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
    this.owed = 0
    this.held.length = 0
    this.pieces?.headroom.close()
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
