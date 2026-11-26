export { WireSender }

import {
  LOSSY_CONFIRM_SLACK_MS,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_BACKLOG_BASE_BYTES,
} from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { HEADER } from '../../wire/header.js'
import { TAG, passesData } from '../../wire/tags.js'
import { assertProtocol } from '../../wire/protocolViolation.js'
import type { ServerChannel } from './ServerChannel.js'
import type { PeerSender } from './IndexedPeer.js'
import type { ServerTransport, Wire } from './ChannelMux.js'
import type { ChannelHandle } from './SessionRegistry.js'
import type { Deliveries } from '../../flowControl/Deliveries.js'
import {
  countWaitingPublishes,
  droppingFrom,
  droppingLossy,
  droppingPublishes,
  type WireQueue,
} from '../../flowControl/server/waitingPublishes.js'
import { SocketWait, type SocketWaitOwner } from '../../flowControl/server/SocketWait.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { ServerPieceReceiver } from '../../flowControl/server/ServerPieceReceiver.js'
import type { PieceReceiverOwner } from '../../flowControl/pieces.js'
import { TransportSocket } from './TransportSocket.js'
import { EventStream } from './EventStream.js'

type WireSenderOptions = {
  wire: Wire
  transport: ServerTransport<unknown>
  /** The wire's ping interval, which sizes the frames its headroom sends whole. */
  pingInterval: number
  /** The POST turn of the page's confirmations on SSE. */
  sseFlushThrottle: number
  /** What the wire delivered. */
  deliveries: Deliveries
  /** The channels of the session the wire carries, which its backlog counts; undefined before its RECONCILED. */
  heldChannels: () => ReadonlyMap<number, ChannelHandle> | undefined
  /** The channels attaching to the wire in this turn, which its backlog counts too. */
  attaching: ReadonlySet<ServerChannel>
  /** The wire's backlog passed what its channels allow, in a frame the sender was handed: the owner ends the wire. */
  onPastBacklog: () => void
}

/** A wire's headroom and the receiver that acknowledges what the wire took, on a wire that takes pieces. */
type WirePieces = { headroom: ServerHeadroom; receiver: ServerPieceReceiver }

/** Sends a wire's frames in call order, but for a RECONCILED and an attach probe's answer, which go ahead of what waits
 *  for the RECONCILED; has its owner end the wire once its backlog passes what its channels allow. The peers of its
 *  channels send through it, and once the wire closed it drops what they send. */
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
  private readonly heldChannels: () => ReadonlyMap<number, ChannelHandle> | undefined
  private readonly attaching: ReadonlySet<ServerChannel>
  private readonly onPastBacklog: () => void
  /** The wire closed: it sends nothing more, holds nothing and delivered nothing. */
  private closed = false
  /** What may be written before the backlog is read again against what its channels' flow control allows. */
  private backlogRoom = 0
  /** The largest frame sent on the wire. */
  private largestSent = 0
  /** Its backlog passed that, and its owner ends the wire. */
  private pastBacklog = false
  /** A RECONCILED is owed: what is sent meanwhile is held behind it. */
  private owed = false
  /** What was sent while a RECONCILED was owed, in order: channels' frames, which their replay also keeps, and the
   *  wire's own control frames, which the next attach sends anew if the wire goes. */
  private readonly held: Uint8Array<ArrayBuffer>[] = []
  /** Their bytes, which the backlog counts as it does what the socket holds. */
  private heldBytes = 0
  /** WebSocket attach probe answers waiting to ride the RECONCILED. */
  private readonly probeAnswers: Uint8Array<ArrayBuffer>[] = []
  /** The channels' ends a close the server decides keeps for the transport, in their order (see `close`). */
  private readonly lastEnds: Uint8Array<ArrayBuffer>[] = []

  constructor({
    wire,
    transport,
    pingInterval,
    sseFlushThrottle,
    deliveries,
    heldChannels,
    attaching,
    onPastBacklog,
  }: WireSenderOptions) {
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
    this.heldChannels = heldChannels
    this.attaching = attaching
    this.onPastBacklog = onPastBacklog
  }

  /** A WebSocket page's confirmation waits behind its wire's queues; an SSE page's waits for its POST's turn: the
   *  throttle, or the POST ahead of it, which takes a round trip. */
  behindAfter(roundTrip: number): number {
    const wait = this.pieces ? WIRE_QUEUE_DELAY_MS : Math.max(this.sseFlushThrottle, roundTrip)
    return LOSSY_CONFIRM_SLACK_MS + wait + roundTrip
  }

  send(frame: Uint8Array<ArrayBuffer>): void {
    if (!this.admit(frame)) return
    if (!this.owed) return this.hand(frame)
    this.held.push(frame)
    this.heldBytes += frame.byteLength
  }

  sendPublish(index: number, seq: number, body: Uint8Array<ArrayBuffer>): void {
    if (this.closed || this.pastBacklog) return
    const bytes = HEADER + body.byteLength
    if (!this.owed && bytes <= this.backlogRoom) {
      const headroom = this.pieces?.headroom
      if (headroom) {
        const room = headroom.gatherRoom(bytes)
        if (room !== undefined) {
          this.handGatheredPublish(body.byteLength, bytes)
          headroom.sender.gatherPublish(index, seq, body, room)
          return
        }
      } else if (this.wireQueue instanceof EventStream && this.wireQueue.canGatherPublish(bytes)) {
        this.handGatheredPublish(body.byteLength, bytes)
        this.wireQueue.gatherPublish(index, seq, body)
        return
      }
    }
    this.send(encode.publishTo(index, seq))
    this.send(body)
  }

  private handGatheredPublish(bodyBytes: number, bytes: number): void {
    this.backlogRoom -= bytes
    this.largestSent = Math.max(this.largestSent, HEADER, bodyBytes)
    this.wait.handed(bytes, false, performance.now())
  }

  /** A RECONCILE is answered: what is sent from now, its replay first, goes behind its RECONCILED (`sendReconciled`),
   *  so the page reads that first on every host and transport. A page has one RECONCILE out at a time. */
  owesReconciled(): void {
    assertProtocol(!this.owed, 'RECONCILE while its wire owes a RECONCILED')
    this.owed = true
    // What is held is measured from its first frame, on every host.
    this.backlogRoom = 0
  }

  /** An attach probe's answer precedes the RECONCILED, which may take time to arrive in full. */
  answerProbe(ix: number, probe: number): void {
    const answer = encode.bdpPingAck(ix, probe, Infinity, Infinity)
    if (!this.admit(answer)) return
    if (!this.pieces) return this.hand(answer)
    this.probeAnswers.push(answer)
    this.heldBytes += answer.byteLength
  }

  /** The RECONCILED goes ahead of what was held, and that follows, in order. */
  sendReconciled(reconciled: Uint8Array<ArrayBuffer>): void {
    this.owed = false
    if (this.admit(reconciled)) {
      const first = this.probeAnswers[0]
      if (first !== undefined) {
        const bytes = first.byteLength * this.probeAnswers.length
        // Keep a large group of replies out of the first message of an unmeasured link.
        const carry = bytes <= WIRE_PROBE_FIRST_BYTES && bytes + reconciled.byteLength <= 2 * WIRE_PROBE_PAIR_BYTES
        for (const answer of this.probeAnswers) {
          if (carry) this.carry(answer)
          else this.hand(answer)
        }
      }
      this.hand(reconciled)
      // A receipt can leave while a large reply waits behind data in pieces.
      const sender = this.pieces?.headroom.sender
      if (sender?.dataWaits) sender.flush()
    }
    this.probeAnswers.length = 0
    // `hand` gives the frame to the transport only, which sends nothing back through here meanwhile.
    for (const frame of this.held) this.hand(frame)
    this.held.length = 0
    this.heldBytes = 0
  }

  /** What the wire holds back for its page: what its transport holds, then what it holds for a RECONCILED. */
  get queue(): WireQueue {
    return this
  }

  waitingPublishes(ix: number): number {
    const queued = this.wireQueue.waitingPublishes(ix)
    return this.held.length === 0 ? queued : queued + countWaitingPublishes(this.held, ix)
  }

  dropWaitingPublishes(ix: number, keep: number, upTo?: number): void {
    if (this.held.length === 0) return this.wireQueue.dropWaitingPublishes(ix, keep, upTo)
    const queued = this.wireQueue.waitingPublishes(ix)
    this.wireQueue.dropWaitingPublishes(ix, keep, upTo)
    this.dropHeld(droppingPublishes(ix, Math.max(0, keep - queued), upTo === undefined ? undefined : upTo - queued))
  }

  dropWaitingFrom(ix: number, from: 'publish' | 'start'): void {
    if (this.held.length === 0) return this.wireQueue.dropWaitingFrom(ix, from)
    // From a publish the transport holds on, all of the channel's frames held behind it go too.
    const fromStart = from === 'start' || this.wireQueue.waitingPublishes(ix) > 0
    this.wireQueue.dropWaitingFrom(ix, from)
    this.dropHeld(droppingFrom(ix, fromStart ? 'start' : 'publish'))
  }

  /** The socket has no byte of what the transport holds, nor of what the wire holds for a RECONCILED, which is newer. */
  dropUngiven(ix: number): number {
    const dropped = this.wireQueue.dropUngiven(ix)
    if (this.held.length === 0) return dropped
    const cut = droppingLossy(ix)
    this.dropHeld(cut.drops)
    return dropped + cut.count()
  }

  /** `drops` is asked in the order the frames were held. */
  private dropHeld(drops: (frame: Uint8Array) => boolean): void {
    let kept = 0
    for (const frame of this.held) {
      if (!drops(frame)) this.held[kept++] = frame
      else this.heldBytes -= frame.byteLength
    }
    this.held.length = kept
  }

  /** Counts `frame` against what the wire may hold, whether it is handed now or held for a RECONCILED; false once the
   *  wire's backlog passed that, and the owner is told, after the turn, to end the wire. */
  private admit(frame: Uint8Array<ArrayBuffer>): boolean {
    if (this.closed || this.pastBacklog) return false
    if (frame.byteLength > this.largestSent) this.largestSent = frame.byteLength
    if (frame.byteLength > this.backlogRoom) {
      this.backlogRoom = this.measureBacklogRoom()
      if (this.backlogRoom < 0) {
        this.pastBacklog = true
        queueMicrotask(this.onPastBacklog)
        return false
      }
    }
    this.backlogRoom -= frame.byteLength
    return true
  }

  /** Gives an admitted frame to the transport. */
  private hand(frame: Uint8Array<ArrayBuffer>): void {
    if (this.closed || this.pastBacklog) return
    if (frame[0] === TAG.ACK_RES) this.pieces?.receiver.answering()
    const at = performance.now()
    this.wait.handed(frame.byteLength, passesData(frame[0]!), at)
    if (this.pieces) this.pieces.headroom.send(frame)
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

  /** The wire closed: drops what is sent from now on, and stops its headroom. `ends: 'first'`, at an end the server
   *  decides: the channels' ends decided before it (`ERROR`) that the socket has no byte of are kept, in their order,
   *  for `handLastEnds` to give the transport once the close is decided; what it holds for a RECONCILED goes after what
   *  its piece sender holds. `'dropped'` drops them too. */
  close(ends: 'first' | 'dropped' = 'dropped'): void {
    if (ends === 'first') this.lastEnds.push(...this.ungivenEnds())
    else this.lastEnds.length = 0
    this.closed = true
    this.owed = false
    this.probeAnswers.length = 0
    this.held.length = 0
    this.heldBytes = 0
    this.pieces?.headroom.close()
  }

  /** Gives the transport the channels' ends `close('first')` kept, as they would have gone on the wire. */
  handLastEnds(): void {
    for (const frame of this.lastEnds.splice(0)) this.transport.sendNow(this.wire, frame)
  }

  /** The channels' ends the socket has no byte of, taken from what holds them, in the order they go. */
  private ungivenEnds(): Uint8Array<ArrayBuffer>[] {
    const ends: Uint8Array<ArrayBuffer>[] = []
    const take = (frame: Uint8Array<ArrayBuffer>) => {
      if (frame[0] !== TAG.ERROR) return false
      ends.push(frame)
      return true
    }
    this.pieces?.headroom.sender.dropUngiven(take)
    for (const frame of this.held) take(frame)
    return ends
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

  /** What the wire may hold, less what it holds, in its socket and for a RECONCILED; `Infinity` if the runtime can't tell
   *  the socket's while no RECONCILED is owed: what is held counts on every host, the socket's as nothing where unknown.
   *  Its data is what its channels hold, so what passes this is what nothing else bounds: a page flooding it with what
   *  it answers at once. */
  private measureBacklogRoom(): number {
    const socket = this.bufferedAmount()
    if (socket === undefined && !this.owed) return Infinity
    let allowed = WIRE_SEND_BACKLOG_BASE_BYTES + this.largestSent
    for (const channel of this.channels()) allowed += channel.heldBytes
    return allowed - (socket ?? 0) - this.heldBytes
  }

  /** The channels the wire's send backlog counts: its session's, and those attaching in this turn. */
  private channels(): Set<ServerChannel> {
    const channels = new Set(this.attaching)
    for (const { channel } of this.heldChannels()?.values() ?? []) channels.add(channel)
    return channels
  }
}
