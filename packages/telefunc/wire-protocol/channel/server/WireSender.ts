export { WireSender }

import { WIRE_SEND_BACKLOG_BASE_BYTES } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { TAG, passesData } from '../../wire/tags.js'
import type { ServerChannel } from './channel.js'
import type { PeerSender } from './IndexedPeer.js'
import type { ServerTransport, Wire, WirePieces } from './mux.js'
import type { Deliveries } from '../../flowControl/Deliveries.js'
import { SocketWait, type SocketWaitOwner } from '../../flowControl/server/SocketWait.js'

type WireSenderOptions = {
  wire: Wire
  transport: ServerTransport<unknown>
  pieces: WirePieces | null
  /** What the wire delivered. */
  deliveries: Deliveries
  /** The channels whose flow control the headroom counts. */
  channels: () => Iterable<ServerChannel>
}

/** Sends a wire's frames in call order, and terminates the wire once its backlog passes what its channels allow; the
 *  peers of its channels send through it, and once the wire closed it drops what they send. */
class WireSender implements PeerSender, SocketWaitOwner {
  private readonly wire: Wire
  private readonly transport: ServerTransport<unknown>
  private readonly pieces: WirePieces | null
  /** How long the frames that go at once wait behind what the socket holds. */
  private readonly wait = new SocketWait({ owner: this })
  private readonly deliveries: Deliveries
  private readonly channels: () => Iterable<ServerChannel>
  /** The number of the delivery that completed the frame the wire is handling. */
  frameDelivery = 0
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

  constructor({ wire, transport, pieces, deliveries, channels }: WireSenderOptions) {
    this.wire = wire
    this.transport = transport
    this.pieces = pieces
    this.deliveries = deliveries
    this.channels = channels
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
        queueMicrotask(() => this.transport.terminateConnection(this.wire))
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

  sendWait(): SocketWait {
    return this.wait
  }

  deliveredBeforeFrame(ms: number): number {
    return this.closed ? 0 : this.deliveries.before(this.frameDelivery, ms)
  }

  close(): void {
    this.closed = true
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

  /** What its channels' flow control lets the wire hold, less what it holds; `Infinity` if the runtime can't tell. */
  private measureHeadroom(): number {
    const backlog = this.bufferedAmount()
    if (backlog === undefined) return Infinity
    let allowed = WIRE_SEND_BACKLOG_BASE_BYTES
    for (const channel of this.channels()) allowed += channel._sendAllowance() + this.largestSent
    return allowed - backlog
  }
}
