export { ClientHeadroom }
export type { ClientHeadroomOwner }

import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_ACK_GAP_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from '../../constants.js'
import { PathEstimate } from '../PathEstimate.js'
import { PieceSender, type PieceReceiverOwner, type PieceSenderOptions, type PieceSenderOwner } from '../pieces.js'
import type { Uplink } from './Uplink.js'
import type { Downlink } from './Downlink.js'
import type { PiecesAck } from '../../wire/frame.js'
import { isSequencedTag } from '../../wire/tags.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

/** The WebSocket transport a page's headroom belongs to. */
interface ClientHeadroomOwner {
  /** The pacing timer found the uplink drained some of what it held. */
  onHeadroom(headroom: ClientHeadroom): void
}

type ClientHeadroomOptions = Pick<PieceSenderOptions, 'socket' | 'pingInterval'> & {
  uplink: Uplink
  downlink: Downlink
  owner: ClientHeadroomOwner
}

/** Caps the data a page lets its WebSocket's queues hold, so the page's window refreshes don't wait long behind it. */
class ClientHeadroom implements PieceSenderOwner, PieceReceiverOwner {
  /** Sends every frame the WebSocket carries. */
  readonly sender: PieceSender
  /** What the path to the server delivers, from the server's acknowledgements of every data frame: one rides the
   *  server's next message after its gap, or goes alone a `WIRE_PIECES_ACK_GAP_MS` later. */
  readonly path = new PathEstimate({ reportGap: WIRE_SEND_ACK_GAP_MS + WIRE_PIECES_ACK_GAP_MS })
  private readonly uplink: Uplink
  private readonly downlink: Downlink
  private readonly owner: ClientHeadroomOwner
  /** When the newest acknowledged data arrived, in ms on the server's clock, counted from the first data an
   *  acknowledgement covered. */
  private arrivedAt = 0
  /** The shortest time an acknowledgement took to come back, up to that clock's offset: one that waited behind no
   *  uplink queue. */
  private leastBack = Number.POSITIVE_INFINITY
  private lateMs = 0
  private lateAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  private paceDue = 0

  constructor({ socket, pingInterval, uplink, downlink, owner }: ClientHeadroomOptions) {
    this.uplink = uplink
    this.downlink = downlink
    this.owner = owner
    this.sender = new PieceSender({
      socket,
      pingInterval,
      pieceRate: uplink.pieceRate,
      cuts: 'cut',
      owner: this,
    })
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    if (isSequencedTag(frame[0]!)) {
      this.sender.send(frame, this.sender.dataWaits ? 0 : this.headroom())
      return this.pace()
    }
    // What the frame waits behind, read before it is handed: once handed, the uplink holds it too.
    const wait = this.uplink.waitNow()
    this.sender.send(frame, 0)
    this.uplink.noteWait(wait)
  }

  /** `PieceReceiverOwner`: the wire's receiver sends its acknowledgements through this sender, past the headroom. */
  sendAck(frame: Uint8Array<ArrayBuffer>): void {
    this.sender.send(frame, 0)
  }

  carryAck(take: () => Uint8Array<ArrayBuffer> | null): void {
    this.sender.carry(take)
  }

  /** Sends what waits in the sender, as far as the headroom goes now. */
  sendWaiting(): void {
    if (this.sender.dataWaits) this.sender.sendWaiting(this.headroom())
  }

  /** The bytes the uplink drains in `WIRE_QUEUE_DELAY_MS`: at least `WIRE_SEND_AHEAD_MIN_BYTES`, at most a window. */
  private get allowance(): number {
    return Math.min(
      CREDIT_WINDOW_MAX_BYTES,
      Math.max(WIRE_SEND_AHEAD_MIN_BYTES, this.uplink.drain.rate * WIRE_QUEUE_DELAY_MS),
    )
  }

  /** The headroom past what the sender handed the socket. */
  headroom(): number {
    return this.headroomPast(this.sender.transmittedBytes)
  }

  /** The headroom past everything given to the sender, waiting frames included: none once used up or data waits. */
  headroomPastSent(): number {
    return this.headroomPast(this.sender.sentBytes)
  }

  /** The socket closed: pacing stops, and nothing more is handed to it. */
  close(): void {
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
    this.sender.close()
  }

  /** A message arrived. If the pacing timer is overdue, it looks now: a hidden tab throttles timers, not messages. */
  received(): void {
    if (this.paceTimer === null || performance.now() < this.paceDue) return
    clearTimeout(this.paceTimer)
    this.lookAgain()
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), then sends what the freed headroom allows. */
  acknowledged(ack: PiecesAck): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(ack)) return false
    const acknowledged = this.sender.acknowledgedBytes
    const newly = acknowledged - before
    const spanMs = ack.spanUs / 1000
    this.arrivedAt += spanMs
    const now = performance.now()
    this.path.reported(acknowledged, now, ack.heldMs)
    const back = now - this.arrivedAt - ack.heldMs
    if (back < this.leastBack) this.leastBack = back
    this.lateMs = Math.max(back - this.leastBack, this.lateMs - (now - this.lateAt))
    this.lateAt = now
    this.uplink.arrived(newly, spanMs)
    this.uplink.acknowledged(this.sender.transmittedBytes - acknowledged)
    this.sendWaiting()
    this.pace()
    return true
  }

  /** The allowance less what is still in flight, counting `sent` bytes as sent. */
  private headroomPast(sent: number): number {
    const unacknowledged = sent - this.sender.acknowledgedBytes
    const waiting = Math.min(unacknowledged, this.uplink.inUplink() + sent - this.sender.transmittedBytes)
    // Unarrived: what neither an ack nor the newest heartbeat says the server took, less what crosses while late.
    const late = Math.max(this.lateMs, this.downlink.peerQueueWait())
    const untaken = sent - Math.max(this.sender.acknowledgedBytes, this.sender.heartbeatBytes)
    const unarrived = untaken - this.uplink.uplinkRate() * late
    return this.allowance - Math.max(waiting, unarrived)
  }

  /** With no headroom and data in the uplink, looks again once the uplink should have carried what the server
   *  acknowledges at once (`WIRE_PIECES_ACK_BYTES`), or all it holds. */
  private pace(): void {
    const drainRate = this.uplink.drain.rate
    if (drainRate === 0 || this.headroomPastSent() > 0 || this.paceTimer !== null) return
    const queued = this.uplink.inUplink()
    if (queued === 0) return
    const ms = Math.max(1, Math.min(queued, WIRE_PIECES_ACK_BYTES) / drainRate)
    this.paceDue = performance.now() + ms
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.owner.onHeadroom(this)
    this.pace()
  }

  /** `PieceSenderOwner`: notes `bytes` the sender handed the socket. */
  handed(bytes: number): void {
    this.path.handed(this.sender.transmittedBytes, performance.now())
    this.uplink.handed(bytes)
  }
}
