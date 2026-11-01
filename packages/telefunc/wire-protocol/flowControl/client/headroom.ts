export { ClientHeadroom }
export type { ClientHeadroomOwner }

import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from '../../constants.js'
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

/** The bytes of data a page lets its WebSocket's queues hold, so its window refreshes don't wait long behind them. */
class ClientHeadroom implements PieceSenderOwner, PieceReceiverOwner {
  /** Every frame the WebSocket carries goes through it. */
  readonly sender: PieceSender
  private readonly uplink: Uplink
  private readonly downlink: Downlink
  private readonly owner: ClientHeadroomOwner
  /** In ms, the server's clock from the first data an acknowledgement covered. */
  private arrivedAt = 0
  /** The least time an acknowledgement took back, up to the same offset: behind no queue of the uplink's. */
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
    // What it waits behind: the uplink holds it too once handed.
    const wait = this.uplink.waitNow()
    this.sender.send(frame, 0)
    this.uplink.noteWait(wait)
  }

  /** `PieceReceiverOwner`: the wire's receiver acknowledges through its sender, past the headroom. */
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

  /** What the server takes in `WIRE_QUEUE_DELAY_MS`, `WIRE_SEND_AHEAD_MIN_BYTES` at least. */
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

  /** The headroom past all the sender was sent, what waits in it included: none once used or data waits for it. */
  headroomPastSent(): number {
    return this.headroomPast(this.sender.sentBytes)
  }

  /** Its socket closed: it hands it nothing more. */
  close(): void {
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
    this.sender.close()
  }

  /** A message arrived: where the timer is late to look again, looks now. A hidden tab throttles timers, not messages. */
  received(): void {
    if (this.paceTimer === null || performance.now() < this.paceDue) return
    clearTimeout(this.paceTimer)
    this.lookAgain()
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), and sends what it made headroom for. */
  acknowledged(ack: PiecesAck): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(ack)) return false
    const acknowledged = this.sender.acknowledgedBytes
    const newly = acknowledged - before
    const spanMs = ack.spanUs / 1000
    this.arrivedAt += spanMs
    const now = performance.now()
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

  /** The headroom less what the sender handed past `sent` bytes. */
  private headroomPast(sent: number): number {
    const unacknowledged = sent - this.sender.acknowledgedBytes
    const waiting = Math.min(unacknowledged, this.uplink.inUplink() + sent - this.sender.transmittedBytes)
    // Unarrived: what no ack or newest heartbeat says the server took, less what crosses while late.
    const late = Math.max(this.lateMs, this.downlink.peerQueueWait())
    const untaken = sent - Math.max(this.sender.acknowledgedBytes, this.sender.heartbeatBytes)
    const unarrived = untaken - this.uplink.uplinkRate() * late
    return this.allowance - Math.max(waiting, unarrived)
  }

  /** With no headroom and data in the uplink, looks again once it carried what the server acks at once, or all it holds. */
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
    this.uplink.handed(bytes)
  }
}
