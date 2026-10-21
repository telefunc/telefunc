export { SendBudget }

import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from '../../constants.js'
import { PieceSender, type PieceSenderOptions, type SendRoom } from '../pieces.js'
import type { Uplink } from './Uplink.js'
import type { Downlink } from './Downlink.js'
import { isSequencedTag, type PiecesAck } from '../../wire/connectionFrame.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

type SendBudgetOptions = Pick<PieceSenderOptions, 'transmit' | 'queued' | 'pingInterval'> & {
  uplink: Uplink
  downlink: Downlink
  /** Called when the timer finds the uplink drained some of what it held. */
  onRoom: () => void
}

/** The bytes of data a page lets its WebSocket's queues hold, so its window refreshes don't wait long behind them. */
class SendBudget implements SendRoom {
  readonly cuts = true
  /** Every frame the WebSocket carries goes through it. */
  readonly sender: PieceSender
  private readonly uplink: Uplink
  private readonly downlink: Downlink
  private readonly onRoom: () => void
  /** In ms, the server's clock from the first data an acknowledgement covered. */
  private arrivedAt = 0
  /** The least time an acknowledgement took back, up to the same offset: behind no queue of the uplink's. */
  private leastBack = Number.POSITIVE_INFINITY
  private lateMs = 0
  private lateAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  private paceDue = 0

  constructor({ transmit, queued, pingInterval, uplink, downlink, onRoom }: SendBudgetOptions) {
    this.uplink = uplink
    this.downlink = downlink
    this.onRoom = onRoom
    this.sender = new PieceSender({ transmit, queued, pingInterval, link: uplink.link, room: this })
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    if (isSequencedTag(frame[0]!)) {
      this.sender.send(frame)
      return this.pace()
    }
    // What it waits behind: the uplink holds it too once handed.
    const wait = this.uplink.waitNow()
    this.sender.send(frame)
    this.uplink.noteWait(wait)
  }

  /** What the server takes in `WIRE_QUEUE_DELAY_MS`, `WIRE_SEND_AHEAD_MIN_BYTES` at least. */
  private get allowance(): number {
    return Math.min(
      CREDIT_WINDOW_MAX_BYTES,
      Math.max(WIRE_SEND_AHEAD_MIN_BYTES, this.uplink.drain.rate * WIRE_QUEUE_DELAY_MS),
    )
  }

  /** `SendRoom`: the room past what the sender handed the socket. */
  dataRoom(): number {
    return this.roomPast(this.sender.transmittedBytes)
  }

  /** The room past all the sender was sent, what it holds included: none once the room is used or data waits for it. */
  roomPastSent(): number {
    return this.roomPast(this.sender.sentBytes)
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

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), and sends what it made room for. */
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
    this.sender.sendHeld()
    this.pace()
    return true
  }

  /** The room less what the sender handed past `sent` bytes. */
  private roomPast(sent: number): number {
    const unacknowledged = sent - this.sender.acknowledgedBytes
    const waiting = Math.min(unacknowledged, this.uplink.inUplink() + sent - this.sender.transmittedBytes)
    // Unarrived: what no ack or newest heartbeat says the server took, less what crosses while late.
    const late = Math.max(this.lateMs, this.downlink.peerQueueWait())
    const untaken = sent - Math.max(this.sender.acknowledgedBytes, this.sender.heartbeatBytes)
    const unarrived = untaken - this.uplink.uplinkRate() * late
    return this.allowance - Math.max(waiting, unarrived)
  }

  /** With no room and data in the uplink, looks again once it carried what the server acks at once, or all it holds. */
  private pace(): void {
    const drainRate = this.uplink.drain.rate
    if (drainRate === 0 || this.roomPastSent() > 0 || this.paceTimer !== null) return
    const queued = this.uplink.inUplink()
    if (queued === 0) return
    const ms = Math.max(1, Math.min(queued, WIRE_PIECES_ACK_BYTES) / drainRate)
    this.paceDue = performance.now() + ms
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.onRoom()
    this.pace()
  }

  /** `SendRoom`: notes `bytes` the sender handed the socket. */
  handed(bytes: number): void {
    this.uplink.handed(bytes)
  }
}
