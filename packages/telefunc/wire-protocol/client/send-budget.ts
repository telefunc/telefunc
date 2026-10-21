export { SendBudget }

import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_ACK_MIN_BYTES,
  WIRE_SEND_ACK_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from '../constants.js'
import { PieceSender, type PieceSenderOptions, type SendRoom } from '../pieces.js'
import type { Uplink } from './uplink.js'
import type { Downlink } from './downlink.js'
import { isSequencedTag, type PiecesAck } from '../shared-ws.js'
import { Fifo } from '../../utils/Fifo.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

/** How much of the excess queue a round of acknowledgements sheds. */
const SHED_SHARE = 0.5
/** The least allowance passes what the server takes before it acknowledges by this much at least. */
const LEAST_OVER_SLACK_BYTES = 1024
/** Parts of data handed within this long of one another make one sample. */
const SAMPLE_MERGE_MS = 1

/** Data handed to the socket at once; `ahead` is what the uplink held ahead of it. */
type SentData = { end: number; bytes: number; ahead: number; at: number }

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
  /** The allowance as measured delays moved it, null before the first: till then it is the least. */
  private movedAllowance: number | null = null
  /** The data handed to the socket and not covered by an acknowledgement. */
  private readonly sent = new Fifo<SentData>()
  /** In ms, the server's clock from the first data an acknowledgement covered. */
  private arrivedAt = 0
  /** The least one-way delay of data on the path, up to the offset between the clocks. */
  private leastDelay = Number.POSITIVE_INFINITY
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

  private get allowance(): number {
    return this.movedAllowance ?? this.least
  }

  /** `SendRoom`: the room less what the sender handed. */
  dataRoom(): number {
    return this.roomPast(this.sender.transmittedBytes)
  }

  /** `SendRoom`: a PIECES_ACK or a window refresh takes its share of the uplink. */
  sentFrame(tag: number, bytes: number): void {
    this.uplink.sentFrame(tag, bytes)
  }

  /** The bytes of data it takes now: none, once the room is used or data waits for it. */
  get sendRoom(): number {
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
    let newest: SentData | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    this.uplink.acknowledged(this.sender.transmittedBytes - acknowledged)
    if (newest !== undefined) this.measure(newest, newly, spanMs)
    this.sender.sendHeld()
    this.pace()
    return true
  }

  /** The room less what the sender handed past `sent` bytes. */
  private roomPast(sent: number): number {
    const unacknowledged = sent - this.sender.acknowledgedBytes
    const waiting = Math.min(unacknowledged, this.uplink.inUplink() + sent - this.sender.transmittedBytes)
    // Unarrived: what no ack or newest heartbeat says the server took, less `slack` and what crosses while late.
    const late = Math.max(this.lateMs, this.downlink.peerQueueWait())
    const untaken = sent - Math.max(this.sender.acknowledgedBytes, this.sender.heartbeatBytes)
    const unarrived = untaken - this.uplink.uplinkRate() * late - this.slack
    return this.allowance - Math.max(waiting, unarrived)
  }

  /** With no room and data in the uplink, looks again once it carried what the server acks at once, or all it holds. */
  private pace(): void {
    const { drainRate } = this.uplink
    if (drainRate === 0 || this.sendRoom > 0 || this.paceTimer !== null) return
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

  /** `SendRoom`: notes `bytes` the sender handed the socket; data handed within `SAMPLE_MERGE_MS` makes one sample. */
  handed(bytes: number, data: boolean): void {
    const ahead = this.uplink.handed(bytes)
    if (!data) return
    const end = this.sender.transmittedBytes
    const at = performance.now()
    const last = this.sent.peekLast()
    if (last !== undefined && at - last.at < SAMPLE_MERGE_MS) {
      last.bytes += bytes
      last.end = end
    } else this.sent.push({ end, bytes, ahead, at })
  }

  /** Moves the allowance by the delay of `newest`, the newest data an acknowledgement of `newly` bytes covered. */
  private measure(newest: SentData, newly: number, spanMs: number): void {
    const delay = this.arrivedAt - newest.at
    const { drainRate } = this.uplink
    const path = drainRate > 0 ? delay - (newest.ahead + newest.bytes) / drainRate : delay
    // A path that stays over the target with the least allowance rose.
    const least = this.least
    let allowance = this.allowance
    const atLeast = allowance === least
    if (path < this.leastDelay || (atLeast && path - this.leastDelay >= WIRE_QUEUE_DELAY_MS)) this.leastDelay = path
    const wait = delay - this.leastDelay
    this.uplink.noteWait(Math.max(0, wait))
    if (wait < WIRE_QUEUE_DELAY_MS) {
      allowance += (newly * (WIRE_QUEUE_DELAY_MS - wait)) / WIRE_QUEUE_DELAY_MS
    } else if (spanMs > 0) {
      const rate = newly / spanMs
      allowance -= (SHED_SHARE * rate * (wait - WIRE_QUEUE_DELAY_MS) * newly) / allowance
    }
    this.movedAllowance = Math.min(CREDIT_WINDOW_MAX_BYTES, Math.max(least, allowance))
  }

  /** The least allowance: what the server takes in `WIRE_QUEUE_DELAY_MS`, more than it takes before it acknowledges. */
  private get least(): number {
    const { drainRate } = this.uplink
    if (drainRate === 0) return WIRE_SEND_AHEAD_MIN_BYTES
    return Math.min(
      WIRE_SEND_AHEAD_MIN_BYTES,
      Math.max(this.slack + LEAST_OVER_SLACK_BYTES, drainRate * WIRE_QUEUE_DELAY_MS),
    )
  }

  /** What the server may have taken before it acknowledges: `WIRE_PIECES_ACK_BYTES` at most. */
  private get slack(): number {
    const { drainRate } = this.uplink
    if (drainRate === 0) return WIRE_PIECES_ACK_BYTES
    return Math.min(WIRE_PIECES_ACK_BYTES, WIRE_SEND_ACK_MIN_BYTES + drainRate * WIRE_SEND_ACK_MS)
  }
}
