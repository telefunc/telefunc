export { SendBudget }

import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_ACK_MIN_BYTES,
  WIRE_SEND_ACK_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from './constants.js'
import type { Link } from './link.js'
import { PieceSender } from './pieces.js'
import { isSequencedTag } from './shared-ws.js'
import { Fifo } from '../utils/Fifo.js'
import { DOUBLE_ZERO } from '../utils/DOUBLE_ZERO.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** How much of the excess queue a round of acknowledgements sheds. */
const SHED_SHARE = 0.5

/** How many bytes of data a page lets its WebSocket's queues hold: the browser's, the network stack's and the path's,
 *  where it can't see them or reorder them, and where a flow-control frame waits behind the data sent before it. A
 *  download whose refreshes wait behind an upload crawls. The page hands data to the socket while the allowance has
 *  room, and holds the rest back: its piece sender cuts a frame past the room into pieces as the room opens, and the
 *  frames that don't wait pass it.
 *
 *  The room is the allowance less what the uplink holds, or less what the server hasn't acknowledged where that is more,
 *  but for what late acknowledgements hide and what the server takes before it acknowledges. An acknowledgement waits
 *  behind what the server sends, as long as what the download's credit lets the server have sent takes to arrive (see
 *  `Link.peerQueueWait`). What the uplink holds is every byte handed to the socket, headers included, drained at the rate
 *  the server takes it (see `Link`). While the room is used up and the uplink holds data, a timer looks again as it
 *  drains. The frames that go at once wait behind what it holds, and a page's download window covers that wait: the
 *  budget notes it on the link as the uplink's model says when each is handed, and as the server's acknowledgements
 *  measure it for data.
 *
 *  The allowance is what keeps data from waiting long in those queues. A PIECES_ACK says when the newest data it covers
 *  arrived, only as the µs after the data the ack before covered, so the clocks of the two ends need not agree: data's
 *  one-way delay is known up to an offset the same for all. Less what the uplink held ahead of it and its own bytes'
 *  time on the link, it is the path's, and its wait is its delay less the least of those. While the wait is under
 *  `WIRE_QUEUE_DELAY_MS` the allowance grows by the bytes each ack covers, less as the wait nears it; past it, the
 *  allowance falls by the excess at the rate the server took. It is at least what the server takes in
 *  `WIRE_QUEUE_DELAY_MS`, within what keeps it acknowledging, and starts there: on a slow uplink, what it would hold more
 *  takes seconds to shed. */
class SendBudget {
  /** Every frame the WebSocket carries goes through it. */
  readonly sender: PieceSender
  /** The allowance as measured delays moved it, null before the first: till then it is the least. */
  private movedAllowance: number | null = null
  /** The data handed to the socket and not covered by an acknowledgement: where each part handed at once ends, of all
   *  the sender handed, its bytes, what the uplink held ahead of it, and when. */
  private readonly sent = new Fifo<{ end: number; bytes: number; ahead: number; at: number }>()
  /** In ms, the server's clock from the first data an acknowledgement covered. */
  private arrivedAt = DOUBLE_ZERO
  /** The least one-way delay of data on the path, up to the offset between the clocks. */
  private leastDelay = Number.POSITIVE_INFINITY
  /** The least time an acknowledgement took back, up to the same offset, and how much longer the newest took: behind
   *  what the server sends, which a download fills, and no queue of the uplink's. Its wait falls no faster than time
   *  passes. */
  private leastBack = Number.POSITIVE_INFINITY
  private lateMs = DOUBLE_ZERO
  private lateAt = DOUBLE_ZERO
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  private paceDue = DOUBLE_ZERO
  /** Called when the timer finds the uplink drained some of what it held. */
  onRoom: () => void = () => {}

  /** `transmit` and `queued` as `PieceSender`'s. */
  constructor(
    transmit: (message: Uint8Array<ArrayBuffer>) => void,
    queued: () => number,
    private readonly link: Link,
  ) {
    this.sender = new PieceSender(transmit, queued, link, {
      dataRoom: () => this.roomPast(this.sender.transmittedBytes),
      handed: (bytes, data) => this.handed(bytes, data),
    })
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    if (isSequencedTag(frame[0]!)) {
      this.sender.send(frame, pingInterval)
      return this.pace()
    }
    // What it waits behind: the uplink holds it too once handed.
    const wait = this.link.waitNow()
    this.sender.send(frame, pingInterval)
    this.link.noteWait(wait)
  }

  private get allowance(): number {
    return this.movedAllowance ?? this.least
  }

  /** The bytes of data it takes now: none, once the room is used or data waits for it. */
  get room(): number {
    return this.roomPast(this.sender.sentBytes)
  }

  /** Sends what the sender holds for room, as far as it goes now. */
  sendHeld(): void {
    this.sender.sendHeld()
  }

  /** A message arrived: where the timer is late to look again, looks now. A hidden tab throttles timers, not messages. */
  received(): void {
    if (this.paceTimer === null || performance.now() < this.paceDue) return
    clearTimeout(this.paceTimer)
    this.lookAgain()
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), and sends what it made room for. */
  acknowledged(bytes: number, heldMs: number, spanUs: number): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(bytes, heldMs, spanUs)) return false
    const acknowledged = this.sender.acknowledgedBytes
    const newly = acknowledged - before
    const spanMs = spanUs / 1000
    this.arrivedAt += spanMs
    const now = performance.now()
    const back = now - this.arrivedAt - heldMs
    if (back < this.leastBack) this.leastBack = back
    this.lateMs = Math.max(back - this.leastBack, this.lateMs - (now - this.lateAt))
    this.lateAt = now
    this.link.arrived(newly, spanMs)
    let newest: { end: number; bytes: number; ahead: number; at: number } | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    this.link.acknowledged(this.sender.transmittedBytes - acknowledged)
    if (newest !== undefined) this.measure(newest, newly, spanUs)
    this.sendHeld()
    this.pace()
    return true
  }

  /** The room less what the sender handed past `sent` bytes. */
  private roomPast(sent: number): number {
    const unacknowledged = sent - this.sender.acknowledgedBytes
    const waiting = Math.min(unacknowledged, this.link.inUplink() + sent - this.sender.transmittedBytes)
    // Up to `slack` arrived unacknowledged. More arrives while an acknowledgement comes back, or would wait behind what
    // the server sends now.
    const late = Math.max(this.lateMs, this.link.peerQueueWait())
    const unarrived = unacknowledged - this.link.uplinkRate() * late - this.slack
    return this.allowance - Math.max(waiting, unarrived)
  }

  /** While the room is used up and the uplink holds data, the uplink sets the pace, and data may wait for room: looks
   *  again once the uplink carried what its server acknowledges at once, or all it holds. */
  private pace(): void {
    const { drainRate } = this.link
    if (drainRate === 0 || this.room > 0 || this.paceTimer !== null) return
    const queued = this.link.inUplink()
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

  /** Notes `bytes` the sender handed the socket: all are in the uplink, and data is dated. Parts of data handed within a
   *  ms of one another make one sample, dated by the first. */
  private handed(bytes: number, data: boolean): void {
    const ahead = this.link.handed(bytes)
    if (!data) return
    const end = this.sender.transmittedBytes
    const at = performance.now()
    const last = this.sent.peekLast()
    if (last !== undefined && at - last.at < 1) {
      last.bytes += bytes
      last.end = end
    } else this.sent.push({ end, bytes, ahead, at })
  }

  /** Moves the allowance by the delay of `newest`, the newest data an acknowledgement of `newly` bytes covered, `spanUs`
   *  after the data the one before covered. */
  private measure(
    newest: { end: number; bytes: number; ahead: number; at: number },
    newly: number,
    spanUs: number,
  ): void {
    const delay = this.arrivedAt - newest.at
    const { drainRate } = this.link
    const path = drainRate > 0 ? delay - (newest.ahead + newest.bytes) / drainRate : delay
    // A path that stays over the target with the least allowance rose.
    const least = this.least
    let allowance = this.allowance
    const atLeast = allowance === least
    if (path < this.leastDelay || (atLeast && path - this.leastDelay >= WIRE_QUEUE_DELAY_MS)) this.leastDelay = path
    const wait = delay - this.leastDelay
    this.link.noteWait(Math.max(0, wait))
    if (wait < WIRE_QUEUE_DELAY_MS) {
      allowance += (newly * (WIRE_QUEUE_DELAY_MS - wait)) / WIRE_QUEUE_DELAY_MS
    } else if (spanUs > 0) {
      const rate = newly / (spanUs / 1000)
      allowance -= (SHED_SHARE * rate * (wait - WIRE_QUEUE_DELAY_MS) * newly) / allowance
    }
    this.movedAllowance = Math.min(CREDIT_WINDOW_MAX_BYTES, Math.max(least, allowance))
  }

  /** The least allowance: what the server takes in `WIRE_QUEUE_DELAY_MS`, more than it takes before it acknowledges,
   *  and `WIRE_SEND_AHEAD_MIN_BYTES` at most or before its rate is measured. */
  private get least(): number {
    const { drainRate } = this.link
    if (drainRate === 0) return WIRE_SEND_AHEAD_MIN_BYTES
    return Math.min(WIRE_SEND_AHEAD_MIN_BYTES, Math.max(this.slack + 1024, drainRate * WIRE_QUEUE_DELAY_MS))
  }

  /** What the server may have taken before it acknowledges: `WIRE_PIECES_ACK_BYTES` at most, and where less,
   *  `WIRE_SEND_ACK_MIN_BYTES` and what the uplink carries in `WIRE_SEND_ACK_MS` after them. */
  private get slack(): number {
    const { drainRate } = this.link
    if (drainRate === 0) return WIRE_PIECES_ACK_BYTES
    return Math.min(WIRE_PIECES_ACK_BYTES, WIRE_SEND_ACK_MIN_BYTES + drainRate * WIRE_SEND_ACK_MS)
  }
}
