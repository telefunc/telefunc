export { SendBudget }

import { CREDIT_WINDOW_MAX_BYTES, WIRE_QUEUE_DELAY_MS, WIRE_SEND_AHEAD_MIN_BYTES } from './constants.js'
import type { PieceSender } from './pieces.js'
import { isSequencedTag } from './shared-ws.js'

/** How much of the excess queue a round of acknowledgements sheds. */
const SHED_SHARE = 0.5

/** How many bytes of data a page lets its WebSocket's queues hold: the browser's, the network stack's and the path's,
 *  where it can't see them or reorder them, and where a flow-control frame waits behind the data sent before it. A
 *  download whose refreshes wait behind an upload crawls. The page holds data back once it has sent more than the
 *  allowance past what its server acknowledged, and sends the frames that don't wait at once.
 *
 *  The allowance is what keeps a data frame from waiting long in those queues. A PIECES_ACK says when the newest data
 *  frame it covers arrived, only as the µs after the one the ack before covered, so the clocks of the two ends need not
 *  agree: a frame's one-way delay is known up to an offset the same for all, and its wait is its delay less the least
 *  seen. The path back, which a download may fill, has no part in it. While the wait is under `WIRE_QUEUE_DELAY_MS` the
 *  allowance grows by the bytes each ack covers, less as the wait nears it; past it, the allowance falls by the excess at the
 *  rate the server took. */
class SendBudget {
  private allowance = WIRE_SEND_AHEAD_MIN_BYTES
  /** The data frames sent and not covered by an acknowledgement: the bytes sent through each, and when. */
  private readonly sent: { end: number; at: number }[] = []
  /** In ms, the server's clock from the first data frame an acknowledgement covered. */
  private arrivedAt = 0
  /** The least one-way delay of a data frame, up to the offset between the clocks. */
  private leastDelay = Number.POSITIVE_INFINITY

  constructor(private readonly sender: PieceSender) {}

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    this.sender.send(frame, pingInterval)
    if (!isSequencedTag(frame[0]!)) return
    // Frames sent within a ms of one another make one sample, dated by the first.
    const end = this.sender.sentBytes
    const at = performance.now()
    const last = this.sent.at(-1)
    if (last !== undefined && at - last.at < 1) last.end = end
    else this.sent.push({ end, at })
  }

  /** The bytes of data it takes now: none, once the allowance is used. */
  get room(): number {
    return this.allowance - (this.sender.sentBytes - this.sender.acknowledgedBytes)
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`). */
  acknowledged(bytes: number, heldMs: number, spanUs: number): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(bytes, heldMs)) return false
    const acknowledged = this.sender.acknowledgedBytes
    const newly = acknowledged - before
    this.arrivedAt += spanUs / 1000
    let covered = 0
    while (covered < this.sent.length && this.sent[covered]!.end <= acknowledged) covered++
    // It covered no data frame.
    if (covered === 0) return true
    const newest = this.sent[covered - 1]!
    this.sent.splice(0, covered)
    const delay = this.arrivedAt - newest.at
    // A delay that stays over the target with the least allowance is the path's: it rose.
    const atLeast = this.allowance === WIRE_SEND_AHEAD_MIN_BYTES
    if (delay < this.leastDelay || (atLeast && delay - this.leastDelay >= WIRE_QUEUE_DELAY_MS)) this.leastDelay = delay
    const wait = delay - this.leastDelay
    if (wait < WIRE_QUEUE_DELAY_MS) {
      this.allowance += (newly * (WIRE_QUEUE_DELAY_MS - wait)) / WIRE_QUEUE_DELAY_MS
    } else if (spanUs > 0) {
      const rate = newly / (spanUs / 1000)
      this.allowance -= (SHED_SHARE * rate * (wait - WIRE_QUEUE_DELAY_MS) * newly) / this.allowance
    }
    this.allowance = Math.min(CREDIT_WINDOW_MAX_BYTES, Math.max(WIRE_SEND_AHEAD_MIN_BYTES, this.allowance))
    return true
  }
}
