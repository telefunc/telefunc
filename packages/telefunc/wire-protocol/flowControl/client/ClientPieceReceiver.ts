export { ClientPieceReceiver }

import { WIRE_PIECES_ACK_GAP_MS, WIRE_UNREPORTED_MAX_BYTES } from '../../constants.js'
import { TOOK_TAGS, withTook } from '../../wire/frame.js'
import { PieceReceiver, acknowledges, type FrameArrival, type PieceArrival } from '../pieces.js'

/** The page's `PieceReceiver`, which puts a WebSocket frame sent in pieces back together. It acknowledges each frame
 *  over `WIRE_PIECES_ACK_BYTES` and the ANNOUNCE that closes an announcement, at most every `WIRE_PIECES_ACK_GAP_MS`.
 *  What else it took rides the frames the page sends anyway (`report`), unless more than `WIRE_UNREPORTED_MAX_BYTES`
 *  waits for one: then it acknowledges that too. */
class ClientPieceReceiver extends PieceReceiver {
  /** What the page last said it took, in a frame `report` stamped. */
  private reportedTook = 0

  protected tookFrame({ bytes, data, at }: FrameArrival): void {
    this.date(data, at)
    const unreported = this.took - Math.max(this.acknowledgedTook, this.reportedTook)
    if (acknowledges(bytes) || unreported > WIRE_UNREPORTED_MAX_BYTES) this.acknowledgeWithNextSoon()
  }

  protected tookPiece({ whole }: PieceArrival): void {
    if (whole && acknowledges(this.total)) this.acknowledgeWithNextSoon()
  }

  protected tookClose(at: number): void {
    this.date(false, at)
    this.acknowledgeSoon()
  }

  protected ackWait(): number {
    return this.ackedAt + WIRE_PIECES_ACK_GAP_MS - this.newestAt
  }

  /** `frame`, saying what the page took if it is a frame that can (`tookOf`): the server frees its copies of that. */
  report(frame: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
    if (!TOOK_TAGS.has(frame[0]!)) return frame
    this.reportedTook = this.took
    return withTook(frame, this.took)
  }
}
