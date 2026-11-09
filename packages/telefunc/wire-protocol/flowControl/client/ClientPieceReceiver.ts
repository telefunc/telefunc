export { ClientPieceReceiver }

import { WIRE_PIECES_ACK_GAP_MS } from '../../constants.js'
import { PieceReceiver, acknowledges, type FrameArrival, type PieceArrival } from '../pieces.js'

/** The page's `PieceReceiver`, which puts a WebSocket frame sent in pieces back together. It acknowledges each frame
 *  over `WIRE_PIECES_ACK_BYTES` and the ANNOUNCE that closes an announcement, and each data frame, so that the server
 *  measures its path whatever their size, at most every `WIRE_PIECES_ACK_GAP_MS`: in the page's next message, or alone
 *  a gap later. */
class ClientPieceReceiver extends PieceReceiver {
  protected tookFrame({ bytes, data, at }: FrameArrival): void {
    if (!data && !acknowledges(bytes)) return
    this.date(data, at)
    this.acknowledgeWithNextSoon()
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
}
