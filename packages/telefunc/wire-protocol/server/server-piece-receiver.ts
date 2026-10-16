export { ServerPieceReceiver }

import { WIRE_PIECES_ACK_BYTES, WIRE_SEND_ACK_GAP_MS, WIRE_SEND_ACK_MIN_BYTES, WIRE_SEND_ACK_MS } from '../constants.js'
import { PieceReceiver } from '../pieces.js'

/** The server's: it acknowledges data soon, as the page's send budget waits on it, and in its answers. */
class ServerPieceReceiver extends PieceReceiver {
  /** Its side answers what arrived: the answer's message acknowledges the data it took, so the page's next request
   *  doesn't wait for the acknowledgement's timer. */
  answering(): void {
    if (this.took - this.acknowledgedTook >= WIRE_SEND_ACK_MIN_BYTES) this.acknowledgeWithNext()
  }

  protected tookFrame(_bytes: number, data: boolean, announced: boolean, at: number): void {
    this.date(data, at)
    this.acknowledgeTaken(data, announced)
  }

  protected tookPiece(_whole: boolean, data: boolean, announced: boolean): void {
    this.acknowledgeTaken(data, announced)
  }

  /** Acknowledges data within `WIRE_SEND_ACK_MS` once it took enough since the last acknowledgement, and soon an
   *  announced message or more than `WIRE_PIECES_ACK_BYTES`. */
  private acknowledgeTaken(data: boolean, announced: boolean): void {
    if (data && this.took - this.acknowledgedTook >= WIRE_SEND_ACK_MIN_BYTES) this.acknowledgeIn(WIRE_SEND_ACK_MS)
    if (announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES) this.acknowledgeSoon()
  }

  protected tookClose(): void {}

  protected ackWait(): number {
    return this.ackedAt + WIRE_SEND_ACK_GAP_MS - this.newestAt
  }
}
