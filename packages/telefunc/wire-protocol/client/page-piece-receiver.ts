export { PagePieceReceiver }

import { WIRE_PIECES_ACK_GAP_MS } from '../constants.js'
import { PieceReceiver, acknowledges, type PieceReceiverOptions } from '../pieces.js'
import type { Uplink } from './uplink.js'

type PagePieceReceiverOptions = PieceReceiverOptions & {
  uplink: Uplink
}

/** The page's `PieceReceiver`, which puts a WebSocket frame sent in pieces back together. It acknowledges each frame
 *  over `WIRE_PIECES_ACK_BYTES` and the ANNOUNCE that closes an announcement,
 *  at most every `WIRE_PIECES_ACK_GAP_MS`. */
class PagePieceReceiver extends PieceReceiver {
  private readonly uplink: Uplink

  constructor(options: PagePieceReceiverOptions) {
    super(options)
    this.uplink = options.uplink
  }

  protected tookFrame(bytes: number, data: boolean, _announced: boolean, at: number): void {
    if (!acknowledges(bytes)) return
    this.date(data, at)
    this.acknowledgeWithNextSoon()
  }

  protected tookPiece(whole: boolean): void {
    if (whole && acknowledges(this.total)) this.acknowledgeWithNextSoon()
  }

  protected tookClose(at: number): void {
    this.date(false, at)
    this.acknowledgeSoon()
  }

  protected ackWait(): number {
    return Math.max(this.ackedAt + WIRE_PIECES_ACK_GAP_MS - this.newestAt, this.uplink.acks.wait())
  }
}
