export { PagePieceReceiver }

import { WIRE_PIECES_ACK_GAP_MS } from '../../constants.js'
import {
  PieceReceiver,
  acknowledges,
  type FrameArrival,
  type PieceArrival,
  type PieceReceiverOptions,
} from '../pieces.js'
import type { Uplink } from './Uplink.js'

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

  protected tookFrame({ bytes, data, at }: FrameArrival): void {
    if (!acknowledges(bytes)) return
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
    return Math.max(this.ackedAt + WIRE_PIECES_ACK_GAP_MS - this.newestAt, this.uplink.acks.wait())
  }
}
