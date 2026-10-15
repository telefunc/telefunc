export { PagePieceReceiver }

import { WIRE_PIECES_ACK_GAP_MS } from '../constants.js'
import { PieceReceiver, acknowledges } from '../pieces.js'
import type { Uplink } from './uplink.js'

/** The page's: it acknowledges frames over `WIRE_PIECES_ACK_BYTES` and announcements' close, within its uplink's share. */
class PagePieceReceiver extends PieceReceiver {
  constructor(
    send: (frame: Uint8Array<ArrayBuffer>) => void,
    carry: (frame: () => Uint8Array<ArrayBuffer> | null) => void,
    private readonly uplink: Uplink,
  ) {
    super(send, carry)
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
