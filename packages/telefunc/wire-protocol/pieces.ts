export { PieceSender, PieceAssembler, acknowledges }

import {
  WIRE_PIECE_BYTES,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PIECE_RATE_WINDOW_MS,
  WIRE_UNMEASURED_WHOLE_BYTES,
} from './constants.js'
import { assertProtocol, encode } from './shared-ws.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** Whether its receiver answers a frame of this size with a PIECES_ACK. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECE_BYTES
}

/** One WebSocket's: a frame that may take a ping interval to cross goes in pieces, so its receiver sees it arrive. */
class PieceSender {
  /** A frame this size or smaller goes whole. */
  private wholeUpTo = WIRE_UNMEASURED_WHOLE_BYTES
  /** The link is known to be too slow for big whole frames. */
  private slow = false
  private readonly unacknowledged: { bytes: number; at: number }[] = []
  /** In bytes per ms, from a frame sent to its acknowledgement: never faster than the frame crossed. */
  private fastest = 0
  private forget: ReturnType<typeof setTimeout> | null = null

  /** The pieces to send instead of `frame`, or null to send it whole. */
  pieces(frame: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer>[] | null {
    const bytes = frame.byteLength
    if (!acknowledges(bytes)) return null
    this.unacknowledged.push({ bytes, at: performance.now() })
    if (bytes <= this.wholeUpTo) return null
    // The first is empty: its receiver hears of the frame at once, before the first piece of it can cross.
    const pieces = [encode.piece(bytes, frame.subarray(0, 0))]
    for (let offset = 0; offset < bytes; offset += WIRE_PIECE_BYTES) {
      pieces.push(encode.piece(bytes, frame.subarray(offset, offset + WIRE_PIECE_BYTES)))
    }
    return pieces
  }

  /** Starts every frame over `WIRE_PIECE_BYTES` in pieces, until a measured rate says otherwise. */
  markSlow(): void {
    this.slow = true
    if (this.fastest === 0) this.wholeUpTo = WIRE_PIECE_BYTES
  }

  /** Whether the wire, which never measured a rate, holds a frame its receiver hasn't acknowledged. */
  get stalled(): boolean {
    return this.fastest === 0 && this.unacknowledged.length > 0
  }

  /** Returns false if it sent nothing left to acknowledge. */
  acknowledged(pingInterval: number): boolean {
    const sent = this.unacknowledged.shift()
    if (sent === undefined) return false
    const rate = sent.bytes / (performance.now() - sent.at)
    if (rate < this.fastest) return true
    this.fastest = rate
    this.wholeUpTo = Math.max(WIRE_PIECE_BYTES, (rate * pingInterval) / WIRE_PIECE_RATE_MARGIN)
    if (this.forget) clearTimeout(this.forget)
    this.forget = unrefTimer(
      setTimeout(() => {
        this.fastest = 0
        this.wholeUpTo = this.slow ? WIRE_PIECE_BYTES : WIRE_UNMEASURED_WHOLE_BYTES
      }, WIRE_PIECE_RATE_WINDOW_MS),
    )
    return true
  }
}

/** One WebSocket's: puts a frame sent in pieces back together, taking pieces only as `PieceSender` cuts them. */
class PieceAssembler {
  private pieces: Uint8Array[] = []
  private received = 0
  private total = 0

  /** Bytes it holds of a frame not yet whole. */
  get held(): number {
    return this.received
  }

  /** Returns the frame once its last piece arrived. */
  add(total: number, piece: Uint8Array): Uint8Array<ArrayBuffer> | null {
    if (this.received === 0) {
      this.total = total
      if (piece.byteLength === 0) return null
    }
    assertProtocol(
      total === this.total && piece.byteLength === Math.min(WIRE_PIECE_BYTES, total - this.received),
      'PIECE not cut as its frame is',
    )
    this.pieces.push(piece)
    this.received += piece.byteLength
    if (this.received < total) return null
    const frame = new Uint8Array(total)
    let offset = 0
    for (const part of this.pieces) {
      frame.set(part, offset)
      offset += part.byteLength
    }
    this.pieces = []
    this.received = 0
    return frame
  }
}
