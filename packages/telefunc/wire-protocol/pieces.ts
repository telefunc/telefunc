export { PieceSender, PieceReceiver }

import {
  WIRE_PIECE_BYTES,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PIECE_RATE_WINDOW_MS,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_UNMEASURED_WHOLE_BYTES,
} from './constants.js'
import { assertProtocol, encode } from './shared-ws.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** Whether its receiver acknowledges a frame of this size. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECE_BYTES
}

/** One WebSocket's: a frame that may take a ping interval to cross goes in pieces, so its receiver sees it arrive.
 *  None of its messages is over `wholeUpTo`. */
class PieceSender {
  /** A frame this size or smaller goes whole, a larger one in pieces this size: what the fastest measured rate carries
   *  in a ping interval over `WIRE_PIECE_RATE_MARGIN`; with none, `WIRE_UNMEASURED_WHOLE_BYTES`, or `WIRE_PIECE_BYTES`
   *  on a link known slow. */
  private wholeUpTo = WIRE_UNMEASURED_WHOLE_BYTES
  /** The link is known to be too slow for big whole frames. */
  private slow = false
  /** The frames over `WIRE_PIECE_BYTES` sent and not yet acknowledged, each with the bytes that crossed before it
   *  arrived, and how many were acknowledged before them. */
  private readonly unacknowledged: { crossing: number; at: number }[] = []
  private acknowledgedCount = 0
  private largest = 0
  /** In bytes per ms, from a frame sent to its arrival: never faster than it and what was queued ahead of it crossed. */
  private fastest = 0
  private roseAt = 0

  /** The pieces to send instead of `frame`, or null to send it whole. `queued`: the bytes its socket holds unsent. */
  pieces(frame: Uint8Array<ArrayBuffer>, queued: number): Uint8Array<ArrayBuffer>[] | null {
    const bytes = frame.byteLength
    if (bytes > this.largest) this.largest = bytes
    if (!acknowledges(bytes)) return null
    const now = performance.now()
    if (this.fastest > 0 && now - this.roseAt > WIRE_PIECE_RATE_WINDOW_MS) {
      this.fastest = 0
      this.wholeUpTo = this.slow ? WIRE_PIECE_BYTES : WIRE_UNMEASURED_WHOLE_BYTES
    }
    // What its socket holds unsent crosses before it; a browser may count the message it is sending in full, so less the
    // largest frame
    this.unacknowledged.push({ crossing: Math.max(0, queued - this.largest) + bytes, at: now })
    if (bytes <= this.wholeUpTo) return null
    const size = Math.floor(this.wholeUpTo)
    // The first is empty: its receiver hears of the frame at once, before the first piece of it can cross.
    const pieces = [encode.piece(bytes, frame.subarray(0, 0))]
    for (let offset = 0; offset < bytes; offset += size) {
      pieces.push(encode.piece(bytes, frame.subarray(offset, offset + size)))
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

  /** Takes a PIECES_ACK: its receiver took `count` frames in all, the newest `heldMs` before it sent the ack. Returns
   *  false if that acknowledges nothing sent, or more than was sent. */
  acknowledged(count: number, heldMs: number, pingInterval: number): boolean {
    const newly = count - this.acknowledgedCount
    if (newly < 1 || newly > this.unacknowledged.length) return false
    this.acknowledgedCount = count
    const arrived = performance.now() - heldMs
    for (const sent of this.unacknowledged.splice(0, newly)) {
      const rate = sent.crossing / (arrived - sent.at)
      if (!(rate > this.fastest)) continue
      this.fastest = rate
      this.roseAt = arrived
      this.wholeUpTo = Math.max(WIRE_PIECE_BYTES, (rate * pingInterval) / WIRE_PIECE_RATE_MARGIN)
    }
    return true
  }
}

/** One WebSocket's: puts a frame sent in pieces back together, taking pieces only as `PieceSender` cuts them (all as
 *  large as the first, at least `WIRE_PIECE_BYTES` or the whole frame, but the last), and
 *  acknowledges the frames over `WIRE_PIECE_BYTES` it took, at most one PIECES_ACK per `WIRE_PIECES_ACK_GAP_MS`. */
class PieceReceiver {
  private pieces: Uint8Array[] = []
  private received = 0
  private total = 0
  private size = 0
  private took = 0
  private ackedUpTo = 0
  private newestAt = 0
  private ackedAt = Number.NEGATIVE_INFINITY
  private ackTimer: ReturnType<typeof setTimeout> | null = null

  constructor(private readonly send: (frame: Uint8Array<ArrayBuffer>) => void) {}

  /** After a whole frame of `bytes` arrived, in one message or in pieces. */
  arrived(bytes: number): void {
    if (!acknowledges(bytes)) return
    this.took++
    this.newestAt = performance.now()
    const wait = this.ackedAt + WIRE_PIECES_ACK_GAP_MS - this.newestAt
    if (wait <= 0) this.acknowledge()
    else this.ackTimer ??= unrefTimer(setTimeout(() => this.acknowledge(), wait))
  }

  private acknowledge(): void {
    if (this.ackTimer !== null) clearTimeout(this.ackTimer)
    this.ackTimer = null
    if (this.ackedUpTo === this.took) return
    this.ackedAt = performance.now()
    this.ackedUpTo = this.took
    // Rounded down: its sender then dates the arrival no earlier than it was, so the rate it takes is never higher
    this.send(encode.piecesAck(this.took, Math.floor(this.ackedAt - this.newestAt)))
  }

  /** Bytes it holds of a frame not yet whole. */
  get held(): number {
    return this.received
  }

  /** Returns the frame once its last piece arrived. */
  add(total: number, piece: Uint8Array): Uint8Array<ArrayBuffer> | null {
    if (this.received === 0) {
      this.total = total
      if (piece.byteLength === 0) return null
      this.size = piece.byteLength
    }
    assertProtocol(
      total === this.total &&
        this.size >= Math.min(WIRE_PIECE_BYTES, total) &&
        piece.byteLength === Math.min(this.size, total - this.received),
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
