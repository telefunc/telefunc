export { ServerHeadroom }

import { WIRE_PIECES_ACK_BYTES, WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS } from '../../constants.js'
import { DrainRate } from '../DrainRate.js'
import { PieceRate } from '../PieceRate.js'
import {
  PieceSender,
  acknowledges,
  type PieceSenderOptions,
  type PieceSenderOwner,
  type PieceSocket,
} from '../pieces.js'
import type { PiecesAck } from '../../wire/frame.js'
import { isSequencedTag } from '../../wire/tags.js'
import { countWaitingPublishes, droppingPublishes, type WireQueue } from './waitingPublishes.js'
import { Fifo } from '../../../utils/Fifo.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

type ServerHeadroomOptions = Pick<PieceSenderOptions, 'socket' | 'pingInterval'>

/** The path to the page holds what `headroom` allows; the rest waits in its sender. */
class ServerHeadroom implements PieceSenderOwner, WireQueue {
  /** Sends every frame the WebSocket carries. */
  readonly sender: PieceSender
  private readonly socket: PieceSocket
  /** The rate the page takes data at, as acknowledgements measure it. */
  private readonly drain = new DrainRate()
  /** The rate the probe of the wire's first large frame measured, in a round trip: that frame's acknowledgement comes
   *  only when all of it arrived, which on a slow link is seconds of publishes later. */
  private readonly pieceRate = new PieceRate()
  /** When the newest acknowledged data arrived, in ms on the page's clock, counted from the first data an
   *  acknowledgement covered. */
  private arrivedAt = 0
  /** The shortest time, in ms, an acknowledgement took to come back, up to the clocks' offset: one that waited behind
   *  no queue on the path. */
  private leastBack = Number.POSITIVE_INFINITY
  /** The least time from data handed to the acknowledgement that covered it, `Infinity` before one. */
  private leastRtt = Number.POSITIVE_INFINITY
  /** The data handed and not covered by an acknowledgement: where it ends, of all the sender handed, and when. */
  private readonly sent = new Fifo<{ end: number; at: number }>()
  /** Where the newest data handed with nothing waiting ahead ends: arrivals up to it came at the app's rate. */
  private appLimitedTo = 0
  /** Whether the sender is sending what waited for headroom. */
  private releasing = false
  /** Bytes handed since the newest data: an in-order socket holding no more holds none of it, bar what is carried. */
  private handedSinceData = 0
  /** What the path holds of what the sender handed, as of `inPathAt`: the rate drains it, as `Uplink.inUplink`. */
  private inPath = 0
  private inPathAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null

  constructor({ socket, pingInterval }: ServerHeadroomOptions) {
    this.socket = socket
    this.sender = new PieceSender({
      socket,
      pingInterval,
      pieceRate: this.pieceRate,
      cuts: 'whole',
      owner: this,
    })
  }

  /** Sends `frame`, sent at `at`, through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, at: number): void {
    this.sender.send(frame, isSequencedTag(frame[0]!) && !this.sender.dataWaits ? this.headroom(at) : 0)
    this.pace()
  }

  waitingPublishes(ix: number): number {
    return countWaitingPublishes(this.sender.waitingFrames, ix)
  }

  dropWaitingPublishes(ix: number, count: number): void {
    this.sender.dropWaiting(droppingPublishes(ix, count))
  }

  drainRate(): number {
    return this.drain.rate || this.pieceRate.fastest
  }

  /** What the path takes at `at` past what it holds. */
  headroom(at: number): number {
    const rate = this.drainRate()
    if (rate === 0) return Number.POSITIVE_INFINITY
    const roundTrips = this.leastRtt === Number.POSITIVE_INFINITY ? 0 : WIRE_SEND_AHEAD_ROUND_TRIPS * this.leastRtt
    return Math.max(WIRE_SEND_AHEAD_MIN_BYTES, rate * roundTrips) - this.pathHolds(at)
  }

  /** `PieceSenderOwner`: the path holds `bytes`; each dated part is dated apart, so a burst's first part measures the path. */
  handed(bytes: number, data: boolean): void {
    const now = performance.now()
    this.inPath = this.pathHolds(now) + bytes
    this.inPathAt = now
    if (!data) {
      this.handedSinceData += bytes
      return
    }
    const end = this.sender.transmittedBytes
    if (!this.releasing && this.socket.bufferedAmount <= this.handedSinceData && !this.sender.dataWaits)
      this.appLimitedTo = end
    this.handedSinceData = 0
    if (acknowledges(bytes)) this.sent.push({ end, at: now })
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), then sends what the freed headroom allows. */
  acknowledged(ack: PiecesAck): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(ack)) return false
    const now = performance.now()
    const spanMs = ack.spanUs / 1000
    this.arrivedAt += spanMs
    this.leastBack = Math.min(this.leastBack, now - ack.heldMs - this.arrivedAt)
    // When the newest data the acknowledgement covers arrived, on this clock.
    const arrived = this.arrivedAt + this.leastBack
    const acknowledged = this.sender.acknowledgedBytes
    let newest: { end: number; at: number } | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    if (newest !== undefined) this.leastRtt = Math.min(this.leastRtt, arrived - newest.at)
    // Arrivals of data handed while nothing waited came at the app's rate: they neither set the rate nor lower it.
    if (before < this.appLimitedTo) this.drain.dropSpan()
    else this.drain.spanned(acknowledged - before, spanMs)
    // The path holds what the page hasn't acknowledged, less what the rate drained since that data arrived.
    this.inPath = Math.max(
      0,
      this.sender.transmittedBytes - acknowledged - this.drainRate() * Math.max(0, now - arrived),
    )
    this.inPathAt = now
    this.sendWaiting()
    return true
  }

  /** The socket closed: pacing stops, and nothing more is handed to it. */
  close(): void {
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
    this.sender.close()
  }

  /** Sends what waits for headroom, as far as the headroom goes now. */
  private sendWaiting(): void {
    if (this.sender.dataWaits) {
      this.releasing = true
      this.sender.sendWaiting(this.headroom(performance.now()))
      this.releasing = false
    }
    this.pace()
  }

  /** What the path holds at `now`, as the rate drained it since `inPathAt`. */
  private pathHolds(now: number): number {
    return Math.max(0, this.inPath - this.drainRate() * (now - this.inPathAt))
  }

  /** While data waits for headroom, looks again once the rate freed what a piece takes (see `PieceSender.cut`). */
  private pace(): void {
    if (this.paceTimer !== null || this.sender.waitingBytes === 0) return
    const ms = Math.max(1, (WIRE_PIECES_ACK_BYTES - this.headroom(performance.now())) / this.drainRate())
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.sendWaiting()
  }
}
