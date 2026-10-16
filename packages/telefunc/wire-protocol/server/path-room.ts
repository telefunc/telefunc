export { PathRoom }

import { WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS } from '../constants.js'
import { DRAIN_SPAN_MS, Link, newestFastest } from '../link.js'
import { PieceSender, acknowledges, type SendRoom } from '../pieces.js'
import type { PiecesAck } from '../shared-ws.js'
import { Fifo } from '../../utils/Fifo.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

/** The data a server lets the path to its page hold: what the rate its page's acknowledgements measure carries in
 *  `WIRE_SEND_AHEAD_ROUND_TRIPS` of the least round trip, and `WIRE_SEND_AHEAD_MIN_BYTES` at least. The rest waits whole
 *  in its sender, where its acknowledgements and window refreshes pass it. */
class PathRoom implements SendRoom {
  readonly cuts = false
  /** Every frame the WebSocket carries goes through it. */
  readonly sender: PieceSender
  /** In bytes per ms: the fastest of the newest rates its data arrived at while data waited to go, 0 before any; the
   *  bytes and ms of arrivals not yet in a rate. */
  private drainRate = 0
  private readonly drainRates: number[] = []
  private spanBytes = 0
  private spanMs = 0
  /** In ms, the page's clock from the first data an acknowledgement covered, and the least time an acknowledgement took
   *  back, up to the clocks' offset: the one that waited least behind what the page sends dates the data. */
  private arrivedAt = 0
  private leastBack = Number.POSITIVE_INFINITY
  /** The least time from data handed to the acknowledgement that covered it, `Infinity` before one. */
  private leastRtt = Number.POSITIVE_INFINITY
  /** The data handed and not covered by an acknowledgement: where it ends, of all the sender handed, and when. */
  private readonly sent = new Fifo<{ end: number; at: number }>()
  /** Where the newest data handed with no data waiting ahead of it ends, of all the sender handed: arrivals up to it
   *  came at the rate the app sent, not the link's. Whether the sender is sending what it held for room. */
  private appLimitedTo = 0
  private releasing = false
  /** What the path holds of what the sender handed, as of `inPathAt`: the rate drains it, as `Uplink.inUplink`. */
  private inPath = 0
  private inPathAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null

  /** `transmit` and `queued` as `PieceSender`'s. */
  constructor(
    transmit: (message: Uint8Array<ArrayBuffer>) => void,
    private readonly queued: () => number,
  ) {
    this.sender = new PieceSender(transmit, queued, new Link(), this)
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    this.sender.send(frame, pingInterval)
    this.pace()
  }

  /** `SendRoom`: what the path takes past what it holds. */
  dataRoom(): number {
    if (this.drainRate === 0 || this.leastRtt === Number.POSITIVE_INFINITY) return Number.POSITIVE_INFINITY
    const room = Math.max(WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS * this.drainRate * this.leastRtt)
    return room - this.pathHolds(performance.now())
  }

  /** `SendRoom`: the path holds what it handed; data the page dates is dated, each part apart, so the first of a burst
   *  measures the path without it. */
  handed(bytes: number, data: boolean): void {
    const now = performance.now()
    this.inPath = this.pathHolds(now) + bytes
    this.inPathAt = now
    if (!data) return
    const end = this.sender.transmittedBytes
    if (!this.releasing && this.queued() === 0 && !this.sender.holdsData) this.appLimitedTo = end
    if (acknowledges(bytes)) this.sent.push({ end, at: now })
  }

  sentFrame(): void {}

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), and sends what it made room for. */
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
    if (before < this.appLimitedTo) {
      this.spanBytes = this.spanMs = 0
    } else {
      this.spanBytes += acknowledged - before
      this.spanMs += spanMs
      if (this.spanMs >= DRAIN_SPAN_MS) {
        this.drainRate = newestFastest(this.drainRates, this.spanBytes / this.spanMs)
        this.spanBytes = this.spanMs = 0
      }
    }
    // The path holds what the page hasn't acknowledged, less what the rate drained since that data arrived.
    this.inPath = Math.max(0, this.sender.transmittedBytes - acknowledged - this.drainRate * Math.max(0, now - arrived))
    this.inPathAt = now
    this.sendHeld()
    return true
  }

  /** Its socket closed. */
  close(): void {
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
    this.sender.close()
  }

  /** Sends what waits for room, as far as its room goes now. */
  private sendHeld(): void {
    this.releasing = true
    this.sender.sendHeld()
    this.releasing = false
    this.pace()
  }

  /** What the path holds at `now`, as the rate drained it since `inPathAt`. */
  private pathHolds(now: number): number {
    return Math.max(0, this.inPath - this.drainRate * (now - this.inPathAt))
  }

  /** While data waits for room, looks again once the rate drained what the path holds past it. */
  private pace(): void {
    if (this.paceTimer !== null || this.sender.heldBytes === 0) return
    const ms = Math.max(1, -this.dataRoom() / this.drainRate)
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.sendHeld()
  }
}
