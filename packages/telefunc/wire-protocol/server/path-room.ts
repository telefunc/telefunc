export { PathRoom }

import { WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS } from '../constants.js'
import { DRAIN_SPAN_MS, Link, newestFastest } from '../link.js'
import { PieceSender, acknowledges, type PieceSenderOptions, type SendRoom } from '../pieces.js'
import type { PiecesAck } from '../wire/connectionFrame.js'
import { Fifo } from '../../utils/Fifo.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

type PathRoomOptions = Pick<PieceSenderOptions, 'transmit' | 'queued' | 'pingInterval'>

/** The path to the page holds what `dataRoom` allows; the rest waits whole in its sender. */
class PathRoom implements SendRoom {
  readonly cuts = false
  /** Every frame the WebSocket carries goes through it. */
  readonly sender: PieceSender
  private readonly queued: () => number
  /** In bytes per ms: the fastest of the newest drain rates, 0 before any; `spanBytes` and `spanMs` gather the next. */
  private drainRate = 0
  private readonly drainRates: number[] = []
  private spanBytes = 0
  private spanMs = 0
  /** In ms, the page's clock from the first data an acknowledgement covered. */
  private arrivedAt = 0
  /** In ms, the least time an acknowledgement took back, up to the clocks' offset. */
  private leastBack = Number.POSITIVE_INFINITY
  /** The least time from data handed to the acknowledgement that covered it, `Infinity` before one. */
  private leastRtt = Number.POSITIVE_INFINITY
  /** The data handed and not covered by an acknowledgement: where it ends, of all the sender handed, and when. */
  private readonly sent = new Fifo<{ end: number; at: number }>()
  /** Where the newest data handed with nothing waiting ahead ends: arrivals up to it came at the app's rate. */
  private appLimitedTo = 0
  /** Whether the sender is sending what it held for room. */
  private releasing = false
  /** Bytes handed since the newest data: an in-order socket holding no more holds none of it, bar what is gathered. */
  private handedSinceData = 0
  /** What the path holds of what the sender handed, as of `inPathAt`: the rate drains it, as `Uplink.inUplink`. */
  private inPath = 0
  private inPathAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null

  constructor({ transmit, queued, pingInterval }: PathRoomOptions) {
    this.queued = queued
    this.sender = new PieceSender({ transmit, queued, pingInterval, link: new Link(), room: this })
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    this.sender.send(frame)
    this.pace()
  }

  /** `SendRoom`: what the path takes past what it holds. */
  dataRoom(): number {
    if (this.drainRate === 0 || this.leastRtt === Number.POSITIVE_INFINITY) return Number.POSITIVE_INFINITY
    const room = Math.max(WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS * this.drainRate * this.leastRtt)
    return room - this.pathHolds(performance.now())
  }

  /** `SendRoom`: the path holds `bytes`; each dated part is dated apart, so a burst's first part measures the path. */
  handed(bytes: number, data: boolean): void {
    const now = performance.now()
    this.inPath = this.pathHolds(now) + bytes
    this.inPathAt = now
    if (!data) {
      this.handedSinceData += bytes
      return
    }
    const end = this.sender.transmittedBytes
    if (!this.releasing && this.queued() <= this.handedSinceData && !this.sender.holdsData) this.appLimitedTo = end
    this.handedSinceData = 0
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
