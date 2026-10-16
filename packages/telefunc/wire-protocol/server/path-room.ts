export { PathRoom }

import { WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS } from '../constants.js'
import { DRAIN_SPAN_MS, newestFastest } from '../link.js'
import { PieceSender, acknowledges, type SendRoom } from '../pieces.js'
import type { PiecesAck } from '../shared-ws.js'
import { Fifo } from '../../utils/Fifo.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

/** The data a server lets the path to its page hold: what the rate its page's acknowledgements measure carries in
 *  `WIRE_SEND_AHEAD_ROUND_TRIPS` of the least round trip, and `WIRE_SEND_AHEAD_MIN_BYTES` at least. The rest waits whole
 *  in its sender, where its acknowledgements and window refreshes pass it. */
class PathRoom implements SendRoom {
  readonly whole = true
  /** Every frame the WebSocket carries goes through it. */
  readonly sender: PieceSender
  /** In bytes per ms: the fastest of the newest rates its data arrived at, 0 before any; the bytes and ms of arrivals not
   *  yet in a rate. */
  private rate = 0
  private readonly rates: number[] = []
  private spanBytes = 0
  private spanMs = 0
  /** In ms, the page's clock from the first data an acknowledgement covered, and the least it lagged this clock by: as
   *  an acknowledgement that waited least behind what the page sends dates it. */
  private arrivedAt = 0
  private leastOffset = Number.POSITIVE_INFINITY
  /** The least time from data handed to the acknowledgement that covered it, `Infinity` before one. */
  private leastRtt = Number.POSITIVE_INFINITY
  /** The data handed and not covered by an acknowledgement: where it ends, of all the sender handed, and when. */
  private readonly sent = new Fifo<{ end: number; at: number }>()
  private paceTimer: ReturnType<typeof setTimeout> | null = null

  /** `transmit` and `queued` as `PieceSender`'s. */
  constructor(transmit: (message: Uint8Array<ArrayBuffer>) => void, queued: () => number) {
    this.sender = new PieceSender(transmit, queued, undefined, this)
  }

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    this.sender.send(frame, pingInterval)
    this.pace()
  }

  /** `SendRoom`: what the path takes past what it holds, as the rate drains it from the newest arrival. */
  dataRoom(): number {
    if (this.rate === 0 || this.leastRtt === Number.POSITIVE_INFINITY) return Number.POSITIVE_INFINITY
    const held = this.sender.transmittedBytes - this.sender.acknowledgedBytes
    const drained = this.rate * Math.max(0, performance.now() - this.arrived)
    return Math.max(WIRE_SEND_AHEAD_MIN_BYTES, WIRE_SEND_AHEAD_ROUND_TRIPS * this.rate * this.leastRtt) - held + drained
  }

  /** `SendRoom`: data the page dates is dated, each part apart, so the first of a burst measures the path without it. */
  handed(bytes: number, data: boolean): void {
    if (data && acknowledges(bytes)) this.sent.push({ end: this.sender.transmittedBytes, at: performance.now() })
  }

  sentFrame(): void {}

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), and sends what it made room for. */
  acknowledged(ack: PiecesAck): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(ack)) return false
    const spanMs = ack.spanUs / 1000
    this.arrivedAt += spanMs
    this.leastOffset = Math.min(this.leastOffset, performance.now() - ack.heldMs - this.arrivedAt)
    const acknowledged = this.sender.acknowledgedBytes
    let newest: { end: number; at: number } | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    if (newest !== undefined) this.leastRtt = Math.min(this.leastRtt, this.arrived - newest.at)
    this.spanBytes += acknowledged - before
    this.spanMs += spanMs
    if (this.spanMs >= DRAIN_SPAN_MS) {
      this.rate = newestFastest(this.rates, this.spanBytes / this.spanMs)
      this.spanBytes = this.spanMs = 0
    }
    this.sender.sendHeld()
    this.pace()
    return true
  }

  /** When the newest data an acknowledgement covered arrived, on this clock. */
  private get arrived(): number {
    return this.arrivedAt + this.leastOffset
  }

  /** Its socket closed. */
  close(): void {
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
    this.sender.close()
  }

  /** While data waits for room, looks again once the rate drained what the path holds past it. */
  private pace(): void {
    if (this.paceTimer !== null || this.sender.heldBytes === 0) return
    const ms = Math.max(1, -this.dataRoom() / this.rate)
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.sender.sendHeld()
    this.pace()
  }
}
