export { ServerHeadroom }

import {
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
  WIRE_SEND_AHEAD_ROUND_TRIPS,
} from '../../constants.js'
import { addWireCopies, dropWireCopies, hasCopyRoom } from '../../channel/server/channelMemory.js'
import { DrainRate } from '../DrainRate.js'
import { PathEstimate } from '../PathEstimate.js'
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
import {
  countWaitingPublishes,
  droppingFromPublish,
  droppingPublishes,
  overtakes,
  type WireQueue,
} from './waitingPublishes.js'
import { Fifo } from '../../../utils/Fifo.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

type ServerHeadroomOptions = Pick<PieceSenderOptions, 'socket' | 'pingInterval'>

/** The path to the page holds what `headroom` allows; the rest waits in its sender. */
class ServerHeadroom implements PieceSenderOwner, WireQueue {
  /** Sends every frame the WebSocket carries. */
  readonly sender: PieceSender
  /** What the path to the page delivers, from the page's acknowledgements of every data frame. */
  readonly path = new PathEstimate({ reportGap: WIRE_PIECES_ACK_GAP_MS })
  private readonly socket: PieceSocket
  /** The rate the page takes data at, as acknowledgements measure it. */
  private readonly drain = new DrainRate()
  /** The rates frames crossed at. The probe of the wire's first large frame gives one in a round trip: that frame's
   *  acknowledgement comes only when all of it arrived, which on a slow link is seconds of publishes later. */
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
  /** The copies handed and not covered by an acknowledgement: where each ends, of all the sender handed. */
  private readonly copied = new Fifo<{ end: number; bytes: number }>()
  /** Waits for an acknowledgement, a heartbeat, or room for copies, which it looks for on its own timer. */
  private awaitsCopyRoom = false

  constructor({ socket, pingInterval }: ServerHeadroomOptions) {
    this.socket = socket
    this.sender = new PieceSender({
      socket,
      pingInterval,
      pieceRate: this.pieceRate,
      cuts: 'whole',
      overtakes,
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

  dropWaitingFromPublish(ix: number): void {
    this.sender.dropWaiting(droppingFromPublish(ix))
  }

  get unhandedBytes(): number {
    return this.sender.unhandedBytes
  }

  drainRate(): number {
    return this.drain.rate || this.pieceRate.carried
  }

  /** The rate sends go by, which a probe gives before any data does. */
  private pathRate(): number {
    return this.drain.rate || this.pieceRate.fastest
  }

  /** What the path takes at `at` past what it holds. */
  headroom(at: number): number {
    const rate = this.pathRate()
    if (rate === 0) return Number.POSITIVE_INFINITY
    // On a slow link the least round trip still spans a whole frame's arrival, and what goes ahead is copied pieces.
    // The rate's rise waits for acknowledgements, which go a gap apart however short the round trip.
    const toAck = WIRE_SEND_AHEAD_ROUND_TRIPS * this.leastRtt + 2 * WIRE_PIECES_ACK_GAP_MS
    const ahead = Math.min(toAck, WIRE_QUEUE_DELAY_MS)
    return Math.max(WIRE_SEND_AHEAD_MIN_BYTES, rate * ahead) - this.pathHolds(at)
  }

  /** `PieceSenderOwner`: the path holds `bytes`; each dated part is dated apart, so a burst's first part measures the path. */
  handed(bytes: number, data: boolean, copied: boolean): void {
    const now = performance.now()
    this.path.handed(this.sender.transmittedBytes, now)
    this.inPath = this.pathHolds(now) + bytes
    this.inPathAt = now
    if (copied) {
      this.copied.push({ end: this.sender.transmittedBytes, bytes })
      addWireCopies(bytes)
    }
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

  /** `PieceSenderOwner`: past what copies may hold, stalled wires hold that till their ping deadline, so each wire keeps
   *  the least any sender sends ahead, which is how it shows it reads. One that never did waits for that to arrive: room
   *  freed by a stalled wire's close would otherwise go to the next stalled wire. */
  copyRoom(): number {
    if (hasCopyRoom()) return Number.POSITIVE_INFINITY
    const { transmittedBytes, acknowledgedBytes } = this.sender
    const room = WIRE_SEND_AHEAD_MIN_BYTES - (transmittedBytes - acknowledgedBytes)
    if (room >= WIRE_PIECES_ACK_BYTES) return room
    this.awaitsCopyRoom = true
    return 0
  }

  /** Takes a heartbeat (see `PieceSender.heartbeat`), which an acknowledgement's lost tail waits for. */
  heartbeat(bytes: number, at: number): boolean {
    if (!this.sender.heartbeat(bytes, at)) return false
    this.releaseCopies(this.sender.heartbeatBytes)
    // Small frames are never acknowledged: for their wire, this is what shows its page reads.
    if (this.awaitsCopyRoom) this.sendWaiting()
    return true
  }

  /** The page took `bytes` of all the sender handed. */
  private releaseCopies(bytes: number): void {
    let released = 0
    while (this.copied.length > 0 && this.copied.peek()!.end <= bytes) released += this.copied.shift()!.bytes
    if (released > 0) dropWireCopies(released)
  }

  /** Takes a PIECES_ACK (see `PieceSender.acknowledged`), then sends what the freed headroom allows. */
  acknowledged(ack: PiecesAck): boolean {
    const before = this.sender.acknowledgedBytes
    if (!this.sender.acknowledged(ack)) return false
    const now = performance.now()
    this.path.acknowledged(ack, now)
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
      this.sender.transmittedBytes - acknowledged - this.pathRate() * Math.max(0, now - arrived),
    )
    this.inPathAt = now
    this.releaseCopies(acknowledged)
    this.sendWaiting()
    return true
  }

  /** The socket closed: pacing stops, and nothing more is handed to it. */
  close(): void {
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
    this.sender.close()
    this.releaseCopies(Number.POSITIVE_INFINITY)
  }

  /** Sends what waits for headroom, as far as the headroom goes now. */
  private readonly sendWaiting = (): void => {
    this.awaitsCopyRoom = false
    if (this.sender.dataWaits) {
      this.releasing = true
      this.sender.sendWaiting(this.headroom(performance.now()))
      this.releasing = false
    }
    this.pace()
  }

  /** What the path holds at `now`, as the rate drained it since `inPathAt`. */
  private pathHolds(now: number): number {
    return Math.max(0, this.inPath - this.pathRate() * (now - this.inPathAt))
  }

  /** While data waits for headroom, looks again once the rate freed what a piece takes (see `PieceSender.cut`). */
  private pace(): void {
    const { waitingBytes, acknowledgedBytes, awaitsProbe } = this.sender
    if (this.paceTimer !== null || waitingBytes === 0 || awaitsProbe) return
    if (this.awaitsCopyRoom && acknowledgedBytes === 0) return
    // Another wire freeing room can't wake this one: on a Durable Object its socket is out of that wire's reach.
    const ms = this.awaitsCopyRoom
      ? WIRE_PIECES_ACK_GAP_MS
      : Math.max(1, (WIRE_PIECES_ACK_BYTES - this.headroom(performance.now())) / this.pathRate())
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.sendWaiting()
  }
}
