export { ServerHeadroom }

import {
  WIRE_PIECES_ACK_BYTES,
  WIRE_FLIGHT_LEAST_MS,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_PROBE_CLOCK_MS,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
  WIRE_SEND_AHEAD_ROUND_TRIPS,
} from '../../constants.js'
import { addWireCopies, dropWireCopies, hasCopyRoom } from '../../channel/server/channelMemory.js'
import { DrainRate } from '../DrainRate.js'
import { PieceRate } from '../PieceRate.js'
import {
  PieceSender,
  acknowledges,
  type PieceSenderOptions,
  type PieceSenderOwner,
  type PieceSocket,
} from '../pieces.js'
import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES, encode, type PiecesAck } from '../../wire/frame.js'
import { isSequencedTag } from '../../wire/tags.js'
import {
  countWaitingPublishes,
  droppingFrom,
  droppingLossy,
  droppingPublishes,
  type WireQueue,
} from './waitingPublishes.js'
import { Fifo } from '../../../utils/Fifo.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

/** How long a flight copy room blocked waits for its page's word before it asks. */
const WIRE_ASK_AFTER_MS = WIRE_PIECES_ACK_GAP_MS / 5
/** Where an unmeasured flight starts: the least send-ahead, as one message. */
const START_FLIGHT = WIRE_SEND_AHEAD_MIN_BYTES + MESSAGE_FRAME_OVERHEAD_MAX_BYTES

type ServerHeadroomOptions = Pick<PieceSenderOptions, 'socket' | 'pingInterval'>

/** The path to the page holds what `headroom` allows; the rest waits in its sender. */
class ServerHeadroom implements PieceSenderOwner, WireQueue {
  /** Sends every frame the WebSocket carries. */
  readonly sender: PieceSender
  private readonly socket: PieceSocket
  /** The rate the page takes data at, as acknowledgements measure it. */
  private readonly drain = new DrainRate()
  /** The rates frames crossed at. The probe of the wire's first large frame gives one in a round trip: that frame's
   *  acknowledgement comes only when all of it arrived, which on a slow link is seconds of publishes later. */
  private readonly pieceRate = new PieceRate()
  /** The least time from data handed to the acknowledgement that covered it, `Infinity` before one. */
  private leastRtt = Number.POSITIVE_INFINITY
  /** The data handed and not covered by an acknowledgement: where it ends, of all the sender handed, and when. */
  private readonly sent = new Fifo<{ end: number; at: number }>()
  /** Where the newest data handed with nothing waiting ahead ends: arrivals up to it came at the app's rate. */
  private appLimitedTo = 0
  /** Whether the flight reached half its bound since the last ask: the next send asks. */
  private askEarly = false
  /** Whether the sender is sending what waited for headroom. */
  private releasing = false
  /** Bytes handed since the newest data: an in-order socket holding no more holds none of it, bar what is carried. */
  private handedSinceData = 0
  /** What the path holds of what the sender handed, as of `inPathAt`: the rate drains it, as `Uplink.inUplink`. */
  private inPath = 0
  private inPathAt = 0
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  /** When `paceTimer` fires. */
  private paceAt = 0
  /** Asks for the page's word once a blocked flight has heard nothing for a while (`askSoon`). */
  private askTimer: ReturnType<typeof setTimeout> | null = null
  /** The copies handed and not covered by an acknowledgement: where each ends, of all the sender handed. */
  private readonly copied = new Fifo<{ end: number; bytes: number }>()
  /** Waits for an acknowledgement, a heartbeat, or room for copies, which it looks for on its own timer. */
  private awaitsCopyRoom = false
  /** With no rate, the most the path holds of what the page hasn't said it took: it grows as slow start does. */
  private flight = START_FLIGHT
  /** What the sender had handed when the flight last filled, and how much of what the page took grew the flight. */
  private filledTo = 0
  private grownTo = 0
  /** What the sender had handed when the flight last halved. */
  private halvedAt = 0
  /** Where the rate sample still open began: an answer's, when the answer came too soon to time. */
  private sampleFrom: { bytes: number; at: number } | null = null
  /** How long the newest report's data queued behind the least round trip. */
  private queuedMs = 0
  /** How long the page took to reach a request behind what it took first, past the path's round trip and queue. */
  private turnaround = 0
  /** When data was last handed. */
  private dataAt = Number.NEGATIVE_INFINITY
  /** What was handed when the page was last asked what it took, the request included: open till the page took it. */
  private askedUpTo = 0
  /** For the open ask: what the page had said it took then, and when the oldest data past it was handed. */
  private askedFrom: { bytes: number; at: number; askedAt: number } | null = null
  /** The newest rate an answer showed the path carried, 0 before one. */
  private carried = 0

  constructor({ socket, pingInterval }: ServerHeadroomOptions) {
    this.socket = socket
    this.sender = new PieceSender({
      socket,
      pingInterval,
      pieceRate: this.pieceRate,
      cuts: 'whole',
      lanes: true,
      owner: this,
    })
  }

  /** Sends `frame`, sent at `at`, through the sender. */
  send(frame: Uint8Array<ArrayBuffer>, at: number): void {
    // A path idle past what its flight took to cross may have changed: the flight and the rate start over.
    if (isSequencedTag(frame[0]) && !this.sender.dataWaits && at - this.dataAt > this.idleAfter()) {
      this.flight = START_FLIGHT
      this.drain.reset()
      this.askedFrom = null
      this.sampleFrom = null
      this.carried = 0
    }
    this.sender.send(frame, this.headroom(at))
    this.askIfHalf()
    this.pace()
  }

  private askIfHalf(): void {
    if (!this.askEarly) return
    this.askEarly = false
    this.askForReport()
  }

  /** How long a report's newest data may queue behind the least round trip while the flight grows. */
  private queueLimit(): number {
    const rtt = Number.isFinite(this.leastRtt) ? this.leastRtt : WIRE_QUEUE_DELAY_MS
    return Math.min(
      WIRE_QUEUE_DELAY_MS,
      Math.max(rtt, WIRE_PROBE_CLOCK_MS, WIRE_FLIGHT_LEAST_MS - rtt - this.turnaround),
    )
  }

  /** How long a wire is idle before its flight starts over: the queue delay and a least round trip, or two of those. */
  private idleAfter(): number {
    const rtt = Number.isFinite(this.leastRtt) ? this.leastRtt : 0
    return Math.max(WIRE_QUEUE_DELAY_MS + rtt, 2 * rtt)
  }

  waitingPublishes(ix: number): number {
    return countWaitingPublishes(this.sender.waitingFrames, ix)
  }

  dropWaitingPublishes(ix: number, keep: number, upTo?: number): void {
    this.sender.dropWaiting(droppingPublishes(ix, keep, upTo))
  }

  dropWaitingFrom(ix: number, from: 'publish' | 'start'): void {
    this.sender.dropWaiting(droppingFrom(ix, from))
  }

  dropUngiven(ix: number): number {
    const cut = droppingLossy(ix)
    this.sender.dropUngiven(cut.drops)
    return cut.count()
  }

  /** The rate sends go by, which a probe gives before any data does. */
  private pathRate(): number {
    return this.drain.rate
  }

  /** What the path takes at `at` past what it holds. */
  headroom(at: number): number {
    const rate = this.pathRate()
    // Unmeasured, the path takes the least any sender sends ahead and as much again as the page said it took, which
    // doubles each round trip as TCP's slow start does, until acknowledgements of data that waited give the rate.
    if (rate === 0) return this.flight - this.unconfirmed()
    return this.window() - this.pathHolds(at)
  }

  /** `PieceSenderOwner`: the most data the path may hold. On a slow link the least round trip still spans a whole frame's
   *  arrival, and what goes ahead is copied pieces. The rate's rise waits for acknowledgements, which go a gap apart
   *  however short the round trip. */
  window(): number {
    const rate = this.pathRate()
    if (rate === 0) return this.flight
    const toAck = WIRE_SEND_AHEAD_ROUND_TRIPS * this.leastRtt + 2 * WIRE_PIECES_ACK_GAP_MS
    return Math.max(WIRE_SEND_AHEAD_MIN_BYTES, rate * Math.min(toAck, WIRE_QUEUE_DELAY_MS))
  }

  /** `PieceSenderOwner`: the path holds `bytes`; each dated part is dated apart, so a burst's first part measures the path. */
  handed(bytes: number, data: boolean, copied: boolean): void {
    const now = performance.now()
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
    this.dataAt = now
    if (!this.releasing && this.socket.bufferedAmount <= this.handedSinceData && !this.sender.dataWaits)
      this.appLimitedTo = end
    this.handedSinceData = 0
    if (acknowledges(bytes)) this.sent.push({ end, at: now })
    // Asked at half the flight, the page's answer frees its first half while the second still crosses.
    const asks = this.pieceRate.fastest > 0 && !this.sender.awaitsProbe && hasCopyRoom()
    if (asks && this.pathRate() === 0 && this.unconfirmed() >= this.flight / 2) this.askEarly = true
  }

  /** `PieceSenderOwner`: the most copies this wire may hold once its page took all it handed. */
  copyAllowance(): number {
    return hasCopyRoom() ? Number.POSITIVE_INFINITY : WIRE_SEND_AHEAD_MIN_BYTES
  }

  /** `PieceSenderOwner`: past what copies may hold, stalled wires hold that till their ping deadline, so each wire keeps
   *  the least any sender sends ahead of what its page said it took. One whose page never said waits for that to arrive:
   *  room freed by a stalled wire's close would otherwise go to the next stalled wire. */
  copyRoom(): number {
    if (hasCopyRoom()) return Number.POSITIVE_INFINITY
    // What this task gathered goes with its message, so it takes copy room as handed bytes do.
    const room = WIRE_SEND_AHEAD_MIN_BYTES - this.unconfirmed() - this.sender.ungivenBytes
    // Where copy room and not the flight holds what waits, its end frees it with no word from the page: look again.
    if (room < this.headroom(performance.now())) this.awaitsCopyRoom = true
    if (room >= WIRE_PIECES_ACK_BYTES) return room
    this.awaitsCopyRoom = true
    return 0
  }

  /** What the sender handed that the page hasn't said it took. */
  private unconfirmed(): number {
    return this.sender.transmittedBytes - this.sender.confirmedBytes
  }

  /** Takes a heartbeat (see `PieceSender.heartbeat`), which an acknowledgement's lost tail waits for. */
  heartbeat(bytes: number, at: number): boolean {
    if (!this.sender.heartbeat(bytes, at)) return false
    this.heard()
    this.measureAnswer()
    this.fit(0)
    this.releaseCopies(this.sender.heartbeatBytes)
    // A page taking small frames says what it took mostly here, in the frames it sends anyway.
    if (this.awaitsCopyRoom || this.pathRate() === 0) this.sendWaiting()
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
    this.heard()
    this.measureAnswer()
    const now = performance.now()
    const spanMs = ack.spanUs / 1000
    // When the newest data the acknowledgement covers arrived, on this clock.
    const arrived = this.sender.arrivedAt
    const acknowledged = this.sender.acknowledgedBytes
    let newest: { end: number; at: number } | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    if (newest !== undefined) this.leastRtt = Math.min(this.leastRtt, arrived - newest.at)
    this.fit(newest === undefined ? 0 : arrived - newest.at - this.leastRtt)
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

  /** A filled flight grows by what the page took of it, or halves where its data queued past `queueLimit`. */
  private fit(queuedMs: number): void {
    this.queuedMs = queuedMs
    const took = Math.min(this.sender.confirmedBytes, this.filledTo)
    if (queuedMs > this.queueLimit()) {
      // Once a flight: the reports on what was handed before a halving tell of the queue it already answered.
      if (this.sender.confirmedBytes <= this.halvedAt) return
      this.halvedAt = this.sender.transmittedBytes
      this.flight = Math.max(START_FLIGHT, this.flight / 2)
      this.drain.reset()
      return
    }
    if (this.pathRate() === 0 && took > this.grownTo)
      this.flight = Math.min(this.flight + took - this.grownTo, this.ceiling())
    this.grownTo = Math.max(this.grownTo, took)
  }

  /** The most the flight grows to: what the path carries over a round trip, the turnaround and `queueLimit`. */
  private ceiling(): number {
    if (this.carried === 0) return Number.POSITIVE_INFINITY
    const crossing = (Number.isFinite(this.leastRtt) ? this.leastRtt : 0) + this.turnaround + this.queueLimit()
    return Math.max(START_FLIGHT, this.carried * crossing)
  }

  /** Asks the page soon, unless it says first: a blocked flight would otherwise wait out its acknowledgement gap. */
  private askSoon(): void {
    if (this.askTimer === null) this.askTimer = unrefTimer(setTimeout(this.askNow, WIRE_ASK_AFTER_MS))
  }

  /** The page said what it took: the flight it frees is the one to ask about, if it blocks again. */
  private heard(): void {
    if (this.askTimer !== null) clearTimeout(this.askTimer)
    this.askTimer = null
  }

  private readonly askIfBlocked = (): void => {
    if (this.sender.dataWaits && !this.awaitsCopyRoom) this.askForReport()
  }

  private readonly askNow = (): void => {
    this.askTimer = null
    if (this.sender.dataWaits) this.askForReport()
  }

  private askForReport(): void {
    const { confirmedBytes, transmittedBytes } = this.sender
    if (confirmedBytes < this.askedUpTo || confirmedBytes >= transmittedBytes) return
    // An open announcement counts every message till it closes, at the end of this task.
    if (this.sender.announcing) return void queueMicrotask(this.askIfBlocked)
    this.askedFrom = { bytes: confirmedBytes, at: this.handedAt(confirmedBytes), askedAt: performance.now() }
    this.sender.send(encode.piecesAckReq(), 0)
    this.sender.flush()
    // Both ends count the request: a report the page sent before it read the request doesn't cover it.
    this.askedUpTo = this.sender.transmittedBytes
  }

  /** When the oldest data past `bytes` of all the sender handed was handed. */
  private handedAt(bytes: number): number {
    for (const handed of this.sent) if (handed.end > bytes) return handed.at
    return this.dataAt
  }

  /** The page answered an ask: what it took since, over the time it took, is a rate the path carried at the least. */
  private measureAnswer(): void {
    const from = this.askedFrom
    const took = this.sender.confirmedBytes
    if (from === null || took < this.askedUpTo) return
    this.askedFrom = null
    const now = performance.now()
    // The page answers at once: a round trip where small frames give acknowledgements none.
    this.leastRtt = Math.min(this.leastRtt, now - from.askedAt)
    // Past the round trip and the queue, the answer waited for the page to reach the request.
    const turnaround = Math.max(0, now - from.askedAt - this.leastRtt - this.queuedMs)
    this.turnaround = this.turnaround === 0 ? turnaround : (this.turnaround * 7 + turnaround) / 8
    // An answer sooner than a least round trip, or than this clock can time, leaves its span open for the next.
    const sample = (this.sampleFrom ??= from)
    const ms = now - sample.at - (Number.isFinite(this.leastRtt) ? this.leastRtt : 0)
    if (ms < Math.max(WIRE_PROBE_CLOCK_MS, this.leastRtt) || took <= sample.bytes) return
    this.sampleFrom = null
    // A newer answer replaces the older: a path that slowed shows it here, before any queue does.
    this.carried = (took - sample.bytes) / ms
    this.flight = Math.max(START_FLIGHT, Math.min(Math.max(this.flight, this.ceiling() / 2), this.ceiling()))
  }

  /** The socket closed: pacing stops, and nothing more is handed to it. */
  close(): void {
    this.heard()
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
    this.askIfHalf()
    this.pace()
  }

  /** What the path holds at `now`, as the rate drained it since `inPathAt`. */
  private pathHolds(now: number): number {
    return Math.max(0, this.inPath - this.pathRate() * (now - this.inPathAt))
  }

  /** While data waits for headroom, looks again once the rate freed what a piece takes (see `PieceSender.cut`). */
  private pace(): void {
    const { waitingBytes, acknowledgedBytes, awaitsProbe } = this.sender
    if (waitingBytes === 0 || awaitsProbe) return
    // With no rate, nothing drains on a timer: the page's report frees headroom, so a full flight asks for it.
    if (this.awaitsCopyRoom) this.askSoon()
    else if (this.pathRate() === 0) {
      this.filledTo = this.sender.transmittedBytes
      this.askForReport()
      // What goes after this look holds room no ask covers: ask again once the page says nothing for a while.
      this.askSoon()
    }
    if (this.awaitsCopyRoom && acknowledgedBytes === 0) return
    if (!this.awaitsCopyRoom && this.pathRate() === 0) return
    // Another wire freeing room can't wake this one: on a Durable Object its socket is out of that wire's reach.
    const ms = this.awaitsCopyRoom
      ? WIRE_PIECES_ACK_GAP_MS
      : Math.max(1, (WIRE_PIECES_ACK_BYTES - this.headroom(performance.now())) / this.pathRate())
    // A rate measured since the timer was set may free the room sooner: it looks again then.
    const at = performance.now() + ms
    if (this.paceTimer !== null) {
      if (at >= this.paceAt) return
      clearTimeout(this.paceTimer)
    }
    this.paceAt = at
    this.paceTimer = unrefTimer(setTimeout(this.lookAgain, ms))
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.sendWaiting()
  }
}
