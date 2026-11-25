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
import { PieceRate } from '../PieceRate.js'
import {
  PieceSender,
  acknowledges,
  type PieceSenderOptions,
  type PieceSenderOwner,
  type PieceSocket,
} from '../pieces.js'
import { encode, MESSAGE_FRAME_OVERHEAD_MAX_BYTES, type PiecesAck } from '../../wire/frame.js'
import {
  countWaitingPublishes,
  droppingFrom,
  droppingLossy,
  droppingPublishes,
  type WireQueue,
} from './waitingPublishes.js'
import { Fifo } from '../../../utils/Fifo.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

/** How long a blocked window waits for its page's word before it asks. */
const WIRE_ASK_AFTER_MS = WIRE_PIECES_ACK_GAP_MS / 5
/** A message of the least send-ahead, with the most its frame adds. */
const MESSAGE_BYTES = WIRE_SEND_AHEAD_MIN_BYTES + MESSAGE_FRAME_OVERHEAD_MAX_BYTES

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
  /** Whether what the page hasn't confirmed reached half the window since the last ask: the next send asks. */
  private askEarly = false
  /** Whether the sender is sending what waited for headroom, and whether it handed data of it yet. */
  private releasing = false
  private releasedData = false
  /** Bytes handed since the newest data: an in-order socket holding no more holds none of it, bar what is carried. */
  private handedSinceData = 0
  /** Looks again for copy room, which another wire's page may free. */
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  /** Asks for the page's word once a blocked window has heard nothing for a while (`askSoon`). */
  private askTimer: ReturnType<typeof setTimeout> | null = null
  /** The copies handed and not covered by an acknowledgement: where each ends, of all the sender handed. */
  private readonly copied = new Fifo<{ end: number; bytes: number }>()
  /** Waits for an acknowledgement, a heartbeat, or room for copies, which it looks for on its own timer. */
  private awaitsCopyRoom = false
  /** What was handed when the page was last asked what it took, the request included: open till the page took it. */
  private askedUpTo = 0
  /** When the open ask went, null when none is open. */
  private askedAt: number | null = null

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

  /** Sends `frame` through the sender. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    this.sender.send(frame, this.headroom())
    this.askIfHalf()
    this.pace()
  }

  private askIfHalf(): void {
    if (!this.askEarly) return
    this.askEarly = false
    this.askForReport()
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

  /** What the path takes past what the page hasn't confirmed. */
  headroom(): number {
    // Unmeasured, the kernel's TCP paces what the app hands, as on main; measured, only an acknowledgement frees room.
    if (this.pathRate() === 0) return Number.POSITIVE_INFINITY
    // A message may cross the window: on a slow link the next is on its way while the page acknowledges the last.
    const room = this.window() - this.unconfirmed()
    return room > 0 ? room + MESSAGE_BYTES : 0
  }

  /** `PieceSenderOwner`: the most data the path may hold. On a slow link the least round trip still spans a whole frame's
   *  arrival, and what goes ahead is copied pieces. The rate's rise waits for acknowledgements, which go a gap apart
   *  however short the round trip. */
  window(): number {
    const rate = this.pathRate()
    if (rate === 0) return Number.POSITIVE_INFINITY
    const toAck = WIRE_SEND_AHEAD_ROUND_TRIPS * this.leastRtt + 2 * WIRE_PIECES_ACK_GAP_MS
    return Math.max(2 * MESSAGE_BYTES, rate * Math.min(toAck, WIRE_QUEUE_DELAY_MS))
  }

  /** `PieceSenderOwner`: the path holds `bytes`; each dated part is dated apart, so a burst's first part measures the path. */
  handed(bytes: number, data: boolean, copied: boolean): void {
    if (copied) {
      this.copied.push({ end: this.sender.transmittedBytes, bytes })
      addWireCopies(bytes)
    }
    if (!data) {
      this.handedSinceData += bytes
      return
    }
    const end = this.sender.transmittedBytes
    // Unmeasured, what waits in the sender waits for the probe's answer, not for the path: a release's first data
    // arrives after that wait, and only what follows it arrives as the path takes it.
    const pathHeld = this.pathRate() > 0 ? this.releasing || this.sender.dataWaits : this.releasedData
    if (this.releasing) this.releasedData = true
    if (!pathHeld && this.socket.bufferedAmount <= this.handedSinceData) this.appLimitedTo = end
    this.handedSinceData = 0
    if (acknowledges(bytes)) this.sent.push({ end, at: performance.now() })
    // Asked at half the window, the page's answer frees its first half while the second still crosses.
    const asks = this.pieceRate.fastest > 0 && !this.sender.awaitsProbe && hasCopyRoom()
    if (asks && this.pathRate() > 0 && this.unconfirmed() >= this.window() / 2) this.askEarly = true
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
    // Where copy room and not the window holds what waits, its end frees it with no word from the page: look again.
    if (room < this.headroom()) this.awaitsCopyRoom = true
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
    const spanMs = ack.spanUs / 1000
    // When the newest data the acknowledgement covers arrived, on this clock.
    const arrived = this.sender.arrivedAt
    const acknowledged = this.sender.acknowledgedBytes
    let newest: { end: number; at: number } | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= acknowledged) newest = this.sent.shift()
    if (newest !== undefined) this.leastRtt = Math.min(this.leastRtt, arrived - newest.at)
    // Arrivals of data handed while nothing waited came at the app's rate: they neither set the rate nor lower it.
    if (before < this.appLimitedTo) this.drain.dropSpan()
    else this.drain.spanned(acknowledged - before, spanMs)
    this.releaseCopies(acknowledged)
    this.sendWaiting()
    return true
  }

  /** Asks the page soon, unless it says first: a blocked window would otherwise wait out its acknowledgement gap. */
  private askSoon(): void {
    if (this.askTimer === null) this.askTimer = unrefTimer(setTimeout(this.askNow, WIRE_ASK_AFTER_MS))
  }

  /** The page said what it took: the window it frees is the one to ask about, if it blocks again. */
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
    this.askedAt = performance.now()
    this.sender.send(encode.piecesAckReq(), 0)
    this.sender.flush()
    // Both ends count the request: a report the page sent before it read the request doesn't cover it.
    this.askedUpTo = this.sender.transmittedBytes
  }

  /** The page answers an ask at once: a round trip where small frames give acknowledgements none. */
  private measureAnswer(): void {
    if (this.askedAt === null || this.sender.confirmedBytes < this.askedUpTo) return
    this.leastRtt = Math.min(this.leastRtt, performance.now() - this.askedAt)
    this.askedAt = null
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
      this.sender.sendWaiting(this.headroom())
      this.releasing = false
      this.releasedData = false
    }
    this.askIfHalf()
    this.pace()
  }

  /** While data waits, looks again for copy room, or asks for the page's report that frees a measured window. */
  private pace(): void {
    const { waitingBytes, acknowledgedBytes, awaitsProbe } = this.sender
    if (waitingBytes === 0 || awaitsProbe) return
    if (this.awaitsCopyRoom) {
      this.askSoon()
      // Another wire freeing room can't wake this one: on a Durable Object its socket is out of that wire's reach.
      if (acknowledgedBytes > 0 && this.paceTimer === null)
        this.paceTimer = unrefTimer(setTimeout(this.lookAgain, WIRE_PIECES_ACK_GAP_MS))
      return
    }
    if (this.pathRate() === 0) return
    this.askForReport()
    // What goes after this look holds room no ask covers: ask again once the page says nothing for a while.
    this.askSoon()
  }

  private readonly lookAgain = (): void => {
    this.paceTimer = null
    this.sendWaiting()
  }
}
