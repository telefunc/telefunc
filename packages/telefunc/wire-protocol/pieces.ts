export { PieceSender, PieceReceiver, acknowledges }
export type { SendRoom }

import {
  WIRE_ANNOUNCED_MIN_BYTES,
  WIRE_ANNOUNCED_WAIT_MARGIN,
  WIRE_GATHER_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PROBE_CLOCK_MS,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
} from './constants.js'
import { Link } from './link.js'
import {
  LENGTH_BYTES,
  TAG,
  assertProtocol,
  encode,
  fixedBytes,
  isSequencedTag,
  passesData,
  type PiecesAck,
} from './shared-ws.js'
import { assert } from '../utils/assert.js'
import { Fifo } from '../utils/Fifo.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** Whether its receiver acknowledges a frame of this size. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECES_ACK_BYTES
}

/** The most the announced message after one of `last` bytes carries; with neither it nor a `floor`, a probe's first. */
function announcedNext(last: number, floor: number): number {
  return last === 0 && floor === 0 ? WIRE_PROBE_FIRST_BYTES : Math.max(2 * last, WIRE_ANNOUNCED_MIN_BYTES)
}

/** The bytes a PIECE and an ANNOUNCE add to what they carry. */
const PIECE_HEADER_BYTES = encode.piece(0, new Uint8Array(0)).byteLength
const ANNOUNCE_BYTES = encode.announce(0, 0).byteLength

/** How much data a sender hands its socket: a page's `SendBudget`, or a server's `PathRoom`. */
type SendRoom = {
  /** Whether it cuts data to its room; else data goes whole while it has room, and waits whole once it has none. */
  readonly cuts: boolean
  /** With a rate measured, the most bytes of data it hands the socket now. */
  dataRoom(): number
  /** It handed the socket `bytes`, of data or not, or gathered them for the message its turn ends with. */
  handed(bytes: number, data: boolean): void
  /** It was sent a frame of `tag` and `bytes`, which it hands the socket now or once room allows. */
  sentFrame(tag: number, bytes: number): void
}

const UNBOUNDED: SendRoom = {
  cuts: false,
  dataRoom: () => Number.POSITIVE_INFINITY,
  handed: () => {},
  sentFrame: () => {},
}

/** One WebSocket's sender: a turn's frames go in one message, one that may take a ping interval to cross in pieces. */
class PieceSender {
  /** The frames and pieces sent and not yet acknowledged that measure the rate. */
  private readonly unacknowledged = new Fifo<{ crossing: number; at: number; end: number; probe: boolean }>()
  /** The bytes the receiver has acknowledged; the first frame's, and when it was sent. */
  acknowledgedBytes = 0
  private firstBytes = 0
  private firstAt = 0
  /** The bytes its receiver said it took at its newest heartbeat. */
  heartbeatBytes = 0
  /** The bytes of the messages for the socket, headers and announcements included, as its receiver counts them. */
  transmittedBytes = 0
  /** A probe out: the bytes handed to the socket through it, and where its second piece starts, for a pair. */
  private probe: { end: number; second: number | null } | null = null
  /** The rest of a data frame, waiting for room. */
  private rest: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** The frames sent after the rest, but those that pass it. */
  private readonly held = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The bytes waiting for room. */
  heldBytes = 0
  /** The announcement its task's frames go behind, open until the task ends. */
  private open: { start: number; at: number; bytes: number } | null = null
  /** The bytes of the newest message it announced. */
  private lastAnnounced = 0
  /** Each message it announced that its receiver hasn't acknowledged: where it ends, of all it sent, and when. */
  private readonly announcedEnds = new Fifo<{ end: number; at: number }>()
  private acknowledgedAt = 0
  /** Each announcement its receiver hasn't acknowledged: where its bytes start and end, of all it sent, and when. */
  private readonly announcements = new Fifo<{ start: number; end: number; at: number }>()
  /** In bytes per ms, the floor its newest ANNOUNCE named (see `Link.floor`). */
  private namedFloor = 0
  /** With no rate measured, a frame this size or smaller goes unannounced. */
  private unannouncedUpTo = WIRE_PROBE_FIRST_BYTES
  /** A frame this size or smaller goes whole, a larger one in pieces this size (see `setWholeUpTo`). */
  private wholeUpTo = 2 * WIRE_PROBE_PAIR_BYTES
  /** The most bytes a message of whole frames carries: what a frame of them could, whole and unannounced. */
  private gatherUpTo = WIRE_PROBE_FIRST_BYTES
  /** The frames the next message it sends carries (see `carry`). */
  private readonly carried: (() => Uint8Array<ArrayBuffer> | null)[] = []
  /** The frames counted and not yet handed to the socket. */
  private readonly turn: TurnMessage

  constructor(
    transmit: (message: Uint8Array<ArrayBuffer>) => void,
    /** The bytes its socket holds unsent. */
    private readonly queued: () => number,
    /** Its wire's ping interval, which sizes the frames it sends whole. */
    private pingInterval: number,
    /** Its wire's: the rates it measures go there. */
    private readonly link = new Link(),
    private readonly room = UNBOUNDED,
  ) {
    // As the turn ends, it hands what it gathered unless an announcement is open; what was only carried waits.
    this.turn = new TurnMessage(transmit, () => {
      if (this.open === null) this.turn.flush()
    })
    this.setWholeUpTo()
  }

  /** The bytes of the messages for the socket, and of the frames sent, those it holds. */
  get sentBytes(): number {
    return this.transmittedBytes + this.heldBytes
  }

  /** The bytes of the frames sent that its socket doesn't have yet. */
  get unhandedBytes(): number {
    return this.heldBytes + this.turn.bytes
  }

  /** Whether it has data its socket doesn't have yet: what it holds waits behind the rest of a data frame. */
  get holdsData(): boolean {
    return this.rest !== null || this.turn.data
  }

  /** The next message it hands the socket carries ahead of it the frame `frame` returns then, if any. */
  carry(frame: () => Uint8Array<ArrayBuffer> | null): void {
    this.carried.push(frame)
  }

  setPingInterval(ms: number): void {
    this.pingInterval = ms
    this.setWholeUpTo()
  }

  /** Sends `frame`, whole or in pieces. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    if (this.sentBytes === 0) {
      this.firstBytes = frame.byteLength
      this.firstAt = performance.now()
    }
    this.takeCarried()
    const tag = frame[0]!
    this.room.sentFrame(tag, frame.byteLength)
    // It passes what waits: the probe its peer waits on may be its own.
    if (tag === TAG.PIECES_ACK && this.open === null) return this.pass(frame)
    if (this.rest === null) {
      if (this.open === null && !this.announces(frame)) return this.pass(frame)
      return this.sendBehindAnnouncement(frame)
    }
    if (this.passesRest(frame)) return this.pass(frame)
    this.held.push(frame)
    this.heldBytes += frame.byteLength
  }

  /** Whether `frame` sent now goes behind an announcement. */
  private announces(frame: Uint8Array<ArrayBuffer>): boolean {
    const bytes = frame.byteLength
    return (
      this.probe !== null ||
      (this.link.fastest === 0 && bytes > this.unannouncedUpTo && (bytes > this.wholeUpTo || isSequencedTag(frame[0]!)))
    )
  }

  /** While the rest of a data frame waits for room, a frame that `passesData` and goes whole goes first. */
  private passesRest(frame: Uint8Array<ArrayBuffer>): boolean {
    return passesData(frame[0]!) && frame.byteLength <= this.wholeUpTo
  }

  /** Hands the socket `frame`, whole or in pieces. */
  private pass(frame: Uint8Array<ArrayBuffer>): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    if (data && !this.room.cuts && this.room.dataRoom() <= 0) return this.hold(frame, 0)
    if (bytes > this.wholeUpTo || (this.link.fastest > 0 && data && this.room.cuts && bytes > this.room.dataRoom())) {
      return this.cut(frame, 0)
    }
    this.gather(frame, data)
  }

  /** Counts `frame` into its message. A cutting room times data from its handing: data follows only carried frames. */
  private gather(frame: Uint8Array<ArrayBuffer>, data: boolean): void {
    const bytes = frame.byteLength
    const turn = this.turn
    if (
      turn.own &&
      ((data && (turn.atOnce || this.room.cuts)) || turn.bytes + turn.lengthAhead + bytes > this.gatherUpTo)
    ) {
      turn.flush()
    }
    this.count(frame, data)
    if (acknowledges(bytes)) this.track(bytes, performance.now(), false)
    turn.add(frame, data, true)
  }

  /** Counts `frame` as it joins what it gathers, after the LENGTH the frame before it needs now. */
  private count(frame: Uint8Array<ArrayBuffer>, data: boolean): void {
    const length = this.turn.lengthAhead
    if (length > 0) {
      this.countHanded(length, false)
      this.turn.addLength()
    }
    this.countHanded(frame.byteLength, data)
  }

  /** Before a message that gathers nothing: frames sent before it go ahead, and those only carried ride on it. */
  private handGathered(): void {
    if (this.turn.own) this.turn.flush()
  }

  /** Hands the socket what it gathered, in one message. */
  flush(): void {
    this.turn.flush()
  }

  /** Its socket closed: it hands it nothing more, though this turn gathered frames for it. */
  close(): void {
    this.turn.close()
  }

  /** Hands the socket `frame` from `offset` in pieces, of data as far as its room goes, and holds the rest. */
  private cut(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    this.handGathered()
    const bytes = frame.byteLength
    const now = performance.now()
    const room = Math.max(0, Math.floor(this.room.dataRoom()))
    const end = isSequencedTag(frame[0]!) && this.room.cuts ? Math.min(bytes, offset + room) : bytes
    if (end === offset) return this.hold(frame, offset)
    // An empty first piece has the first two acknowledged at once; a frame cut only for room is no measure.
    if (offset === 0 && bytes > this.wholeUpTo) this.sendEmptyPiece(bytes)
    const size = Math.max(1, Math.floor(this.wholeUpTo))
    const handed = end - offset + Math.ceil((end - offset) / size) * PIECE_HEADER_BYTES
    this.countHanded(handed, isSequencedTag(frame[0]!))
    if (acknowledges(bytes)) this.track(handed, now, false)
    for (let at = offset; at < end; at += size) {
      this.turn.hand(encode.piece(bytes, frame.subarray(at, Math.min(end, at + size))))
    }
    if (end < bytes) this.hold(frame, end)
  }

  /** Counts `bytes` handed to the socket, of data or not, or gathered for the message its turn ends with. */
  private countHanded(bytes: number, data: boolean): void {
    this.transmittedBytes += bytes
    this.room.handed(bytes, data)
  }

  /** Hands the socket the empty piece a frame of `total` bytes starts with. */
  private sendEmptyPiece(total: number): void {
    this.countHanded(PIECE_HEADER_BYTES, false)
    this.turn.hand(encode.piece(total, new Uint8Array(0)))
  }

  /** Holds `frame` from `offset`, for room. */
  private hold(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    this.rest = { frame, offset }
    this.heldBytes += frame.byteLength - offset
  }

  /** Sends `frame` at once behind the announcement of this task, opening it with an ANNOUNCE of no bytes if it isn't. */
  private sendBehindAnnouncement(frame: Uint8Array<ArrayBuffer>): void {
    this.handGathered()
    const bytes = frame.byteLength
    const now = performance.now()
    if (this.open === null) {
      this.open = { start: this.transmittedBytes, at: now, bytes: 0 }
      const floorPerS = Math.min(0xffffffff, Math.floor(this.link.floor * 1000))
      this.namedFloor = floorPerS / 1000
      this.countHanded(ANNOUNCE_BYTES, false)
      this.turn.hand(encode.announce(0, floorPerS))
      queueMicrotask(this.closeAnnouncement)
    }
    this.open.bytes += bytes
    if (this.link.fastest === 0 && this.probe === null && bytes > this.wholeUpTo) this.sendProbe(frame, now)
    else this.sendAnnounced(frame, 0, now)
  }

  /** Closes the announcement of this task with an ANNOUNCE of the bytes of the frames sent behind it. */
  private readonly closeAnnouncement = (): void => {
    const { start, at, bytes } = this.open!
    this.open = null
    this.countHanded(ANNOUNCE_BYTES, false)
    this.turn.hand(encode.announce(bytes, 0))
    this.announcements.push({ start, end: this.transmittedBytes, at })
  }

  /** Sends `frame` as two pieces back to back, which its receiver acknowledges at once, then the rest of it. */
  private sendProbe(frame: Uint8Array<ArrayBuffer>, now: number): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    this.sendEmptyPiece(bytes)
    for (const [at, size] of [
      [0, WIRE_PROBE_FIRST_BYTES],
      [WIRE_PROBE_FIRST_BYTES, WIRE_PROBE_PAIR_BYTES],
    ] as const) {
      this.countHanded(PIECE_HEADER_BYTES + size, data)
      this.track(PIECE_HEADER_BYTES + size, now, true)
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, this.transmittedBytes, now)
    }
    // Its receiver dates only data apart.
    const second = data ? this.transmittedBytes - PIECE_HEADER_BYTES - WIRE_PROBE_PAIR_BYTES : null
    this.probe = { end: this.transmittedBytes, second }
    this.sendAnnounced(frame, WIRE_PROBE_FIRST_BYTES + WIRE_PROBE_PAIR_BYTES, now)
  }

  /** Sends `frame` from `offset` as announced messages, none past what `announcedNext` allows after the one before. */
  private sendAnnounced(frame: Uint8Array<ArrayBuffer>, offset: number, now: number): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    if (offset === 0 && bytes <= announcedNext(this.lastAnnounced, this.namedFloor)) {
      this.countHanded(bytes, data)
      if (acknowledges(bytes)) this.track(bytes, now, false)
      return this.sendAnnouncedMessage(frame, bytes, this.transmittedBytes, now)
    }
    const sizes: number[] = []
    for (let left = bytes - offset, last = this.lastAnnounced; left > 0; left -= last) {
      last = Math.min(left, announcedNext(last, this.namedFloor))
      sizes.push(last)
    }
    const n = sizes.length
    if (n >= 2 && sizes[n - 1]! < sizes[n - 2]!) {
      const both = sizes[n - 2]! + sizes[n - 1]!
      sizes[n - 2] = Math.ceil(both / 3)
      sizes[n - 1] = both - sizes[n - 2]!
    }
    let end = this.transmittedBytes
    const handed = bytes - offset + n * PIECE_HEADER_BYTES
    this.countHanded(handed, data)
    if (acknowledges(bytes)) this.track(handed, now, false)
    let at = offset
    for (const size of sizes) {
      end += PIECE_HEADER_BYTES + size
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, end, now)
      at += size
    }
  }

  /** Hands the socket `message` at `at`, which carries `bytes` of the frames it announced, up to `end` of all it sent. */
  private sendAnnouncedMessage(message: Uint8Array<ArrayBuffer>, bytes: number, end: number, at: number): void {
    this.lastAnnounced = bytes
    this.announcedEnds.push({ end, at })
    this.turn.hand(message)
  }

  private track(crossingBytes: number, at: number, probe: boolean): void {
    // What its socket holds and what it gathered cross first, less the largest message, which a browser may count.
    this.unacknowledged.push({
      crossing: Math.max(0, this.queued() + this.turn.bytes - this.turn.largest) + crossingBytes,
      at,
      end: this.transmittedBytes,
      probe,
    })
  }

  /** Sets `wholeUpTo` by the rate and the ping interval. */
  private setWholeUpTo(): void {
    this.wholeUpTo =
      this.link.fastest > 0
        ? (this.link.fastest * this.pingInterval) / WIRE_PIECE_RATE_MARGIN
        : Math.max(
            2 * WIRE_PROBE_PAIR_BYTES,
            // Only a sender that cuts data to its room needs the rate before a frame the floor carries in time.
            this.room.cuts ? 0 : (this.link.floor * this.pingInterval) / WIRE_PIECE_RATE_MARGIN,
          )
    // What crosses within its ping deadline, twice the interval: on any link a PING crosses, or at the floor.
    this.unannouncedUpTo = Math.min(
      this.wholeUpTo,
      Math.max(WIRE_PROBE_FIRST_BYTES, this.link.floor * 2 * this.pingInterval),
    )
    this.gatherUpTo = Math.min(WIRE_GATHER_BYTES, this.link.fastest > 0 ? this.wholeUpTo : this.unannouncedUpTo)
  }

  /** Until when its receiver may take to acknowledge the next announced message; 0 for none. */
  get acknowledgedBy(): number {
    const next = this.announcedEnds.peek()
    const rate = this.link.fastest || this.link.floor
    if (next === undefined || rate === 0) return 0
    const from = Math.max(next.at, this.acknowledgedAt)
    return from + (WIRE_ANNOUNCED_WAIT_MARGIN * (next.end - this.acknowledgedBytes)) / rate
  }

  /** Takes a PIECES_ACK; returns false if it acknowledges nothing sent, or more than was sent. */
  acknowledged({ bytes, heldMs, spanUs }: PiecesAck): boolean {
    const newly = (bytes - this.acknowledgedBytes) >>> 0
    if (newly < 1 || newly > this.transmittedBytes - this.acknowledgedBytes) return false
    const probe = this.probe
    // The ack of a pair's second piece alone dates it apart from the first: the rate the path took them at.
    if (probe !== null && this.acknowledgedBytes === probe.second && newly === probe.end - probe.second) {
      this.rise(newly / Math.max(spanUs / 1000, WIRE_PROBE_CLOCK_MS), performance.now())
    }
    this.acknowledgedBytes += newly
    while (this.announcedEnds.length > 0 && this.announcedEnds.peek()!.end <= this.acknowledgedBytes) {
      this.announcedEnds.shift()
    }
    this.acknowledgedAt = performance.now()
    const arrived = this.acknowledgedAt - heldMs
    const oldest = this.announcements.peek()
    if (oldest !== undefined && oldest.end <= this.acknowledgedBytes) {
      let end = oldest.end
      while (this.announcements.length > 0 && this.announcements.peek()!.end <= this.acknowledgedBytes) {
        end = this.announcements.shift()!.end
      }
      // What it acknowledged went no earlier than the oldest and arrived by the newest: at most the link's rate.
      this.raiseFloor((end - oldest.start) / Math.max(arrived - oldest.at, WIRE_PROBE_CLOCK_MS))
    }
    while (this.unacknowledged.length > 0 && this.unacknowledged.peek()!.end <= this.acknowledgedBytes) {
      const sent = this.unacknowledged.shift()!
      // A sample within one tick of a coarse clock says nothing, but a probe's, which is small, crossed within one.
      if (sent.probe) this.rise(sent.crossing / Math.max(arrived - sent.at, WIRE_PROBE_CLOCK_MS), arrived)
      else if (arrived > sent.at) this.rise(sent.crossing / (arrived - sent.at), arrived)
    }
    if (probe !== null && this.acknowledgedBytes >= probe.end) this.probe = null
    return true
  }

  /** Takes a PING or PONG that arrived at `at`: its receiver took `bytes`. False if that is more than were sent. */
  heartbeat(bytes: number, at: number): boolean {
    // Behind the acknowledged bytes if a PIECES_ACK passed it while it waited behind a probe.
    const took = this.acknowledgedBytes + ((bytes - this.acknowledgedBytes) | 0)
    if (took > this.transmittedBytes) return false
    this.heartbeatBytes = took
    this.raiseFloor(took / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
    return true
  }

  /** Its peer, which sends nothing before this side's first frame arrives, sent a frame that arrived at `at`. */
  answered(at: number): void {
    this.raiseFloor(this.firstBytes / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
  }

  private raiseFloor(floor: number): void {
    if (this.link.raiseFloor(floor)) this.setWholeUpTo()
  }

  private rise(rate: number, at: number): void {
    if (this.link.rise(rate, at)) this.setWholeUpTo()
  }

  /** Sends what waits for room, as far as its room goes now, and the frames held behind it. */
  sendHeld(): void {
    if (this.rest === null) return
    this.takeCarried()
    const { frame, offset } = this.rest
    this.rest = null
    this.heldBytes -= frame.byteLength - offset
    // A room that doesn't cut data held it whole: it goes as if sent now.
    if (this.room.cuts) this.cut(frame, offset)
    else this.pass(frame)
    while (this.rest === null && this.held.length > 0) {
      const next = this.held.shift()!
      this.heldBytes -= next.byteLength
      this.pass(next)
    }
  }

  /** Counts the frames carried into what it gathers, ahead of what is sent next, whatever that holds. */
  private takeCarried(): void {
    if (this.carried.length === 0) return
    for (const take of this.carried.splice(0)) {
      const frame = take()
      if (frame === null) continue
      this.room.sentFrame(frame[0]!, frame.byteLength)
      this.count(frame, false)
      this.turn.add(frame, false, false)
    }
  }
}

/** The frames one turn of a sender gathers, handed to its socket in one message (see `messageFrames`). */
class TurnMessage {
  /** The bytes of the frames it holds, with the LENGTH ahead of each that needs one. */
  bytes = 0
  /** It holds frames that weren't carried. */
  own = false
  /** It holds a frame sent that isn't data. */
  atOnce = false
  /** It holds data. */
  data = false
  /** The largest message handed: a browser may count a message in full while sending it. */
  largest = 0
  private readonly frames: Uint8Array<ArrayBuffer>[] = []
  /** Its last frame needs a LENGTH once one follows it (see `messageFrames`). */
  private lastVaries = false
  private flushQueued = false
  private closed = false

  constructor(
    private readonly transmit: (message: Uint8Array<ArrayBuffer>) => void,
    /** Runs as the turn that added a frame ends, while it holds frames that weren't carried. */
    private readonly turnEnded: () => void,
  ) {}

  /** The bytes of the LENGTH the next frame takes ahead of it. */
  get lengthAhead(): number {
    return this.lastVaries ? LENGTH_BYTES : 0
  }

  /** Counts the LENGTH the next frame takes ahead of it. */
  addLength(): void {
    this.bytes += this.lengthAhead
  }

  /** Adds `frame`, of data or not, sent (`own`) or carried, after its LENGTH. */
  add(frame: Uint8Array<ArrayBuffer>, data: boolean, own: boolean): void {
    this.frames.push(frame)
    this.bytes += frame.byteLength
    this.lastVaries = fixedBytes(frame[0]!) === 0
    if (own) {
      this.own = true
      if (data) this.data = true
      else this.atOnce = true
    }
    if (this.flushQueued) return
    this.flushQueued = true
    queueMicrotask(this.endTurn)
  }

  /** Hands the socket `message`, with what it holds riding ahead of it. */
  hand(message: Uint8Array<ArrayBuffer>): void {
    // Only what was carried rides, of bytes its tag fixes: a LENGTH ahead of it would count after `message`.
    assert(!this.lastVaries)
    this.frames.push(message)
    this.bytes += message.byteLength
    this.flush()
  }

  /** Hands the socket what it holds, in one message. */
  flush(): void {
    const n = this.frames.length
    if (n === 0 || this.closed) return
    const message = n === 1 ? this.frames[0]! : encode.message(this.frames, this.bytes)
    this.frames.length = 0
    this.bytes = 0
    this.lastVaries = this.own = this.atOnce = this.data = false
    if (message.byteLength > this.largest) this.largest = message.byteLength
    this.transmit(message)
  }

  close(): void {
    this.closed = true
  }

  private readonly endTurn = (): void => {
    this.flushQueued = false
    if (this.own) this.turnEnded()
  }
}

/** One WebSocket's receiver: puts a frame sent in pieces back together, and acknowledges the bytes it took. */
abstract class PieceReceiver {
  /** The frame not yet whole: one buffer, grown as its pieces arrive, so what it holds is what arrived. */
  private frame = new Uint8Array(0)
  private received = 0
  protected total = 0
  private data = false
  /** The pieces still to acknowledge at once. */
  private probing = 0
  /** The bytes of the messages that followed the open announcement, null for none open. */
  private announced: number | null = null
  /** When the newest of them arrived, or the opening ANNOUNCE before any. */
  private announcedAt = 0
  private lastAnnounced = 0
  /** In bytes per ms, the rate the newest of them came at. */
  private rate = 0
  /** The floor the opening ANNOUNCE named. */
  private floor = 0
  /** The bytes of the messages it took in all, headers and announcements included, and when it last acknowledged. */
  took = 0
  protected acknowledgedTook = 0
  protected newestAt = 0
  /** When the newest data frame arrived, and when the newest the last acknowledgement covered did. */
  private newestDataAt: number | null = null
  private acknowledgedDataAt: number | null = null
  protected ackedAt = Number.NEGATIVE_INFINITY
  private ackTimer: ReturnType<typeof setTimeout> | null = null
  private ackDue = 0
  /** The acknowledgement the timer sends goes alone (see `acknowledgeIn`). */
  private ackAlone = false

  /** An acknowledgement waits to go in the message its side sends next (see `PieceSender.carry`). */
  private carrying = false

  constructor(
    private readonly send: (frame: Uint8Array<ArrayBuffer>) => void,
    /** Its side's `PieceSender.carry`. */
    private readonly carry: (frame: () => Uint8Array<ArrayBuffer> | null) => void,
  ) {}

  /** A whole frame of `bytes` arrived at `at`, as an announced message if `announced`. */
  protected abstract tookFrame(bytes: number, data: boolean, announced: boolean, at: number): void
  /** A piece arrived and made its frame `whole`, a data frame if `data`, as an announced message if `announced`. */
  protected abstract tookPiece(whole: boolean, data: boolean, announced: boolean): void
  /** An ANNOUNCE that arrived at `at` closed an announcement. */
  protected abstract tookClose(at: number): void
  /** How long an acknowledgement waits from now. */
  protected abstract ackWait(): number

  /** A whole frame of `bytes` arrived at `at`, `riding` if ahead of its message's last frame, which alone dates it. */
  arrived(bytes: number, data: boolean, at: number, riding: boolean): void {
    this.took += bytes
    this.tookFrame(bytes, data, !riding && this.paced(bytes, at), at)
  }

  /** Takes an ANNOUNCE that arrived at `at`: one of no bytes opens an announcement, one of its `bytes` closes it. */
  announce(bytes: number, at: number, floor = 0): void {
    this.took += ANNOUNCE_BYTES
    if (this.announced === null) {
      assertProtocol(bytes === 0, 'ANNOUNCE of bytes with none announced')
      this.announced = 0
      this.announcedAt = at
      this.floor = floor
      return
    }
    assertProtocol(bytes === this.announced, 'ANNOUNCE of other bytes than followed the one before')
    this.announced = null
    this.tookClose(at)
  }

  /** Until when it waits for the next message of what was announced; 0 for none. */
  expectedBy(pingDeadline: number): number {
    const rate = this.rate || this.floor
    if (this.announced === null || rate === 0) return 0
    const next = announcedNext(this.lastAnnounced, this.floor)
    return (
      this.announcedAt + (WIRE_ANNOUNCED_WAIT_MARGIN * next) / Math.max(rate, WIRE_PROBE_FIRST_BYTES / pingDeadline)
    )
  }

  /** Dates a message of `bytes` that arrived at `at`, if it was announced, and returns whether it was. */
  private paced(bytes: number, at: number): boolean {
    if (this.announced === null) return false
    this.rate = bytes / Math.max(at - this.announcedAt, WIRE_PROBE_CLOCK_MS)
    this.announcedAt = at
    this.lastAnnounced = bytes
    this.announced += bytes
    return true
  }

  /** Takes a LENGTH of `bytes`, after the frame it measures: taken, as an ANNOUNCE is, not dated. */
  tookLength(bytes: number): void {
    this.took += bytes
  }

  /** Bytes it holds of a frame not yet whole. */
  get held(): number {
    return this.received
  }

  /** Takes a piece that arrived at `at`, and returns the frame once its last piece arrived. */
  add(total: number, piece: Uint8Array, at: number): Uint8Array<ArrayBuffer> | null {
    const bytes = piece.byteLength
    this.took += PIECE_HEADER_BYTES
    if (this.received === 0) {
      this.total = total
      if (bytes === 0) {
        this.probing = 2
        return null
      }
      this.data = isSequencedTag(piece[0]!)
    }
    assertProtocol(total === this.total && bytes > 0 && bytes <= total - this.received, 'PIECE not cut as its frame is')
    if (this.received + bytes > this.frame.byteLength) {
      const grown = new Uint8Array(Math.min(total, Math.max(2 * this.frame.byteLength, this.received + bytes)))
      grown.set(this.frame.subarray(0, this.received))
      this.frame = grown
    }
    this.frame.set(piece, this.received)
    this.received += bytes
    this.took += bytes
    this.date(this.data, at)
    const announced = this.paced(bytes, at)
    const whole = this.received === total
    if (this.probing > 0) {
      this.probing--
      this.acknowledge()
    } else this.tookPiece(whole, this.data, announced)
    if (!whole) return null
    const frame = this.frame
    this.frame = new Uint8Array(0)
    this.received = this.probing = 0
    return frame
  }

  /** Dates what arrived at `at`, as data if `data`: the newest the next acknowledgement covers. */
  protected date(data: boolean, at: number): void {
    this.newestAt = at
    if (data) this.newestDataAt = at
  }

  /** Acknowledges once `ackWait` allows. */
  protected acknowledgeSoon(): void {
    const wait = this.ackWait()
    if (wait > 0) this.acknowledgeIn(wait)
    else this.acknowledge()
  }

  /** Acknowledges once `ackWait` allows, in the message its side sends next then. */
  protected acknowledgeWithNextSoon(): void {
    const wait = this.ackWait()
    if (wait > 0) this.armAck(wait)
    else this.acknowledgeWithNext()
  }

  /** Acknowledges `ms` from now, or sooner where it already would. */
  protected acknowledgeIn(ms: number): void {
    this.ackAlone = true
    this.armAck(ms)
  }

  /** Acknowledges `ms` from now, or sooner where it already would: alone if `ackAlone`, else with the next message. */
  private armAck(ms: number): void {
    const due = performance.now() + ms
    if (this.ackTimer !== null) {
      if (this.ackDue <= due) return
      clearTimeout(this.ackTimer)
    }
    this.ackDue = due
    this.ackTimer = unrefTimer(
      setTimeout(() => {
        this.ackTimer = null
        if (this.ackAlone) this.acknowledge()
        else this.acknowledgeWithNext()
      }, ms),
    )
  }

  private acknowledge(): void {
    this.send(this.takeAcknowledgement())
  }

  /** Acknowledges in the message its side sends next. */
  protected acknowledgeWithNext(): void {
    if (this.carrying) return
    this.carrying = true
    this.carry(() => (this.carrying ? this.takeAcknowledgement() : null))
  }

  private takeAcknowledgement(): Uint8Array<ArrayBuffer> {
    if (this.ackTimer !== null) clearTimeout(this.ackTimer)
    this.ackTimer = null
    this.ackAlone = this.carrying = false
    this.ackedAt = performance.now()
    this.acknowledgedTook = this.took
    const span =
      this.acknowledgedDataAt === null || this.newestDataAt === this.acknowledgedDataAt
        ? 0
        : Math.round((this.newestDataAt! - this.acknowledgedDataAt) * 1000)
    this.acknowledgedDataAt = this.newestDataAt
    // Rounded down: its sender then dates the arrival no earlier than it was, and takes a rate no faster than the link's.
    return encode.piecesAck(this.took, Math.floor(this.ackedAt - this.newestAt), span)
  }
}
