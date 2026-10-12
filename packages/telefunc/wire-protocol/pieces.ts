export { PieceSender, PieceReceiver }
export type { SendRoom }

import {
  WIRE_ANNOUNCED_MIN_BYTES,
  WIRE_ANNOUNCED_WAIT_MARGIN,
  WIRE_ARENA_BYTES,
  WIRE_GATHER_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_PROBE_CLOCK_MS,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_SEND_ACK_GAP_MS,
  WIRE_SEND_ACK_MIN_BYTES,
  WIRE_SEND_ACK_MS,
} from './constants.js'
import { Link } from './link.js'
import { LENGTH_BYTES, TAG, assertProtocol, encode, fixedBytes, isSequencedTag } from './shared-ws.js'
import { assert } from '../utils/assert.js'
import { Fifo } from '../utils/Fifo.js'
import { DOUBLE_ZERO } from '../utils/DOUBLE_ZERO.js'
import { unrefTimer } from '../utils/unrefTimer.js'

/** Whether its receiver acknowledges a frame of this size. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECES_ACK_BYTES
}

/** The most the announced message after one of `last` bytes carries; after none, what crosses before any is dated, unless
 *  its ANNOUNCE named a `floor` that dates it. */
function announcedNext(last: number, floor: number): number {
  return last === 0 && floor === 0 ? WIRE_PROBE_FIRST_BYTES : Math.max(2 * last, WIRE_ANNOUNCED_MIN_BYTES)
}

/** The bytes a PIECE and an ANNOUNCE add to what they carry. */
const PIECE_HEADER_BYTES = encode.piece(0, new Uint8Array(0)).byteLength
const ANNOUNCE_BYTES = encode.announce(0, 0).byteLength

/** How much data a sender hands its socket: a page's `SendBudget`. */
type SendRoom = {
  /** With a rate measured, the most bytes of data it hands the socket now. */
  dataRoom(): number
  /** It handed the socket `bytes`, of data or not. */
  handed(bytes: number, data: boolean): void
}

const UNBOUNDED: SendRoom = { dataRoom: () => Number.POSITIVE_INFINITY, handed: () => {} }

/** One WebSocket's: the whole frames it sends in one turn go in one message, as many bytes as a frame could carry whole
 *  and unannounced, `WIRE_GATHER_BYTES` at most; data starts a message of its own after a frame that goes at once, and
 *  where a room holds data back, after any. A frame it carries (see `carry`) rides on the next message, whatever that
 *  is. A frame that may take a ping interval to cross goes in pieces, so its receiver sees it arrive. With a rate
 *  measured, no frame or piece it sends carries more than `wholeUpTo` bytes of the frame. With none, a frame
 *  over `wholeUpTo` starts with a probe pair, whose acknowledgements measure the rate, and goes at once behind an
 *  ANNOUNCE, as does each frame sent after it in its task, and in the tasks while the probe is out, whole or in pieces as
 *  `announcedNext` allows; as each task ends, an ANNOUNCE of their bytes closes the announcement. So does a smaller
 *  data frame, unless it crosses within its ping deadline: one of `WIRE_PROBE_FIRST_BYTES` on any link, a larger one at
 *  the floor, the rate the link carries at least by what its receiver took by an answer, a heartbeat or an acknowledged
 *  announcement. A smaller control frame, such as the handshake's, goes whole. With a rate measured, a data frame past
 *  its room (see `SendRoom`) goes in pieces as far as that goes, and its rest waits for `sendHeld`: the frames after it
 *  wait too, but those that aren't data and go whole, as a window update does.
 */
class PieceSender {
  /** The frames and pieces sent and not yet acknowledged that measure the rate: the bytes that crossed before each
   *  arrived, when it was sent, the bytes sent through it, and whether it was a probe's. */
  private readonly unacknowledged = new Fifo<{ crossing: number; at: number; end: number; probe: boolean }>()
  /** The bytes the receiver has acknowledged; the first frame's, and when it was sent. */
  acknowledgedBytes = 0
  private firstBytes = 0
  private firstAt = DOUBLE_ZERO
  /** The bytes of the messages handed to the socket, headers and announcements included, as its receiver counts them. */
  transmittedBytes = 0
  /** A probe out: the bytes handed to the socket through it, and where its second piece starts, for a pair. */
  private probe: { end: number; second: number | null } | null = null
  /** The rest of a data frame, waiting for room. */
  private rest: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** The frames sent after the rest, but those that pass it. */
  private readonly held = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The bytes waiting for room. */
  heldBytes = 0
  /** The announcement its task's frames go behind, open until the task ends: where it starts, of all it sent, when it
   *  opened, and the bytes of the frames sent behind it. */
  private open: { start: number; at: number; bytes: number } | null = null
  /** The bytes of the newest message it announced; each it announced and its receiver hasn't acknowledged: where it ends,
   *  of all it sent, and when it went; and when the newest acknowledgement came. */
  private lastAnnounced = 0
  private readonly announcedEnds = new Fifo<{ end: number; at: number }>()
  private acknowledgedAt = DOUBLE_ZERO
  /** Each announcement its receiver hasn't acknowledged: where its bytes start and end, of all it sent, and when. */
  private readonly announcements = new Fifo<{ start: number; end: number; at: number }>()
  /** In bytes per ms, the floor its newest ANNOUNCE named (see `Link.floor`). */
  private namedFloor = DOUBLE_ZERO
  /** With no rate measured, a frame this size or smaller goes unannounced. */
  private unannouncedUpTo = WIRE_PROBE_FIRST_BYTES + DOUBLE_ZERO
  private pingInterval = 0
  /** A frame this size or smaller goes whole, a larger one in pieces this size: what the fastest measured rate carries
   *  in a ping interval over `WIRE_PIECE_RATE_MARGIN`; with none, what a probe carries, or what the floor carries
   *  so where no room holds data back, if more. */
  private wholeUpTo = 2 * WIRE_PROBE_PAIR_BYTES + DOUBLE_ZERO
  /** The most bytes a message of whole frames carries: what a frame of them could, whole and unannounced. */
  private gatherUpTo = WIRE_PROBE_FIRST_BYTES + DOUBLE_ZERO
  /** The largest message sent: a browser may count a message in full while sending it. */
  private largest = 0
  /** The frames the next message it sends carries (see `carry`). */
  private readonly carried: (() => Uint8Array<ArrayBuffer> | null)[] = []
  /** The frames counted and not yet handed to the socket, which go in one message as this turn ends, and their bytes with
   *  the LENGTH ahead of each that one followed; whether its last frame needs a LENGTH once one follows it (see
   *  `messageFrames`), it holds frames that weren't carried, and one of them that goes at once. */
  private readonly gathered: Uint8Array<ArrayBuffer>[] = []
  private gatheredBytes = 0
  private lastVaries = false
  private gatheredSent = false
  private gatheredAtOnce = false
  private flushQueued = false
  /** What the messages of several frames are written in, and where the next starts (see `WIRE_ARENA_BYTES`). */
  private arena = new Uint8Array(0)
  private arenaAt = 0

  constructor(
    private readonly transmit: (message: Uint8Array<ArrayBuffer>) => void,
    /** The bytes its socket holds unsent. */
    private readonly queued: () => number,
    /** Its wire's: the rates it measures go there. */
    private readonly link = new Link(),
    private readonly room = UNBOUNDED,
  ) {}

  /** The bytes handed to the socket, and of the frames sent, those it holds. */
  get sentBytes(): number {
    return this.transmittedBytes + this.heldBytes
  }

  /** The bytes of the frames sent that its socket doesn't have yet. */
  get unhandedBytes(): number {
    return this.heldBytes + this.gatheredBytes
  }

  /** The next message it hands the socket carries the frame `frame` returns as that message is sent, if any, ahead of
   *  it: a PIECES_ACK, or a WINDOW that only acknowledges. */
  carry(frame: () => Uint8Array<ArrayBuffer> | null): void {
    this.carried.push(frame)
  }

  /** Sends `frame`, whole or in pieces. */
  send(frame: Uint8Array<ArrayBuffer>, pingInterval: number): void {
    if (this.sentBytes === 0) {
      this.firstBytes = frame.byteLength
      this.firstAt = performance.now()
    }
    this.takeCarried()
    if (pingInterval !== this.pingInterval) {
      this.pingInterval = pingInterval
      this.setWholeUpTo()
    }
    const tag = frame[0]!
    if (tag === TAG.WINDOW || tag === TAG.MSG_WINDOW) this.link.refreshes.handed(frame.byteLength)
    if (tag === TAG.PIECES_ACK) {
      this.link.acks.handed(frame.byteLength)
      // It passes what waits: the probe its peer waits on may be its own.
      if (this.open === null) return this.pass(frame)
    }
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

  /** Whether `frame`, sent while the rest of a data frame waits for room, goes ahead of it: a frame that isn't data and
   *  goes whole, as a flow-control frame does. */
  private passesRest(frame: Uint8Array<ArrayBuffer>): boolean {
    return this.rest !== null && this.probe === null && !isSequencedTag(frame[0]!) && frame.byteLength <= this.wholeUpTo
  }

  /** Hands the socket `frame`, whole or in pieces. */
  private pass(frame: Uint8Array<ArrayBuffer>): void {
    const bytes = frame.byteLength
    if (bytes > this.largest) this.largest = bytes
    if (bytes > this.wholeUpTo || (this.link.fastest > 0 && isSequencedTag(frame[0]!) && bytes > this.room.dataRoom()))
      return this.cut(frame, 0)
    this.gather(frame, isSequencedTag(frame[0]!))
  }

  /** Counts `frame` into the message it gathers, after handing the socket the frames sent before it if `frame` would take
   *  that past `gatherUpTo`, or is data that would hold back a frame that goes at once, or that a room holds back: its room
   *  times each data frame from when it's handed (see `SendRoom`), and a frame arrives with those after it in its message.
   *  What was carried rides on it. */
  private gather(frame: Uint8Array<ArrayBuffer>, data: boolean): void {
    const bytes = frame.byteLength
    const length = this.lastVaries ? LENGTH_BYTES : 0
    if (
      this.gatheredSent &&
      ((data && (this.gatheredAtOnce || this.room !== UNBOUNDED)) ||
        this.gatheredBytes + length + bytes > this.gatherUpTo)
    )
      this.flush()
    this.count(frame, data)
    if (acknowledges(bytes)) this.track(bytes, performance.now(), false)
    this.add(frame)
    this.gatheredSent = true
    if (!data) this.gatheredAtOnce = true
  }

  /** Counts `frame` as it joins what it gathers, after the LENGTH the frame before it needs now. */
  private count(frame: Uint8Array<ArrayBuffer>, data: boolean): void {
    if (this.lastVaries) {
      this.transmitted(LENGTH_BYTES, false)
      this.gatheredBytes += LENGTH_BYTES
    }
    this.transmitted(frame.byteLength, data)
  }

  private add(frame: Uint8Array<ArrayBuffer>): void {
    this.gathered.push(frame)
    this.gatheredBytes += frame.byteLength
    this.lastVaries = fixedBytes(frame[0]!) === 0
    if (this.flushQueued) return
    this.flushQueued = true
    queueMicrotask(this.flushTurn)
  }

  /** What was carried waits for the next message, as does what it gathered while an announcement is open, which rides on
   *  the next message behind it, or on its close. */
  private readonly flushTurn = (): void => {
    this.flushQueued = false
    if (this.gatheredSent && this.open === null) this.flush()
  }

  /** Hands the socket `message`, the frames carried riding ahead of it (see `handGathered`). */
  private hand(message: Uint8Array<ArrayBuffer>): void {
    if (this.gathered.length === 0) return this.transmit(message)
    // Only what was carried rides, of bytes its tag fixes: a LENGTH ahead of it would count after `message`.
    assert(!this.lastVaries)
    this.gathered.push(message)
    this.gatheredBytes += message.byteLength
    this.flush()
  }

  /** Before a message that gathers nothing: the frames sent before it don't wait for its bytes, but those carried, which
   *  ride on it. */
  private handGathered(): void {
    if (this.gatheredSent) this.flush()
  }

  /** Hands the socket what it gathered, in one message. */
  flush(): void {
    const n = this.gathered.length
    if (n === 0) return
    const message = n === 1 ? this.gathered[0]! : encode.message(this.gathered, this.gatheredBytes, this.carve())
    this.gathered.length = 0
    this.gatheredBytes = 0
    this.lastVaries = this.gatheredSent = this.gatheredAtOnce = false
    if (message.byteLength > this.largest) this.largest = message.byteLength
    this.transmit(message)
  }

  /** The next `gatheredBytes` of the arena; where they don't fit, of a new one twice its size or theirs,
   *  `WIRE_ARENA_BYTES` at most. A message larger than that has its own buffer. */
  private carve(): Uint8Array<ArrayBuffer> {
    const bytes = this.gatheredBytes
    if (bytes > WIRE_ARENA_BYTES) return new Uint8Array(bytes)
    if (this.arenaAt + bytes > this.arena.byteLength) {
      this.arena = new Uint8Array(Math.max(bytes, Math.min(WIRE_ARENA_BYTES, 2 * this.arena.byteLength)))
      this.arenaAt = 0
    }
    const at = this.arenaAt
    this.arenaAt += bytes
    return this.arena.subarray(at, this.arenaAt)
  }

  /** Hands the socket `frame` from `offset` in pieces, of data as far as its room goes, and holds the rest. */
  private cut(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    this.handGathered()
    const bytes = frame.byteLength
    const now = performance.now()
    const room = Math.max(0, Math.floor(this.room.dataRoom()))
    const end = isSequencedTag(frame[0]!) ? Math.min(bytes, offset + room) : bytes
    if (end === offset) return this.hold(frame, offset)
    // The first of a frame over `wholeUpTo` is empty: its receiver hears of the frame at once, and acknowledges its first
    // two pieces at once. One cut only for room is no measure.
    if (offset === 0 && bytes > this.wholeUpTo) this.sendEmptyPiece(bytes)
    const size = Math.max(1, Math.floor(this.wholeUpTo))
    const handed = end - offset + Math.ceil((end - offset) / size) * PIECE_HEADER_BYTES
    this.transmitted(handed, isSequencedTag(frame[0]!))
    if (acknowledges(bytes)) this.track(handed, now, false)
    for (let at = offset; at < end; at += size)
      this.hand(encode.piece(bytes, frame.subarray(at, Math.min(end, at + size))))
    if (end < bytes) this.hold(frame, end)
  }

  /** Counts `bytes` handed to the socket, of data or not. */
  private transmitted(bytes: number, data: boolean): void {
    this.transmittedBytes += bytes
    this.room.handed(bytes, data)
  }

  /** Hands the socket the empty piece a frame of `total` bytes starts with. */
  private sendEmptyPiece(total: number): void {
    this.transmitted(PIECE_HEADER_BYTES, false)
    this.hand(encode.piece(total, new Uint8Array(0)))
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
      this.transmitted(ANNOUNCE_BYTES, false)
      this.hand(encode.announce(0, floorPerS))
      queueMicrotask(this.closeAnnouncement)
    }
    this.open.bytes += bytes
    if (bytes > this.largest) this.largest = bytes
    if (this.link.fastest === 0 && this.probe === null && bytes > this.wholeUpTo) this.sendProbe(frame, now)
    else this.sendAnnounced(frame, 0, now)
  }

  /** Closes the announcement of this task with an ANNOUNCE of the bytes of the frames sent behind it. */
  private readonly closeAnnouncement = (): void => {
    const { start, at, bytes } = this.open!
    this.open = null
    this.transmitted(ANNOUNCE_BYTES, false)
    this.hand(encode.announce(bytes, 0))
    this.announcements.push({ start, end: this.transmittedBytes, at })
  }

  /** Sends `frame` as two pieces back to back, whose arrivals its receiver dates apart and acknowledges at once, then the
   *  rest of it. */
  private sendProbe(frame: Uint8Array<ArrayBuffer>, now: number): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    this.sendEmptyPiece(bytes)
    for (const [at, size] of [
      [0, WIRE_PROBE_FIRST_BYTES],
      [WIRE_PROBE_FIRST_BYTES, WIRE_PROBE_PAIR_BYTES],
    ] as const) {
      this.transmitted(PIECE_HEADER_BYTES + size, data)
      this.track(PIECE_HEADER_BYTES + size, now, true)
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, this.transmittedBytes, now)
    }
    // Its receiver dates only data apart.
    const second = data ? this.transmittedBytes - PIECE_HEADER_BYTES - WIRE_PROBE_PAIR_BYTES : null
    this.probe = { end: this.transmittedBytes, second }
    this.sendAnnounced(frame, WIRE_PROBE_FIRST_BYTES + WIRE_PROBE_PAIR_BYTES, now)
  }

  /** Sends `frame` from `offset` as announced messages: whole if `announcedNext` allows, else in pieces each as large as
   *  it allows after the one before, but the last two, cut so the last is the larger, which the next may carry twice. */
  private sendAnnounced(frame: Uint8Array<ArrayBuffer>, offset: number, now: number): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    if (offset === 0 && bytes <= announcedNext(this.lastAnnounced, this.namedFloor)) {
      this.transmitted(bytes, data)
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
    this.transmitted(handed, data)
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
    this.hand(message)
  }

  private track(crossingBytes: number, at: number, probe: boolean): void {
    // What its socket holds unsent crosses before it, and what it gathered ahead of it; a browser may count the
    // message it is sending in full, so less the largest message.
    this.unacknowledged.push({
      crossing: Math.max(0, this.queued() + this.gatheredBytes - this.largest) + crossingBytes,
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
            // Only a sender that holds data back needs the rate before a frame the floor carries in time.
            this.room === UNBOUNDED ? (this.link.floor * this.pingInterval) / WIRE_PIECE_RATE_MARGIN : 0,
          )
    // What crosses within its ping deadline, twice the interval: on any link a PING crosses, or at the floor.
    this.unannouncedUpTo = Math.min(
      this.wholeUpTo,
      Math.max(WIRE_PROBE_FIRST_BYTES, this.link.floor * 2 * this.pingInterval),
    )
    this.gatherUpTo = Math.min(WIRE_GATHER_BYTES, this.link.fastest > 0 ? this.wholeUpTo : this.unannouncedUpTo)
  }

  /** Until when its receiver, which acknowledges each announced message, may take to acknowledge the next, at the rate it
   *  measured, or with none, its floor: twice the time that message takes from when it went, or the newest
   *  acknowledgement came if later; 0 for none. */
  get acknowledgedBy(): number {
    const next = this.announcedEnds.peek()
    const rate = this.link.fastest || this.link.floor
    if (next === undefined || rate === 0) return 0
    const from = Math.max(next.at, this.acknowledgedAt)
    return from + (WIRE_ANNOUNCED_WAIT_MARGIN * (next.end - this.acknowledgedBytes)) / rate
  }

  /** Takes a PIECES_ACK: its receiver took `bytes` of the messages in all (mod 2^32), the newest `heldMs` before it sent
   *  the ack, `spanUs` after the newest data the ack before covered. Returns false if that acknowledges nothing sent, or
   *  more than was sent. */
  acknowledged(bytes: number, heldMs: number, spanUs: number): boolean {
    const newly = (bytes - this.acknowledgedBytes) >>> 0
    if (newly < 1 || newly > this.transmittedBytes - this.acknowledgedBytes) return false
    const probe = this.probe
    // The ack of a pair's second piece alone dates it apart from the first: the rate the path took them at.
    if (probe !== null && this.acknowledgedBytes === probe.second && newly === probe.end - probe.second) {
      this.rise(newly / Math.max(spanUs / 1000, WIRE_PROBE_CLOCK_MS), performance.now())
    }
    this.acknowledgedBytes += newly
    while (this.announcedEnds.length > 0 && this.announcedEnds.peek()!.end <= this.acknowledgedBytes)
      this.announcedEnds.shift()
    this.acknowledgedAt = performance.now()
    const arrived = this.acknowledgedAt - heldMs
    const oldest = this.announcements.peek()
    if (oldest !== undefined && oldest.end <= this.acknowledgedBytes) {
      let end = oldest.end
      while (this.announcements.length > 0 && this.announcements.peek()!.end <= this.acknowledgedBytes)
        end = this.announcements.shift()!.end
      // What it acknowledged went no earlier than the oldest and arrived by the newest: at most the link's rate.
      this.raiseFloor((end - oldest.start) / Math.max(arrived - oldest.at, WIRE_PROBE_CLOCK_MS))
    }
    while (this.unacknowledged.length > 0 && this.unacknowledged.peek()!.end <= this.acknowledgedBytes) {
      const sent = this.unacknowledged.shift()!
      // Within one tick of a coarsened clock a sample says nothing of the rate, but a probe's, which is small: it
      // crossed within a tick.
      if (sent.probe) this.rise(sent.crossing / Math.max(arrived - sent.at, WIRE_PROBE_CLOCK_MS), arrived)
      else if (arrived > sent.at) this.rise(sent.crossing / (arrived - sent.at), arrived)
    }
    if (probe !== null && this.acknowledgedBytes >= probe.end) {
      this.probe = null
      if (this.rest !== null) this.release()
    }
    return true
  }

  /** Takes a PING or PONG that arrived at `at`, saying its receiver took `bytes` of the messages in all (mod 2^32): as many
   *  crossed since the first was sent. Returns false if that is more than were sent. */
  heartbeat(bytes: number, at: number): boolean {
    // Behind the acknowledged bytes if a PIECES_ACK passed it while it waited behind a probe.
    const took = this.acknowledgedBytes + ((bytes - this.acknowledgedBytes) | 0)
    if (took > this.transmittedBytes) return false
    this.raiseFloor(took / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
    return true
  }

  /** Takes a frame that arrived at `at` from a peer that sends none before the first frame it was sent arrived: that frame
   *  crossed by then. */
  answered(at: number): void {
    this.raiseFloor(this.firstBytes / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
  }

  private raiseFloor(floor: number): void {
    if (this.link.raiseFloor(floor)) this.setWholeUpTo()
  }

  private rise(rate: number, at: number): void {
    if (this.link.rise(rate, at)) this.setWholeUpTo()
  }

  /** Sends what waits for room, as far as its room goes now. */
  sendHeld(): void {
    if (this.rest === null || this.probe !== null) return
    this.takeCarried()
    this.release()
  }

  /** Counts the frames carried into what it gathers, ahead of what is sent next, whatever that holds. */
  private takeCarried(): void {
    if (this.carried.length === 0) return
    for (const take of this.carried.splice(0)) {
      const frame = take()
      if (frame === null) continue
      ;(frame[0] === TAG.PIECES_ACK ? this.link.acks : this.link.refreshes).handed(frame.byteLength)
      this.count(frame, false)
      this.add(frame)
    }
  }

  /** Sends what waited for room, as far as it goes now, and the frames held behind it. */
  private release(): void {
    const { frame, offset } = this.rest!
    this.rest = null
    this.heldBytes -= frame.byteLength - offset
    this.cut(frame, offset)
    while (this.rest === null && this.held.length > 0) {
      const next = this.held.shift()!
      this.heldBytes -= next.byteLength
      this.pass(next)
    }
  }
}

/** One WebSocket's: puts a frame sent in pieces back together, whatever their sizes, and acknowledges the bytes it
 *  took. The two pieces after an empty one are acknowledged at once: their sender may measure the rate by them. While
 *  an announcement is open, the next message is waited for at the rate the newest came at, dated apart from the message
 *  before it. A receiver whose peer holds data back by what it acknowledges (`peerHoldsBack`: the server's, see
 *  `SendBudget`) acknowledges each announced message, as its peer's PINGs wait behind them, once more than
 *  `WIRE_PIECES_ACK_BYTES` of frames of any size are unacknowledged, at most one PIECES_ACK per `WIRE_SEND_ACK_GAP_MS`,
 *  data within `WIRE_SEND_ACK_MS`, and what it took in the message of its side's answer to it (`answering`). Any other
 *  acknowledges when a frame over `WIRE_PIECES_ACK_BYTES` arrived or an announcement closed, at most one PIECES_ACK per
 *  `WIRE_PIECES_ACK_GAP_MS` and none before its wire's share for them allows one (`link`, see `Link.acks`), a frame in
 *  the message its side sends next: all a sender needs that cuts its pieces by the rate it measures, and stops
 *  announcing by the rate it carries. */
class PieceReceiver {
  /** The frame not yet whole: one buffer, grown as its pieces arrive, so what it holds is what arrived. */
  private frame = new Uint8Array(0)
  private received = 0
  private total = 0
  private data = false
  /** The pieces still to acknowledge at once. */
  private probing = 0
  /** The bytes of the messages that followed the open announcement, null for none open; the newest message of them, or
   *  the ANNOUNCE before any: when it arrived, its bytes, and in bytes per ms, the rate it came at; and the floor the
   *  opening ANNOUNCE named. */
  private announced: number | null = null
  private announcedAt = DOUBLE_ZERO
  private lastAnnounced = 0
  private rate = DOUBLE_ZERO
  private floor = DOUBLE_ZERO
  /** The bytes of the messages it took in all, headers and announcements included, and when it last acknowledged. */
  took = 0
  private acknowledgedTook = 0
  private newestAt = DOUBLE_ZERO
  /** When the newest data frame arrived, and when the newest the last acknowledgement covered did. */
  private newestDataAt: number | null = null
  private acknowledgedDataAt: number | null = null
  private ackedAt = Number.NEGATIVE_INFINITY
  private ackTimer: ReturnType<typeof setTimeout> | null = null
  private ackDue = DOUBLE_ZERO
  /** The acknowledgement the timer sends goes alone (see `acknowledgeIn`). */
  private ackAlone = false

  /** An acknowledgement waits to go in the message its side sends next (see `PieceSender.carry`). */
  private carrying = false

  constructor(
    private readonly send: (frame: Uint8Array<ArrayBuffer>) => void,
    /** `link`: the page's wire, whose share for acknowledgements they wait for. `carry`: its side's
     *  `PieceSender.carry`. */
    private readonly options: {
      peerHoldsBack: boolean
      link?: Link
      carry: (frame: () => Uint8Array<ArrayBuffer> | null) => void
    },
  ) {}

  /** After a whole frame of `bytes` arrived at `at`: alone in its message, or `riding` ahead of the frame its message ends
   *  with, which dates the message. A data frame dates the ack's span: it takes the queues an upload fills, where the
   *  frames that go at once pass them. */
  arrived(bytes: number, data: boolean, at: number, riding = false): void {
    this.took += bytes
    const announced = !riding && this.paced(bytes, at)
    if (!this.options.peerHoldsBack && !acknowledges(bytes)) return
    this.date(data, at)
    if (!this.options.peerHoldsBack) this.acknowledgeSoon(true)
    else if (announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES) this.acknowledgeSoon()
  }

  /** Takes an ANNOUNCE that arrived at `at`. One of no bytes opens an announcement: frames follow it at once, over a link
   *  that carries `floor` bytes per ms at least, 0 for none named, until one of their `bytes` closes it. */
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
    if (this.options.peerHoldsBack) return
    this.date(false, at)
    this.acknowledgeSoon()
  }

  /** Until when it waits for the next message of what was announced: twice the time `announcedNext` of the newest takes at
   *  the rate it came at, or before any, at the floor, but no slower than a link a PING crosses carries,
   *  `WIRE_PROBE_FIRST_BYTES` per `pingDeadline`; 0 for none. */
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

  /** Its side answers what arrived: where its peer holds data back, the answer's message acknowledges the data it took,
   *  so the peer's next request doesn't wait for the acknowledgement's timer. */
  answering(): void {
    if (this.options.peerHoldsBack && this.took - this.acknowledgedTook >= WIRE_SEND_ACK_MIN_BYTES)
      this.acknowledgeWithNext()
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
    } else if (this.options.peerHoldsBack) {
      if (announced || this.took - this.acknowledgedTook > WIRE_PIECES_ACK_BYTES) this.acknowledgeSoon()
    } else if (whole && acknowledges(total)) this.acknowledgeSoon(true)
    if (!whole) return null
    const frame = this.frame
    this.frame = new Uint8Array(0)
    this.received = this.probing = 0
    return frame
  }

  private date(data: boolean, at: number): void {
    this.newestAt = at
    if (!data) return
    this.newestDataAt = at
    if (this.options.peerHoldsBack && this.took - this.acknowledgedTook >= WIRE_SEND_ACK_MIN_BYTES)
      this.acknowledgeIn(WIRE_SEND_ACK_MS)
  }

  /** Acknowledges once the gap and the share allow; `withNext`, in the message its side sends next then: an
   *  acknowledgement that only measures the rate, which its own delay dates, as a peer that holds nothing back takes
   *  one of a frame. */
  private acknowledgeSoon(withNext = false): void {
    const wait = Math.max(
      this.ackedAt + (this.options.peerHoldsBack ? WIRE_SEND_ACK_GAP_MS : WIRE_PIECES_ACK_GAP_MS) - this.newestAt,
      this.options.link?.acks.wait() ?? 0,
    )
    if (wait > 0) this.acknowledgeIn(wait, withNext)
    else if (withNext) this.acknowledgeWithNext()
    else this.acknowledge()
  }

  /** Acknowledges `ms` from now, or sooner where it already would; `withNext`, as `acknowledgeSoon` says, unless an
   *  acknowledgement that doesn't wait is due by then too. */
  private acknowledgeIn(ms: number, withNext = false): void {
    if (!withNext) this.ackAlone = true
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
    this.send(this.acknowledgement())
  }

  /** Acknowledges in the message its side sends next. */
  private acknowledgeWithNext(): void {
    if (this.carrying) return
    this.carrying = true
    this.options.carry(() => (this.carrying ? this.acknowledgement() : null))
  }

  private acknowledgement(): Uint8Array<ArrayBuffer> {
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
