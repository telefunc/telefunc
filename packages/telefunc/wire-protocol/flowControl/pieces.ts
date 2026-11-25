export { PieceSender, PieceReceiver, acknowledges }
export type {
  FrameArrival,
  PieceArrival,
  PieceReceiverOptions,
  PieceReceiverOwner,
  PieceSenderOptions,
  PieceSenderOwner,
  PieceSocket,
}

import {
  WIRE_ANNOUNCED_MIN_BYTES,
  WIRE_ANNOUNCED_WAIT_MARGIN,
  WIRE_GATHER_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PIECES_ACK_GAP_MS,
  WIRE_PIECE_RATE_MARGIN,
  WIRE_PROBE_CLOCK_MS,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_SEND_AHEAD_MIN_BYTES,
  CHANNEL_TRANSPORT,
} from '../constants.js'
import type { PieceRate } from './PieceRate.js'
import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES, encode, lengthBytes, type PiecesAck } from '../wire/frame.js'
import { TAG, isPublishBody, isSequencedTag, passesData } from '../wire/tags.js'
import { readU16 } from '../wire/header.js'
import { assertProtocol } from '../wire/protocolViolation.js'
import { pingDeadline } from '../wire/pingDeadline.js'
import { assert } from '../../utils/assert.js'
import { Fifo } from '../../utils/Fifo.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

/** Whether the receiver acknowledges a frame of `bytes`. */
function acknowledges(bytes: number): boolean {
  return bytes > WIRE_PIECES_ACK_BYTES
}

/** The most bytes the announced message after one of `last` bytes may carry. With no `last` and no `floor`, it is
 *  the size of a probe's first piece. */
function announcedNext(last: number, floor: number): number {
  return last === 0 && floor === 0 ? WIRE_PROBE_FIRST_BYTES : Math.max(2 * last, WIRE_ANNOUNCED_MIN_BYTES)
}

/** The lanes of what isn't a channel's: the frame that ends a wire's data, which goes once nothing else waits, and the
 *  other wire frames too large to pass what waits. */
const FIN_LANE = -1
const CONTROL_LANE = -2

/** The bytes a PIECE and an ANNOUNCE add to what they carry. */
const PIECE_HEADER_BYTES = encode.piece(0, new Uint8Array(0)).byteLength
const ANNOUNCE_BYTES = encode.announce(0, 0).byteLength

/** Tracks what a piece sender hands its socket: a page's `ClientHeadroom` or a server's `ServerHeadroom`. */
interface PieceSenderOwner {
  /** The sender handed the socket `bytes`, or carried them into its next message; `data` says whether they are data. */
  handed(bytes: number, data: boolean, copied: boolean): void
  /** The bytes of data the socket may copy now. What goes past them waits, and the owner has it sent. */
  copyRoom?(): number
  /** The most data the path may hold: a frame past it goes in pieces, one within it waits whole for its room. */
  window?(): number
  /** The most copies the socket may hold once the receiver took all it was handed: a frame that copies past it goes in
   *  pieces, as one past the window does. */
  copyAllowance?(): number
}

/** The socket a piece sender hands its messages to: a page's `WebSocket`, a server wire's `TransportSocket`. */
interface PieceSocket {
  send(message: Uint8Array<ArrayBuffer>): void
  /** The bytes the socket holds that it has not sent yet. */
  readonly bufferedAmount: number
  /** Whether it keeps a copy of each message: a frame it holds is then no longer the sender's own bytes. */
  readonly copiesOnSend?: boolean
}

type PieceSenderOptions = {
  socket: PieceSocket
  /** The wire's ping interval, which bounds the size of the frames sent whole. */
  pingInterval: number
  /** The wire's rate record, where the rates this sender measures go. */
  pieceRate: PieceRate
  /** `cut`: data is cut to the headroom; `whole`: a frame that crosses within a ping interval and fits the window goes
   *  whole while headroom is left, and waits whole if none, so that the socket holds the frame itself and not a copy per
   *  piece; any other goes in pieces. */
  cuts: 'cut' | 'whole'
  /** Whether what waits for headroom waits in a lane per channel, the lanes taking turns, so that one channel's backlog
   *  holds up no other's; without it, all of it waits in one line. */
  lanes?: boolean
  owner: PieceSenderOwner
}

/** Sends one WebSocket's frames. The small frames one task sends go in one message, with at most one data frame on a
 *  page; a frame that could take longer than a ping interval to cross goes in pieces.
 *
 *  Its counters agree between calls:
 *  - `transmittedBytes` counts each byte once, as the socket is handed the message holding it (`countHanded`):
 *    headers, LENGTH frames and announcements included. A frame gathered or carried for the next message is counted only
 *    once that message goes (`gave`), and a carried one a message handed at once takes along just before it.
 *  - `waitingBytes` is what `rest` has left from its offset, plus every frame in `waiting`.
 *  - `acknowledgedBytes` and `heartbeatBytes` never pass `transmittedBytes`.
 *  - `unacknowledged`, `announcedEnds` and `announcements` are in order of `end`, and each `end` is past
 *    `acknowledgedBytes`: an ack drops every entry it reaches (`acknowledged`), and `probe` once it reaches its
 *    `end`. */
class PieceSender {
  private readonly socket: PieceSocket
  private pingInterval: number
  private readonly pieceRate: PieceRate
  private readonly cutsData: boolean
  private readonly copiesOnSend: boolean
  private readonly laned: boolean
  private readonly owner: PieceSenderOwner
  /** The bytes of data the socket accepts now: the headroom the current call was given, less what it handed since. */
  private headroom = 0
  /** The frames and pieces sent but not yet acknowledged, whose acknowledgements will measure the rate. */
  private readonly unacknowledged = new Fifo<{ crossing: number; at: number; end: number; probe: boolean }>()
  /** The bytes the receiver has acknowledged. */
  acknowledgedBytes = 0
  /** The first frame's bytes, and when it was sent. */
  private firstBytes = 0
  private firstAt = 0
  /** The bytes the receiver reported taking in its newest heartbeat. */
  heartbeatBytes = 0
  /** When the newest acknowledged data arrived, in ms on the receiver's clock, counted from the first data an
   *  acknowledgement covered. */
  private peerArrivedAt = 0
  /** How long the newest acknowledgement took to come back, in ms, up to the clocks' offset. */
  lastBack = 0
  /** The shortest time, in ms, an acknowledgement took to come back, up to the clocks' offset: one that waited behind
   *  no queue on the path. */
  leastBack = Number.POSITIVE_INFINITY
  /** The bytes handed to the socket, headers and announcements included, counted as the receiver counts them. */
  transmittedBytes = 0
  /** The probe in flight. `end` is the count of bytes handed to the socket when the probe ended; `second` is where
   *  its second piece starts, or null for a non-data frame, which the receiver does not date. */
  private probe: { end: number; second: number | null } | null = null
  /** The rest of a data frame, waiting for headroom. */
  private rest: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** The frames that wait for headroom, by lane (`laneOf`), each lane in order; the first lane has the turn. One that
   *  passes what waits (`passesRest`) isn't here. The rest's lane stays while it waits, empty or not. */
  private readonly lanes = new Map<number, Fifo<Uint8Array<ArrayBuffer>>>()
  /** The lane of the rest, and of the frame being sent. */
  private restLane = 0
  /** Whether the frame in pieces handed bytes since its lane's last turn. */
  private restHanded = false
  private servingLane = 0
  /** The lane of the newest publish head: its body, which names no channel, follows it there. */
  private bodyLane = 0
  /** The bytes waiting for headroom. */
  waitingBytes = 0
  /** The least headroom that lets a frame waiting for headroom go, as the sender last looked at what waits: `Infinity`
   *  where none waits for it. The owner looks again once that much is free, not as soon as any is. */
  headroomNeeded = Number.POSITIVE_INFINITY
  /** The open announcement the current task's frames go behind; a microtask closes it when the task ends. */
  private open: { start: number; at: number; bytes: number } | null = null
  /** The bytes of the newest message it announced. */
  private lastAnnounced = 0
  /** Each announced message the receiver hasn't acknowledged: where it ends among all bytes sent, and when it went. */
  private readonly announcedEnds = new Fifo<{ end: number; at: number }>()
  private acknowledgedAt = 0
  /** Each announcement the receiver hasn't acknowledged: where its bytes start and end among all bytes sent, and when
   *  it opened. */
  private readonly announcements = new Fifo<{ start: number; end: number; at: number }>()
  /** The rate floor, in bytes per ms, that the newest opening ANNOUNCE named (see `PieceRate.floor`). */
  private namedFloor = 0
  /** With no rate measured, a frame this size or smaller goes unannounced. */
  private unannouncedUpTo = WIRE_PROBE_FIRST_BYTES
  /** A frame this size or smaller goes whole, a larger one in pieces this size (see `setWholeUpTo`). */
  private wholeUpTo = 2 * WIRE_PROBE_PAIR_BYTES
  /** The most bytes the message of a task's frames carries: what one of them could, whole and unannounced. */
  private gatherUpTo = WIRE_PROBE_FIRST_BYTES
  /** What returns the frames that ride in the next message handed to the socket (see `carry`). */
  private readonly carried: (() => Uint8Array<ArrayBuffer> | null)[] = []
  /** The frames this task gathered and the frames carried, until they go to the socket in one message. */
  private readonly messages: SocketMessages
  /** Whether `endTask` is queued for this task. */
  private endQueued = false

  constructor({ socket, pingInterval, pieceRate, cuts, lanes = false, owner }: PieceSenderOptions) {
    this.socket = socket
    this.pingInterval = pingInterval
    this.pieceRate = pieceRate
    this.cutsData = cuts === 'cut'
    this.copiesOnSend = socket.copiesOnSend === true
    this.laned = lanes
    this.owner = owner
    this.messages = new SocketMessages(socket, this.gave)
    this.setWholeUpTo()
  }

  /** The bytes of the messages handed to the socket, and of the frames still waiting here. */
  get sentBytes(): number {
    return this.transmittedBytes + this.messages.ungivenBytes + this.waitingBytes
  }

  /** The bytes of the frames this task gathered, and those carried, that go with its next message. */
  get ungivenBytes(): number {
    return this.messages.ungivenBytes
  }

  /** The bytes of the frames sent that have not reached the socket yet. */
  get unhandedBytes(): number {
    return this.waitingBytes + this.messages.bytes
  }

  /** The frames waiting for headroom, in order: in each lane, a rest none of which went, then those behind it. */
  get waitingFrames(): Iterable<Uint8Array<ArrayBuffer>> {
    return this.queued()
  }

  private *queued(): Generator<Uint8Array<ArrayBuffer>> {
    for (const [key, lane] of this.lanes) {
      const rest = this.ungivenRestOf(key)
      if (rest !== null) yield rest
      yield* lane
    }
  }

  /** The rest waiting in lane `key`, where the socket has been given no byte of it. */
  private ungivenRestOf(key: number): Uint8Array<ArrayBuffer> | null {
    return this.rest !== null && this.restLane === key && this.rest.offset === 0 ? this.rest.frame : null
  }

  /** Removes the frames waiting for headroom that `drops` says go, called on each in order as `waitingFrames` names them. */
  dropWaiting(drops: (frame: Uint8Array<ArrayBuffer>) => boolean): void {
    const goes = (frame: Uint8Array<ArrayBuffer>) => {
      if (!drops(frame)) return false
      this.waitingBytes -= frame.byteLength
      return true
    }
    for (const [key, lane] of this.lanes) {
      const rest = this.ungivenRestOf(key)
      if (rest !== null && goes(rest)) this.rest = null
      lane.remove(goes)
      if (lane.length === 0 && !(this.rest !== null && this.restLane === key)) this.lanes.delete(key)
    }
  }

  /** Removes the frames of which the socket has been given no byte that `drops` says go, called on each in order: those
   *  gathered for the next message, a rest none of which went, then those waiting behind it. */
  dropUngiven(drops: (frame: Uint8Array<ArrayBuffer>) => boolean): void {
    this.messages.remove(drops)
    this.dropWaiting(drops)
  }

  /** Whether the rest of a data frame waits for headroom. */
  get dataWaits(): boolean {
    return this.rest !== null || this.lanes.size > 0
  }

  /** Whether an announcement is open: it counts every message handed till it closes, as this task ends. */
  get announcing(): boolean {
    return this.open !== null
  }

  /** Whether what waits waits for the probe's rate, which comes after its first acknowledgement. */
  get awaitsProbe(): boolean {
    return this.probe !== null && !this.cutsData
  }

  /** Puts the frame `frame()` returns at that time, if any, ahead of the next message handed to the socket. */
  carry(frame: () => Uint8Array<ArrayBuffer> | null): void {
    this.carried.push(frame)
  }

  setPingInterval(ms: number): void {
    this.pingInterval = ms
    this.setWholeUpTo()
  }

  /** Sends `frame`, whole or in pieces. `headroom` is how many bytes of data the socket accepts now; it counts only
   *  for a data frame while no data waits (see `dataWaits`). */
  send(frame: Uint8Array<ArrayBuffer>, headroom: number): void {
    this.headroom = headroom
    if (this.sentBytes === 0) {
      this.firstBytes = frame.byteLength
      this.firstAt = performance.now()
    }
    this.takeCarried()
    const tag = frame[0]!
    // While no announcement is open, a PIECES_ACK goes ahead of what waits: the peer's probe may be waiting for it.
    if (tag === TAG.PIECES_ACK && this.open === null) return this.pass(frame)
    const lane = this.laneOf(frame, tag)
    // A publish's head goes only with its body, which follows it at once, so that both are admitted as one.
    if (this.rest === null && this.lanes.size === 0 && !(this.laned && tag === TAG.PUBLISH_TO)) {
      this.servingLane = lane
      return this.start(frame)
    }
    // A probe's answer waits in its channel's lane (see `laneOf`).
    if (!(this.laned && tag === TAG.BDP_PING_ACK) && this.passesRest(frame)) return this.pass(frame)
    this.enqueue(lane, frame)
    // A sender that cuts data lets go of what waits only as its owner frees headroom.
    if (!this.cutsData && tag !== TAG.PUBLISH_TO) this.release()
  }

  /** The lane `frame` waits in: its channel's on a sender with lanes, a publish body its head's, anything else last. */
  private laneOf(frame: Uint8Array<ArrayBuffer>, tag: number): number {
    if (!this.laned) return 0
    if (isPublishBody(tag)) return this.bodyLane
    // A probe's answer goes behind what its channel has waiting, so the probe counts what its window let out.
    if (tag === TAG.BDP_PING_ACK) return readU16(frame, 1)
    if (!isSequencedTag(tag)) return tag === TAG.FIN ? FIN_LANE : CONTROL_LANE
    const ix = readU16(frame, 1)
    if (tag === TAG.PUBLISH_TO) this.bodyLane = ix
    return ix
  }

  private enqueue(key: number, frame: Uint8Array<ArrayBuffer>): void {
    let lane = this.lanes.get(key)
    if (lane === undefined) this.lanes.set(key, (lane = new Fifo()))
    lane.push(frame)
    this.waitingBytes += frame.byteLength
  }

  /** Lets what waits go as far as the headroom goes, the lanes taking turns: on its turn each hands its oldest frames, a
   *  publish's head with its body, or the next pieces of the frame it has in pieces, up to one message of the least
   *  send-ahead, so a lane of small messages moves as fast in bytes as one of large ones. A frame within the window and
   *  past the headroom keeps the turn till its room frees, so no lane's frame waits forever behind others' smaller ones. */
  private release(): void {
    this.passControl()
    let idle = 0
    for (let turn = this.lanes.entries().next(); !turn.done; turn = this.lanes.entries().next()) {
      const [key, lane] = turn.value
      const served = this.serveTurn(key, lane)
      if (served === 'held') return
      this.lanes.delete(key)
      if (served === 'went' && key === this.restLane) this.restHanded = false
      if (lane.length > 0 || (this.rest !== null && this.restLane === key)) this.lanes.set(key, lane)
      idle = served === 'went' ? 0 : idle + 1
      // A round where no lane could go: what goes next waits for headroom.
      if (idle >= this.lanes.size) return
    }
  }

  /** The wire's control frames go ahead of the channels' turns as soon as they may pass the data frame in pieces: on its
   *  turn among the channels, one would wait behind every channel that waits for headroom. */
  private passControl(): void {
    const lane = this.lanes.get(CONTROL_LANE)
    if (lane === undefined) return
    for (let frame = lane.peek(); frame !== undefined; frame = lane.peek()) {
      const passes = this.passesRest(frame)
      const rest = this.rest
      const left = rest === null ? 0 : rest.frame.byteLength - rest.offset
      // Past what may go whole, one that crosses within a ping interval at the fastest rate still goes whole ahead of a
      // frame in pieces with more left than it: behind it, it would wait out all of that.
      if (!passes && (frame.byteLength >= left || frame.byteLength > this.wholeUpTo * WIRE_PIECE_RATE_MARGIN)) break
      lane.shift()
      this.waitingBytes -= frame.byteLength
      if (passes) this.pass(frame)
      else this.sendWhole(frame, false)
    }
    if (lane.length === 0 && !(this.rest !== null && this.restLane === CONTROL_LANE)) this.lanes.delete(CONTROL_LANE)
  }

  /** Lane `key`'s turn: units while what it handed is under one message of the least send-ahead. Finishing a frame in
   *  pieces ends it, so the next lane with a large frame starts that one, instead of this lane its next. */
  private serveTurn(key: number, lane: Fifo<Uint8Array<ArrayBuffer>>): 'went' | 'passed' | 'held' {
    const before = this.transmittedBytes + this.messages.ungivenBytes
    let inPieces = this.rest !== null && this.restLane === key
    const served = this.serve(key, lane)
    if (served !== 'went') return served
    while (lane.length > 0 && this.transmittedBytes + this.messages.ungivenBytes - before < WIRE_SEND_AHEAD_MIN_BYTES) {
      if (inPieces && this.rest === null) break
      inPieces = this.rest !== null && this.restLane === key
      if (this.serve(key, lane) !== 'went') break
    }
    return 'went'
  }

  /** Lane `key`'s unit: what it hands, or whether it passes the turn on or keeps it. */
  private serve(key: number, lane: Fifo<Uint8Array<ArrayBuffer>>): 'went' | 'passed' | 'held' {
    const before = this.transmittedBytes + this.messages.ungivenBytes
    this.servingLane = key
    const rest = this.rest
    if (rest !== null && this.restLane === key) {
      // A wire's control frame keeps the turn while its probe's answer is awaited: the page reads it before any data.
      if (this.awaitsProbe) return key === CONTROL_LANE ? 'held' : 'passed'
      this.rest = null
      this.waitingBytes -= rest.frame.byteLength - rest.offset
      if (this.cutsData || rest.offset > 0) this.cut(rest.frame, rest.offset)
      else this.start(rest.frame)
      // A publish's head that went takes its body along, so that no other publish goes between them.
      const body = lane.peek()
      if (rest.frame[0] === TAG.PUBLISH_TO && this.rest === null && body !== undefined && isPublishBody(body[0])) {
        lane.shift()
        this.waitingBytes -= body.byteLength
        this.start(body)
      }
      if (this.transmittedBytes + this.messages.ungivenBytes > before) return 'went'
      // Blocked, it keeps the turn only if it handed nothing since its lane's last turn.
      return this.restHanded ? 'passed' : 'held'
    }
    const frame = lane.peek()
    if (frame === undefined) return 'passed'
    const tag = frame[0]
    // The frame that ends the wire's data goes last; a frame in pieces keeps the receiver putting it together.
    if (key === FIN_LANE && (rest !== null || this.lanes.size > 1)) return 'passed'
    if (rest !== null && (this.cutsData || !isSequencedTag(tag))) return this.cutsData ? 'held' : 'passed'
    const body = tag === TAG.PUBLISH_TO ? lane.at(1) : undefined
    const bytes = frame.byteLength + (body?.byteLength ?? 0)
    const room = this.roomFor(frame)
    if (rest !== null) {
      // Past a frame in pieces only what fits goes, whole, and a publish only where that frame is no publish's own. A
      // unit that will fit keeps the turn, or the frame in pieces would take every room that frees ahead of it.
      const publishes = isPublishBody(rest.frame[0]) || rest.frame[0] === TAG.PUBLISH_TO
      if (this.open !== null || (body !== undefined && publishes)) return 'passed'
      if (bytes > room) return isSequencedTag(tag) && bytes <= this.mostFor(frame) ? this.holds(bytes, room) : 'passed'
    } else if (!this.cutsData && isSequencedTag(tag) && bytes > room && bytes <= this.mostFor(frame)) {
      return this.holds(bytes, room)
    }
    lane.shift()
    this.waitingBytes -= frame.byteLength
    if (rest !== null) this.sendWhole(frame)
    else this.start(frame)
    // The head went, whole or in part, so its body follows it before any other lane's publish.
    if (body !== undefined && this.rest?.frame !== frame) {
      lane.shift()
      this.waitingBytes -= body.byteLength
      if (rest !== null) this.sendWhole(body)
      else this.start(body)
    }
    return 'went'
  }

  /** The bytes of data `frame` may take now: the headroom, less what waits to go with this task's message, and where the
   *  socket keeps a copy, the copy room. */
  private roomFor(frame: Uint8Array<ArrayBuffer>): number {
    const room = this.headroom - this.messages.ungivenBytes
    return this.copiesOnSend || frame.byteLength > this.wholeUpTo ? Math.min(room, this.copyRoom()) : room
  }

  private window(): number {
    return this.owner.window?.() ?? Number.POSITIVE_INFINITY
  }

  /** The most of `frame` the socket may ever take whole now: the window, and where it copies, the copy allowance. One
   *  past it goes in pieces, so a frame that waits whole always fits once what went is taken. */
  private mostFor(frame: Uint8Array<ArrayBuffer>): number {
    const copies = this.copiesOnSend || frame.byteLength > this.wholeUpTo
    return copies ? Math.min(this.window(), this.owner.copyAllowance?.() ?? Number.POSITIVE_INFINITY) : this.window()
  }

  /** Sends `frame`, which nothing waits ahead of. */
  private start(frame: Uint8Array<ArrayBuffer>): void {
    const tag = frame[0]!
    if (isSequencedTag(tag) && this.copies(frame) && this.copyRoom() < frame.byteLength) {
      // An open announcement names whole frames.
      return this.open === null ? this.cut(frame, 0) : this.wait(frame, 0)
    }
    if (this.open === null && !this.announces(frame)) return this.pass(frame)
    if (this.open === null && this.goesWhole(frame)) return this.sendWhole(frame)
    // Before any rate, a frame too small to probe the path goes behind an announcement only within the headroom.
    if (this.withinNoRate(frame)) return this.wait(frame, 0)
    this.sendBehindAnnouncement(frame)
  }

  /** Whether data `frame`, too small to probe, waits for headroom while no rate is measured. */
  private withinNoRate(frame: Uint8Array<ArrayBuffer>): boolean {
    const bytes = frame.byteLength
    return (
      !this.cutsData &&
      this.pieceRate.fastest === 0 &&
      isSequencedTag(frame[0]!) &&
      bytes <= this.wholeUpTo &&
      bytes <= this.mostFor(frame) &&
      bytes > this.headroom - this.messages.ungivenBytes
    )
  }

  private copyRoom(): number {
    return this.owner.copyRoom?.() ?? Number.POSITIVE_INFINITY
  }

  /** Whether sending `frame` now leaves a copy of it in the socket's hands: pieces do, and on some sockets all does. */
  private copies(frame: Uint8Array<ArrayBuffer>): boolean {
    return this.copiesOnSend || this.open !== null || this.announces(frame) || frame.byteLength > this.wholeUpTo
  }

  /** Whether `frame` sent now goes behind an announcement. */
  private announces(frame: Uint8Array<ArrayBuffer>): boolean {
    const bytes = frame.byteLength
    return (
      this.probe !== null ||
      (this.pieceRate.fastest === 0 &&
        bytes > this.unannouncedUpTo &&
        (bytes > this.wholeUpTo || isSequencedTag(frame[0]!)))
    )
  }

  /** With no rate measured yet, a data frame within the headroom and the least any sender sends ahead goes whole:
   *  pieces would cost a round trip. */
  private goesWhole(frame: Uint8Array<ArrayBuffer>): boolean {
    return (
      this.pieceRate.fastest === 0 &&
      this.probe === null &&
      isSequencedTag(frame[0]!) &&
      frame.byteLength <=
        Math.min(
          WIRE_SEND_AHEAD_MIN_BYTES + MESSAGE_FRAME_OVERHEAD_MAX_BYTES,
          this.headroom - this.messages.ungivenBytes,
        )
    )
  }

  /** Hands `frame` to the socket in one message and takes no rate sample from it: TCP's slow start would make the rate
   *  read low. */
  private sendWhole(frame: Uint8Array<ArrayBuffer>, data = true): void {
    this.give()
    this.countHanded(frame.byteLength, data, this.copiesOnSend)
    this.messages.hand(frame)
  }

  /** Whether `frame` may pass the waiting rest of a data frame: it `passesData` and is small enough to go whole. */
  private passesRest(frame: Uint8Array<ArrayBuffer>): boolean {
    return passesData(frame[0]!) && frame.byteLength <= this.wholeUpTo
  }

  /** Hands the socket `frame`, whole or in pieces. */
  private pass(frame: Uint8Array<ArrayBuffer>): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    // A frame within the window waits whole for its room; one past it goes in pieces of the room.
    if (data && !this.cutsData && bytes > this.headroom - this.messages.ungivenBytes) {
      if (bytes > this.mostFor(frame)) return this.cut(frame, 0)
      this.needs(bytes + this.messages.ungivenBytes)
      return this.wait(frame, 0)
    }
    if (bytes > this.wholeUpTo || (this.pieceRate.fastest > 0 && data && this.cutsData && bytes > this.headroom)) {
      return this.cut(frame, 0)
    }
    if (bytes < this.gatherUpTo) return this.gather(frame, data)
    this.give()
    this.countHanded(bytes, data, this.copiesOnSend)
    if (acknowledges(bytes)) this.track(bytes, performance.now(), false)
    this.messages.hand(frame)
  }

  /** Adds `frame` to the message of this task's frames. It starts a new message where it would take that one past
   *  `gatherUpTo`, or where it is data behind a frame that goes at once, or behind any frame on a sender that cuts
   *  data, which times data from its handing. */
  private gather(frame: Uint8Array<ArrayBuffer>, data: boolean): void {
    const bytes = frame.byteLength
    const messages = this.messages
    if (
      messages.gathered &&
      ((data && (messages.atOnce || this.cutsData)) || messages.bytes + messages.lengthAhead + bytes > this.gatherUpTo)
    ) {
      messages.flush()
    }
    messages.gather(frame)
    if (this.endQueued) return
    this.endQueued = true
    queueMicrotask(this.endTask)
  }

  /** As the task ends, what it gathered goes. */
  private readonly endTask = (): void => {
    this.endQueued = false
    if (this.messages.gathered) this.messages.flush()
  }

  /** Counts a gathered or carried `frame` as handed as its message goes, after the LENGTH `length` the frame before it
   *  needs there. */
  private readonly gave = (frame: Uint8Array<ArrayBuffer>, length: number): void => {
    if (length > 0) this.countHanded(length, false)
    const [tag] = frame
    this.countHanded(frame.byteLength, tag !== undefined && isSequencedTag(tag), this.copiesOnSend)
  }

  /** Before a message that gathers nothing: the frames sent before it go ahead, and those only carried ride on it,
   *  counted first. */
  private give(): void {
    if (this.messages.gathered) this.messages.flush()
    else this.messages.commit()
  }

  /** Hands the socket what it gathered and carries, in one message. */
  flush(): void {
    this.messages.flush()
  }

  /** The socket closed: nothing more is handed to it, not even the frames already gathered or carried for it. */
  close(): void {
    this.messages.close()
  }

  /** Hands the socket `frame` from `offset` in pieces; data goes only as far as the headroom, and the rest waits. */
  private cut(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    this.give()
    const bytes = frame.byteLength
    const now = performance.now()
    const data = isSequencedTag(frame[0]!)
    const copyRoom = data ? this.copyRoom() : Number.POSITIVE_INFINITY
    const headroom = Math.max(0, Math.floor(Math.min(this.headroom, copyRoom, this.largestCut())))
    const end = data ? Math.min(bytes, offset + headroom) : bytes
    // A piece smaller than what the receiver acknowledges at once would cost more than the wait for headroom.
    const least = Math.min(bytes - offset, WIRE_PIECES_ACK_BYTES)
    if (end - offset < least) {
      this.needs(least)
      return this.wait(frame, offset)
    }
    // An empty piece makes the receiver acknowledge the next two at once. A frame not sent whole needs that: its rate and
    // room come back with acknowledgements, and a page sends none of an unfinished frame unasked. A frame cut only for
    // headroom isn't used to measure the rate.
    const awaitsAck = end < bytes
    if (awaitsAck || (offset === 0 && bytes > this.wholeUpTo)) this.sendEmptyPiece(bytes)
    const size = awaitsAck ? Math.ceil((end - offset) / 2) : Math.max(1, Math.floor(this.wholeUpTo))
    const handed = end - offset + Math.ceil((end - offset) / size) * PIECE_HEADER_BYTES
    this.countHanded(handed, data, true)
    if (acknowledges(bytes)) this.track(handed, now, false)
    for (let at = offset; at < end; at += size) {
      this.messages.hand(encode.piece(bytes, frame.subarray(at, Math.min(end, at + size))))
    }
    if (end === bytes) return
    this.wait(frame, end)
    this.restHanded = true
  }

  /** The most data a cut hands: two pieces, each crossing within a ping deadline at the rate data has carried. */
  private largestCut(): number {
    return Math.max(
      WIRE_SEND_AHEAD_MIN_BYTES,
      (2 * this.pieceRate.carried * this.pingInterval) / WIRE_PIECE_RATE_MARGIN,
    )
  }

  /** Counts `bytes` handed to the socket, of data or not, or carried into the next message. */
  private countHanded(bytes: number, data: boolean, copied = false): void {
    this.transmittedBytes += bytes
    this.headroom -= bytes
    this.owner.handed(bytes, data, copied)
  }

  /** Hands the socket the empty piece a frame of `total` bytes starts with. */
  private sendEmptyPiece(total: number): void {
    this.countHanded(PIECE_HEADER_BYTES, false)
    this.messages.hand(encode.piece(total, new Uint8Array(0)))
  }

  /** A frame of `bytes` waits whole for its room: it goes once the headroom is `bytes` past what `room` leaves out. */
  private holds(bytes: number, room: number): 'held' {
    this.needs(bytes + this.headroom - room)
    return 'held'
  }

  /** What waits goes once the headroom is `headroom`. */
  private needs(headroom: number): void {
    this.headroomNeeded = Math.min(this.headroomNeeded, headroom)
  }

  /** `frame` from `offset` waits for headroom. */
  private wait(frame: Uint8Array<ArrayBuffer>, offset: number): void {
    this.rest = { frame, offset }
    this.restLane = this.servingLane
    this.waitingBytes += frame.byteLength - offset
    if (!this.lanes.has(this.restLane)) this.lanes.set(this.restLane, new Fifo())
  }

  /** Sends `frame` at once behind this task's announcement, first opening one with an ANNOUNCE of 0 bytes if needed. */
  private sendBehindAnnouncement(frame: Uint8Array<ArrayBuffer>): void {
    this.give()
    const bytes = frame.byteLength
    const now = performance.now()
    if (this.open === null) {
      this.open = { start: this.transmittedBytes, at: now, bytes: 0 }
      const floorPerS = Math.min(0xffffffff, Math.floor(this.pieceRate.floor * 1000))
      this.namedFloor = floorPerS / 1000
      this.countHanded(ANNOUNCE_BYTES, false)
      this.messages.hand(encode.announce(0, floorPerS))
      queueMicrotask(this.closeAnnouncement)
    }
    if (this.pieceRate.fastest === 0 && this.probe === null && bytes > this.wholeUpTo) return this.sendProbe(frame, now)
    this.open.bytes += bytes
    this.sendAnnounced(frame, 0, now)
  }

  /** Closes the announcement of this task with an ANNOUNCE of the bytes of the frames sent behind it. */
  private readonly closeAnnouncement = (): void => {
    if (this.open === null) return
    const { start, at, bytes } = this.open
    this.open = null
    this.give()
    this.countHanded(ANNOUNCE_BYTES, false)
    this.messages.hand(encode.announce(bytes, 0))
    this.announcements.push({ start, end: this.transmittedBytes, at })
  }

  /** Probes the path: sends `frame`'s first two pieces back to back, which the receiver acknowledges at once, then
   *  the rest. */
  private sendProbe(frame: Uint8Array<ArrayBuffer>, now: number): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    this.sendEmptyPiece(bytes)
    for (const [at, size] of [
      [0, WIRE_PROBE_FIRST_BYTES],
      [WIRE_PROBE_FIRST_BYTES, WIRE_PROBE_PAIR_BYTES],
    ] as const) {
      this.countHanded(PIECE_HEADER_BYTES + size, data, true)
      this.track(PIECE_HEADER_BYTES + size, now, true)
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, this.transmittedBytes, now)
    }
    // The receiver dates the two pieces apart only for a data frame.
    const second = data ? this.transmittedBytes - PIECE_HEADER_BYTES - WIRE_PROBE_PAIR_BYTES : null
    this.probe = { end: this.transmittedBytes, second }
    const sent = WIRE_PROBE_FIRST_BYTES + WIRE_PROBE_PAIR_BYTES
    if (this.cutsData) {
      this.open!.bytes += bytes
      return this.sendAnnounced(frame, sent, now)
    }
    this.open!.bytes += sent
    // The rest goes by the rate the probe measures, and what passes it goes outside the announcement.
    this.closeAnnouncement()
    this.wait(frame, sent)
  }

  /** Sends `frame` from `offset` as announced messages, none past what `announcedNext` allows after the one before. */
  private sendAnnounced(frame: Uint8Array<ArrayBuffer>, offset: number, now: number): void {
    const bytes = frame.byteLength
    const data = isSequencedTag(frame[0]!)
    if (offset === 0 && bytes <= announcedNext(this.lastAnnounced, this.namedFloor)) {
      this.countHanded(bytes, data, this.copiesOnSend)
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
    this.countHanded(handed, data, true)
    if (acknowledges(bytes)) this.track(handed, now, false)
    let at = offset
    for (const size of sizes) {
      end += PIECE_HEADER_BYTES + size
      this.sendAnnouncedMessage(encode.piece(bytes, frame.subarray(at, at + size)), size, end, now)
      at += size
    }
  }

  /** Hands the socket `message`, sent at `at` with `bytes` of announced frames, ending at `end` of all bytes sent. */
  private sendAnnouncedMessage(message: Uint8Array<ArrayBuffer>, bytes: number, end: number, at: number): void {
    this.lastAnnounced = bytes
    this.announcedEnds.push({ end, at })
    this.messages.hand(message)
  }

  private track(crossingBytes: number, at: number, probe: boolean): void {
    // What the socket holds and the task's frames cross first, less the largest message: a browser may count one
    // as held in full while it sends it.
    this.unacknowledged.push({
      crossing: Math.max(0, this.socket.bufferedAmount + this.messages.bytes - this.messages.largest) + crossingBytes,
      at,
      end: this.transmittedBytes,
      probe,
    })
  }

  /** Sets `wholeUpTo` by the rate and the ping interval. */
  private setWholeUpTo(): void {
    this.wholeUpTo =
      this.pieceRate.fastest > 0
        ? (this.pieceRate.fastest * this.pingInterval) / WIRE_PIECE_RATE_MARGIN
        : Math.max(
            2 * WIRE_PROBE_PAIR_BYTES,
            // A sender that cuts data to its headroom needs a measured rate first; any other sends whole what the
            // floor carries within a ping interval.
            this.cutsData ? 0 : (this.pieceRate.floor * this.pingInterval) / WIRE_PIECE_RATE_MARGIN,
          )
    // An unannounced frame must cross within the ping deadline: a probe's first piece on any link a PING crosses,
    // or more at the floor.
    this.unannouncedUpTo = Math.min(
      this.wholeUpTo,
      // Pieces go only on a WebSocket.
      Math.max(WIRE_PROBE_FIRST_BYTES, this.pieceRate.floor * pingDeadline(this.pingInterval, CHANNEL_TRANSPORT.WS)),
    )
    this.gatherUpTo = Math.min(WIRE_GATHER_BYTES, this.pieceRate.fastest > 0 ? this.wholeUpTo : this.unannouncedUpTo)
  }

  /** The time by which the receiver should acknowledge the next announced message; 0 for none or with no rate. */
  get acknowledgedBy(): number {
    const next = this.announcedEnds.peek()
    const rate = this.pieceRate.fastest || this.pieceRate.floor
    if (next === undefined || rate === 0) return 0
    const from = Math.max(next.at, this.acknowledgedAt)
    return from + (WIRE_ANNOUNCED_WAIT_MARGIN * (next.end - this.acknowledgedBytes)) / rate
  }

  /** Takes a PIECES_ACK; returns false if it acknowledges nothing sent, or more than was sent. */
  acknowledged({ bytes, heldMs, spanUs }: PiecesAck): boolean {
    const newly = (bytes - this.acknowledgedBytes) >>> 0
    if (newly < 1 || newly > this.transmittedBytes - this.acknowledgedBytes) return false
    const probe = this.probe
    // An ack of the probe's second piece alone gives the path's rate, by a span the clock tells from one read of both.
    const spanMs = spanUs / 1000
    if (
      probe !== null &&
      this.acknowledgedBytes === probe.second &&
      newly === probe.end - probe.second &&
      spanMs > WIRE_PROBE_CLOCK_MS
    ) {
      this.rise(newly / spanMs, performance.now(), true)
    }
    this.acknowledgedBytes += newly
    while (this.announcedEnds.length > 0 && this.announcedEnds.peek()!.end <= this.acknowledgedBytes) {
      this.announcedEnds.shift()
    }
    this.acknowledgedAt = performance.now()
    this.peerArrivedAt += spanMs
    this.lastBack = this.acknowledgedAt - heldMs - this.peerArrivedAt
    if (this.lastBack < this.leastBack) this.leastBack = this.lastBack
    const arrived = this.acknowledgedAt - heldMs
    this.sampleAnnouncements(arrived)
    this.sampleSent(arrived)
    if (probe !== null && this.acknowledgedBytes >= probe.end) this.probe = null
    return true
  }

  /** When the newest acknowledged data arrived, on this clock: its arrival on the receiver's, moved by the least
   *  time back. */
  get arrivedAt(): number {
    return this.peerArrivedAt + this.leastBack
  }

  /** What the receiver said it took: by its acknowledgements, or its newest heartbeat where that says more. */
  get confirmedBytes(): number {
    return Math.max(this.acknowledgedBytes, this.heartbeatBytes)
  }

  /** Raises the floor from the announcements an ack that arrived at `arrived` covers, and drops them. */
  private sampleAnnouncements(arrived: number): void {
    const oldest = this.announcements.peek()
    if (oldest === undefined || oldest.end > this.acknowledgedBytes) return
    let end = oldest.end
    while (this.announcements.length > 0 && this.announcements.peek()!.end <= this.acknowledgedBytes) {
      end = this.announcements.shift()!.end
    }
    // The acknowledged announcements went no earlier than the oldest opened and arrived by `arrived`, so this rate
    // is at most the link's.
    this.raiseFloor((end - oldest.start) / Math.max(arrived - oldest.at, WIRE_PROBE_CLOCK_MS))
  }

  /** Samples the rate of each sent piece an ack that arrived at `arrived` covers, and drops them. */
  private sampleSent(arrived: number): void {
    while (this.unacknowledged.length > 0 && this.unacknowledged.peek()!.end <= this.acknowledgedBytes) {
      const sent = this.unacknowledged.shift()!
      // A sample within one tick of a coarse clock says nothing, except a probe's: being small, it crossed in one.
      if (sent.probe) this.rise(sent.crossing / Math.max(arrived - sent.at, WIRE_PROBE_CLOCK_MS), arrived, true)
      else if (arrived > sent.at) this.rise(sent.crossing / (arrived - sent.at), arrived, false)
    }
  }

  /** Takes a frame that arrived at `at` saying the receiver took `bytes` (`withTook`); false if more than was sent. */
  heartbeat(bytes: number, at: number): boolean {
    // The count may trail the acknowledged bytes if a PIECES_ACK overtook it while it waited behind a probe.
    const took = this.acknowledgedBytes + ((bytes - this.acknowledgedBytes) | 0)
    if (took > this.transmittedBytes) return false
    this.heartbeatBytes = took
    this.raiseFloor(took / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
    return true
  }

  /** A frame from the peer arrived at `at`. The peer sends nothing before this side's first frame reaches it, so the
   *  first frame crossed by then. */
  answered(at: number): void {
    this.raiseFloor(this.firstBytes / Math.max(at - this.firstAt, WIRE_PROBE_CLOCK_MS))
  }

  private raiseFloor(floor: number): void {
    if (this.pieceRate.raiseFloor(floor)) this.setWholeUpTo()
  }

  private rise(rate: number, at: number, probed: boolean): void {
    if (this.pieceRate.rise(rate, at, probed)) this.setWholeUpTo()
  }

  /** Sends what waits for headroom, as far as `headroom` goes, and the frames waiting behind it. */
  sendWaiting(headroom: number): void {
    if (!this.dataWaits) return
    this.headroom = headroom
    this.headroomNeeded = Number.POSITIVE_INFINITY
    this.takeCarried()
    this.release()
  }

  /** Takes the carried frames, which go in the next message ahead of whatever is sent next. */
  private takeCarried(): void {
    if (this.carried.length === 0) return
    for (const take of this.carried.splice(0)) {
      const frame = take()
      if (frame !== null) this.messages.add(frame)
    }
  }
}

/** The frames a task gathered and the frames carried, handed to the socket in one message (the receiver splits it:
 *  `messageFrames`). */
class SocketMessages {
  private readonly socket: PieceSocket
  /** The bytes of the frames it holds, with the LENGTH ahead of each that needs one. */
  bytes = 0
  /** It holds frames gathered, not only carried. */
  gathered = false
  /** It holds a gathered frame that isn't data. */
  atOnce = false
  /** The bytes of the LENGTH its last frame needs once another follows it. */
  lengthAhead = 0
  /** The largest message handed: a browser may count a message in full while sending it. */
  largest = 0
  private readonly frames: Uint8Array<ArrayBuffer>[] = []
  /** How many of its first frames are counted as handed: carried frames a message handed at once takes along. */
  private given = 0
  /** The bytes of those. */
  private givenBytes = 0
  /** Counts a frame it holds as handed, after the LENGTH the frame before it needs: as the socket takes it. */
  private readonly give: (frame: Uint8Array<ArrayBuffer>, length: number) => void
  private closed = false

  constructor(socket: PieceSocket, give: (frame: Uint8Array<ArrayBuffer>, length: number) => void) {
    this.socket = socket
    this.give = give
  }

  /** Counts the frames it holds as handed, in order: they go to the socket now, or ride on a message that does. */
  commit(): void {
    if (this.given === this.frames.length) return
    // Carried frames counted for a cut that then waited for headroom stay for the next message, and frames carried
    // after them join it: it counts from the first not counted, after the LENGTH the frame before it needs.
    let length = 0
    let i = 0
    for (const frame of this.frames) {
      if (i++ >= this.given) this.give(frame, length)
      const [tag] = frame
      length = tag === undefined ? 0 : lengthBytes(tag)
    }
    this.given = this.frames.length
    this.givenBytes = this.bytes
  }

  /** The bytes of the frames it holds that aren't counted as handed yet. */
  get ungivenBytes(): number {
    return this.bytes - this.givenBytes
  }

  /** Lets go of the frames it holds that `drops` says go, called on each it hasn't counted, in order: their message
   *  hasn't gone, so the socket has no byte of them. */
  remove(drops: (frame: Uint8Array<ArrayBuffer>) => boolean): void {
    const kept = this.frames.filter((frame, i) => i < this.given || !drops(frame))
    if (kept.length === this.frames.length) return
    this.frames.length = 0
    this.bytes = 0
    this.lengthAhead = 0
    for (const frame of kept) this.add(frame)
  }

  private clear(): void {
    this.frames.length = 0
    this.given = this.givenBytes = 0
    this.bytes = 0
    this.lengthAhead = 0
    this.gathered = this.atOnce = false
  }

  /** Adds `frame`, which the task sent. */
  gather(frame: Uint8Array<ArrayBuffer>): void {
    this.gathered = true
    if (!isSequencedTag(frame[0]!)) this.atOnce = true
    this.add(frame)
  }

  /** Adds a gathered or carried `frame`, after the LENGTH the frame before it now needs. */
  add(frame: Uint8Array<ArrayBuffer>): void {
    this.frames.push(frame)
    this.bytes += this.lengthAhead + frame.byteLength
    this.lengthAhead = lengthBytes(frame[0]!)
  }

  /** Hands the socket `message`, with the carried frames riding ahead of it in the same message. */
  hand(message: Uint8Array<ArrayBuffer>): void {
    // Once the socket closed, what it holds stays unhanded, as it does on `flush`.
    if (this.closed) return
    // Only carried frames ride a handed message, and their tags fix their bytes, so it needs no LENGTH ahead of it.
    assert(this.lengthAhead === 0 && this.given === this.frames.length)
    this.frames.push(message)
    this.given++
    this.bytes += message.byteLength
    this.givenBytes = this.bytes
    this.flush()
  }

  /** Hands the socket what it holds, in one message. */
  flush(): void {
    const n = this.frames.length
    if (n === 0 || this.closed) return
    this.commit()
    const message = n === 1 ? this.frames[0]! : encode.message(this.frames, this.bytes)
    this.clear()
    if (message.byteLength > this.largest) this.largest = message.byteLength
    this.socket.send(message)
  }

  close(): void {
    this.closed = true
  }
}

/** A whole frame of `bytes` that arrived at `at`, a data frame if `data`, as an announced message if `announced`. */
type FrameArrival = { bytes: number; data: boolean; announced: boolean; at: number }

/** A piece that arrived and made its frame `whole`, a data frame if `data`, as an announced message if `announced`. */
type PieceArrival = { whole: boolean; data: boolean; announced: boolean }

/** Sends this side's frames on the wire, and so the receiver's acknowledgements too. */
interface PieceReceiverOwner {
  /** Sends the acknowledgement now, alone. */
  sendAck(frame: Uint8Array<ArrayBuffer>): void
  /** Sends what `take` returns in this side's next message, as `PieceSender.carry` does. */
  carryAck(take: () => Uint8Array<ArrayBuffer> | null): void
}

type PieceReceiverOptions = {
  owner: PieceReceiverOwner
}

/** Receives one WebSocket's frames: puts a frame sent in pieces back together, and acknowledges the bytes taken. */
abstract class PieceReceiver implements AckScheduleOwner {
  private readonly owner: PieceReceiverOwner
  /** The frame being put together, in one buffer that grows as its pieces arrive, so it holds only what arrived. */
  private frame = new Uint8Array(0)
  private received = 0
  protected total = 0
  private data = false
  /** How many probe pieces, after an empty first piece, are still to be acknowledged at once. */
  private probing = 0
  /** The bytes of the messages that followed the open announcement, null for none open. */
  private announced: number | null = null
  /** When the newest announced message arrived, or the opening ANNOUNCE if none has yet. */
  private announcedAt = 0
  private lastAnnounced = 0
  /** The rate, in bytes per ms, the newest announced message arrived at. */
  private rate = 0
  /** The floor the opening ANNOUNCE named. */
  private floor = 0
  /** All the bytes it took, headers and announcements included; `acknowledgedTook` is what the last ack reported. */
  took = 0
  protected acknowledgedTook = 0
  protected newestAt = 0
  /** When the newest data frame arrived, and when the newest one the last acknowledgement covered arrived. */
  private newestDataAt: number | null = null
  private acknowledgedDataAt: number | null = null
  protected ackedAt = Number.NEGATIVE_INFINITY
  private readonly ack = new AckSchedule({ owner: this })

  constructor({ owner }: PieceReceiverOptions) {
    this.owner = owner
  }

  protected abstract tookFrame(frame: FrameArrival): void
  protected abstract tookPiece(piece: PieceArrival): void
  /** An ANNOUNCE that arrived at `at` closed an announcement. */
  protected abstract tookClose(at: number): void
  /** How long an acknowledgement waits after the newest arrival. */
  protected abstract ackWait(): number

  /** A whole frame of `tag` and `bytes` arrived at `at`. Only a message's last frame dates the message: one
   *  `'riding'` ahead of it is not dated, and came after its LENGTH if its tag doesn't fix its bytes. */
  arrived(tag: number, bytes: number, at: number, arrival: 'alone' | 'riding'): void {
    this.took += arrival === 'riding' ? bytes + lengthBytes(tag) : bytes
    const announced = arrival === 'alone' && this.paced(bytes, at)
    this.tookFrame({ bytes, data: isSequencedTag(tag), announced, at })
    if (tag === TAG.PIECES_ACK_REQ) this.acknowledge()
  }

  /** Takes an ANNOUNCE that arrived at `at`: one of 0 bytes opens an announcement; one naming its bytes closes it. */
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

  /** The time by which the next announced message should arrive; 0 with no announcement open or no rate known. */
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

  /** Bytes it holds of a frame not yet whole. */
  get held(): number {
    return this.received
  }

  /** The tag of the frame not yet whole, once its first bytes came. */
  get heldTag(): number | undefined {
    return this.received > 0 ? this.frame[0] : undefined
  }

  /** Takes a piece that arrived at `at`, and returns the frame once its last piece arrived. */
  add(total: number, piece: Uint8Array, at: number): Uint8Array<ArrayBuffer> | null {
    const bytes = piece.byteLength
    this.took += PIECE_HEADER_BYTES
    if (this.received === 0) this.total = total
    assertProtocol(total === this.total && bytes <= total - this.received, 'PIECE not cut as its frame is')
    if (bytes === 0) {
      this.probing = 2
      return null
    }
    if (this.received === 0) this.data = isSequencedTag(piece[0]!)
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
    } else this.tookPiece({ whole, data: this.data, announced })
    if (!whole) return null
    const frame = this.frame
    this.frame = new Uint8Array(0)
    this.received = this.probing = 0
    return frame
  }

  /** Records `at` as the newest arrival, and the newest data arrival if `data`; the next acknowledgement covers it. */
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

  /** Acknowledges once `ackWait` allows, in the next message this side sends after that. */
  protected acknowledgeWithNextSoon(): void {
    const wait = this.ackWait()
    if (wait > 0) this.ack.withNextBy(this.newestAt + wait)
    else this.acknowledgeWithNext()
  }

  /** Acknowledges `ms` after the newest arrival, or sooner if an acknowledgement is already due earlier. */
  protected acknowledgeIn(ms: number): void {
    this.ack.aloneBy(this.newestAt + ms)
  }

  /** Acknowledges now, alone. */
  acknowledge(): void {
    this.owner.sendAck(this.takeAcknowledgement())
  }

  /** Acknowledges in the message its side sends next. */
  acknowledgeWithNext(): void {
    if (!this.ack.carry()) return
    this.owner.carryAck(() => (this.ack.carried ? this.takeAcknowledgement() : null))
    // A subscriber may send nothing for a while, and what its peer holds back waits for this.
    this.ack.aloneBy(performance.now() + WIRE_PIECES_ACK_GAP_MS)
  }

  private takeAcknowledgement(): Uint8Array<ArrayBuffer> {
    this.ack.clear()
    this.ackedAt = performance.now()
    this.acknowledgedTook = this.took
    const span =
      this.acknowledgedDataAt === null || this.newestDataAt === this.acknowledgedDataAt
        ? 0
        : Math.round((this.newestDataAt! - this.acknowledgedDataAt) * 1000)
    this.acknowledgedDataAt = this.newestDataAt
    // Rounded down, so the sender dates the arrival no earlier than it was, and takes a rate no faster than the link's.
    return encode.piecesAck(this.took, Math.floor(this.ackedAt - this.newestAt), span)
  }
}

/** The receiver an `AckSchedule` acknowledges for. */
interface AckScheduleOwner {
  acknowledge(): void
  acknowledgeWithNext(): void
}

type AckScheduleOptions = {
  owner: AckScheduleOwner
}

/** Decides when a receiver's acknowledgement goes: when its timer fires, alone or in this side's next message, or
 *  carried in that next message. A timer to go alone keeps running while it is carried: whichever comes first
 *  sends it. */
class AckSchedule {
  private readonly owner: AckScheduleOwner
  private timer: ReturnType<typeof setTimeout> | null = null
  private due = 0
  /** Whether the timer sends the acknowledgement alone. Once a caller asks for that, it stays so, at the sooner due. */
  private alone = false
  private carrying = false

  constructor({ owner }: AckScheduleOptions) {
    this.owner = owner
  }

  /** Whether the acknowledgement waits to go in this side's next message (see `PieceSender.carry`). */
  get carried(): boolean {
    return this.carrying
  }

  /** Acknowledges by `due`, or sooner if already due earlier, in this side's next message unless it goes alone. */
  withNextBy(due: number): void {
    this.arm(due)
  }

  /** Acknowledges alone by `due`, or sooner if already due earlier. */
  aloneBy(due: number): void {
    this.alone = true
    this.arm(due)
  }

  // A due counted from the arrival is the same for every frame of a burst, however late each was taken: one timer.
  private arm(due: number): void {
    if (this.timer !== null) {
      if (this.due <= due) return
      clearTimeout(this.timer)
    }
    this.due = due
    this.timer = unrefTimer(setTimeout(this.fire, Math.max(0, due - performance.now())))
  }

  /** Marks the acknowledgement as carried in this side's next message; false if it already was. */
  carry(): boolean {
    if (this.carrying) return false
    this.carrying = true
    return true
  }

  /** The acknowledgement went: nothing waits. */
  clear(): void {
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
    this.alone = this.carrying = false
  }

  private readonly fire = (): void => {
    this.timer = null
    if (this.alone) this.owner.acknowledge()
    else this.owner.acknowledgeWithNext()
  }
}
