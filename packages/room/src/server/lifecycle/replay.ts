export { ReplayGate, ReplayHold, LocalHolder, heldWeight, heldTextWeight }
export type { LaneHolder, TextWantsChange }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { getOrCreate } from 'telefunc/__internal'
import type { ChannelPublishInfo, OrderingInfo } from 'telefunc/__internal'
import { EVERY_MEMBER, EVERY_TRACK, type BinaryFrame, type BinaryWantChange } from '../../shared/binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from '../lanes/binaryWants.js'
import type { MemberWants } from '../../shared/protocol.js'
import type { RoomDataEnvelope } from '../protocol.js'
import type { RoomState } from '../../shared/RoomState.js'
import { ROOM_REPLAY_HOLD_BYTES_MAX, ROOM_REPLAY_HOLD_ENTRY_BYTES } from '../constants.js'
assertIsNotBrowser()

type HeldFrame = { from: string; bytes: number; relay: () => void }

/** What holding a frame retains: the whole storage behind `bytes`, the `decoded` bytes parsed out of it, and the
 *  entry. */
function heldWeight(bytes: Uint8Array, decoded = 0): number {
  return bytes.buffer.byteLength + decoded + ROOM_REPLAY_HOLD_ENTRY_BYTES
}

/** What holding a message retains: its text, or what was parsed from it, two bytes a code unit, and the entry. */
function heldTextWeight(serialized: string): number {
  return serialized.length * 2 + ROOM_REPLAY_HOLD_ENTRY_BYTES
}

type ReplayHoldOptions = {
  /** The binary want whose retained frames are read, null for a text want. */
  change: BinaryWantChange | null
  gate: ReplayGate
}

/** A want whose retained frames, or retained message (`change` null: a text want), are being read: it holds the live
 *  frames or messages the want covers on lanes its holder has had none from, until `end()` once the read settled, or
 *  `flush()` earlier. */
class ReplayHold {
  readonly change: BinaryWantChange | null
  private readonly gate: ReplayGate
  held: HeldFrame[] = []
  bytes = 0
  holding = true

  constructor({ change, gate }: ReplayHoldOptions) {
    this.change = change
    this.gate = gate
  }

  /** Whether it holds `member`'s frames on `track`, or with `track` null its messages. */
  covers(member: string, track: string | null): boolean {
    const { change } = this
    if (change === null || track === null) return change === null && track === null
    return (
      (change.member === EVERY_MEMBER || change.member === member) &&
      (change.track === EVERY_TRACK || change.track === track)
    )
  }

  /** Lets the held frames go before the read settled: a retained frame it reads later goes out only on a lane the holder
   *  got no live frame on meanwhile. */
  flush(): void {
    this.gate.flush(this)
  }

  /** The read settled: the held frames follow the retained ones it emitted. */
  end(): void {
    this.gate.flush(this)
    this.gate.end(this)
  }
}

/** One holder's order across retained replay and live frames, per sender: a frame is admitted only if its seq is above
 *  every frame's the holder already has from that sender on its lane. A replayed retained frame can win the race
 *  against live frames committed before it, so this drops its live echo and the sender's older frames. One read after
 *  its replay let its held frames go early goes out only where the holder got nothing live on its lane meanwhile: after
 *  newer frames, it would set the lane's state back. Another sender's frames are never dropped by it, except where a
 *  failover rewound their lane (`rewound()`). */
class ReplayGate {
  /** The text lane's seq of each sender's last admitted message. */
  private readonly high = new Map<string, number>()
  /** Binary lanes by sender, then track, so a sender's leave forgets only its own. */
  private readonly binaryHigh = new Map<string, Map<string, number>>()
  /** The binary replays whose reads haven't settled. */
  private readonly replays = new Set<ReplayHold>()
  private _holding = 0
  private heldBytes = 0

  admit(key: string, info: OrderingInfo): boolean {
    return admitInto(this.high, key, info)
  }

  admitBinary(member: string, track: string, info: OrderingInfo): boolean {
    return admitInto(
      getOrCreate(this.binaryHigh, member, () => new Map()),
      track,
      info,
    )
  }

  /** A retained frame `hold` read, or with `track` null its retained message: once the hold let its frames go early, it
   *  goes out only where the holder has had nothing on its lane. */
  admitRetained(hold: ReplayHold, member: string, track: string | null, info: OrderingInfo): boolean {
    if (track === null) return (hold.holding || !this.high.has(member)) && this.admit(member, info)
    return (hold.holding || !this.binaryHigh.get(member)?.has(track)) && this.admitBinary(member, track, info)
  }

  /** A failover replaced what the lane delivered from `seq` on, and the frame at `seq` follows: the holds that would
   *  hold the lane's frames let theirs go first, as they came before it, and the lane's marks drop below `seq`, so the
   *  new history's frames are admitted. `member` null is the text lane, which every sender shares. A retained frame read
   *  later goes out only on a lane that had nothing yet, as after any early flush. */
  rewound(member: string | null, track: string, seq: number): void {
    for (const hold of this.replays) {
      if (member === null ? hold.change !== null : !hold.covers(member, track)) continue
      this.flush(hold)
    }
    if (member === null) {
      for (const [sender, last] of this.high) if (last >= seq) this.high.set(sender, seq - 1)
      return
    }
    const high = this.binaryHigh.get(member)
    if (high !== undefined && (high.get(track) ?? 0) >= seq) high.set(track, seq - 1)
  }

  /** A sender no text want covers any more starts afresh: a later subscription gets its retained message again. */
  forgetUnwantedText(wanted: (member: string) => boolean): void {
    for (const key of this.high.keys()) if (!wanted(key)) this.high.delete(key)
  }

  /** A lane no want covers any more starts afresh: a later subscription to it gets its retained frame again. */
  forgetUnwantedBinary(wanted: (member: string, track: string) => boolean): void {
    for (const [member, high] of this.binaryHigh) {
      for (const track of high.keys()) if (!wanted(member, track)) high.delete(track)
      if (high.size === 0) this.binaryHigh.delete(member)
    }
  }

  /** The frames held from `member` go with it: its leave reached the holder before them. */
  forgetMember(member: string): void {
    this.high.delete(member)
    this.binaryHigh.delete(member)
    for (const hold of this.replays) {
      for (const frame of hold.held) if (frame.from === member) this.unhold(hold, frame.bytes)
      hold.held = hold.held.filter((frame) => frame.from !== member)
    }
  }

  /** Holds the live frames `change` covers on lanes the holder has had none from, or with `change` null the live
   *  messages of senders it has had none from: what is retained, read meanwhile, goes out first. */
  hold(change: BinaryWantChange | null): ReplayHold {
    const hold = new ReplayHold({ change, gate: this })
    this.replays.add(hold)
    this._holding++
    return hold
  }

  /** Whether some replay may hold a live frame. */
  get holding(): boolean {
    return this._holding > 0
  }

  /** Whether a replay holds `member`'s live frame on `track`, or with `track` null its live message, which retains
   *  `bytes` (see `heldWeight()`); `relay` delivers it once that replay lets its frames go. */
  held(member: string, track: string | null, bytes: number, relay: () => void): boolean {
    if (track === null ? this.high.has(member) : this.binaryHigh.get(member)?.has(track)) return false
    for (const hold of this.replays) {
      if (!hold.holding || !hold.covers(member, track)) continue
      hold.held.push({ from: member, bytes, relay })
      hold.bytes += bytes
      this.heldBytes += bytes
      if (this.heldBytes > ROOM_REPLAY_HOLD_BYTES_MAX) for (const replay of this.replays) replay.flush()
      return true
    }
    return false
  }

  /** The holder is gone: what its replays hold is dropped. */
  dropHolds(): void {
    for (const hold of this.replays) {
      hold.holding = false
      hold.held = []
    }
    this.replays.clear()
    this._holding = 0
    this.heldBytes = 0
  }

  flush(hold: ReplayHold): void {
    if (!hold.holding) return
    hold.holding = false
    this._holding--
    this.unhold(hold, hold.bytes)
    const held = hold.held
    hold.held = []
    for (const frame of held) frame.relay()
  }

  end(hold: ReplayHold): void {
    this.replays.delete(hold)
  }

  private unhold(hold: ReplayHold, bytes: number): void {
    hold.bytes -= bytes
    this.heldBytes -= bytes
  }
}

function admitInto(high: Map<string, number>, key: string, { seq }: OrderingInfo): boolean {
  const last = high.get(key)
  if (last !== undefined && seq <= last) return false
  high.set(key, seq)
  return true
}

/** A holder's text wants before a change, holding only what changed: its room-wide want, and the own want of each
 *  member whose own want changed. */
type TextWantsChange = { all: boolean; members: Map<string, boolean> }

/** A consumer of a room's lanes that retained frames replay into: a client stub, or this instance's own listeners. */
interface LaneHolder {
  readonly binaryWants: BinaryWants
  /** The participants it holds, who are the people at its page. */
  heldMembers(): Iterable<string>
  /** Whether `member` is one of `heldMembers()`. */
  holds(member: string): boolean
  /** The text the holder needs ingested; its relay filters further. */
  textDemand(): MemberWants
  wantsTextFrom(member: string): boolean
  wantsBinary(member: string, track: string): boolean
  /** Holds the live frames `change` newly covers, or with `change` null live messages, while its replay reads what is
   *  retained. */
  hold(change: BinaryWantChange | null): ReplayHold
  /** The members whose own frames the holder doesn't get back. */
  suppressedMembers(): ReadonlySet<string>
  /** A retained frame decoded, then in its wire form, which only a holder that forwards it takes. */
  emitRetainedText(event: RoomDataEnvelope, info: OrderingInfo, serialized: string, hold: ReplayHold): void
  emitRetainedBinary(frame: BinaryFrame, info: OrderingInfo, framed: Uint8Array, hold: ReplayHold): void
}

type LocalHolderOptions = {
  state: RoomState
  /** The participants that joined through this handle. */
  participants: ReadonlyMap<string, { readonly _heldByPage: boolean; readonly selfDelivery: boolean }>
}

/** This instance's own listeners as one holder: gated like a client's stub, with wants that change only when they
 *  differ, as a client declares them. */
class LocalHolder implements LaneHolder {
  private readonly state: RoomState
  /** The members whose own messages its page doesn't get back (`PageIndex`). */
  private suppressed: ReadonlySet<string> = new Set()
  private readonly participants: LocalHolderOptions['participants']
  private readonly replay = new ReplayGate()
  readonly binaryWants: BinaryWants = emptyBinaryWants()

  constructor({ state, participants }: LocalHolderOptions) {
    this.state = state
    this.participants = participants
  }

  /** The server-side room is its own page: the participants that joined through it and no page holds are at it. */
  *heldMembers(): Generator<string> {
    for (const [id, participant] of this.participants) if (!participant._heldByPage) yield id
  }

  holds(member: string): boolean {
    return this.participants.get(member)?._heldByPage === false
  }

  /** Every participant that joined through it with `selfDelivery: false`, a page holding it or not: its listeners never
   *  get that participant's messages. */
  *selfSuppressed(): Generator<string> {
    for (const [id, participant] of this.participants) if (!participant.selfDelivery) yield id
  }

  suppresses(member: string): boolean {
    return this.participants.get(member)?.selfDelivery === false
  }

  atPage(suppressed: ReadonlySet<string>): void {
    this.suppressed = suppressed
  }

  /** Its listeners' room-wide text want (`member` null) or `member`'s own turned `on` or off; returns the wants before. */
  textChanged(member: string | null, on: boolean): TextWantsChange {
    this.replay.forgetUnwantedText((member) => this.wantsTextFrom(member))
    if (member === null) return { all: !on, members: new Map() }
    return { all: this.state.textWants().all, members: new Map([[member, !on]]) }
  }

  applyBinary(change: BinaryWantChange): boolean {
    if (!applyBinaryWantChange(this.binaryWants, change)) return false
    if (!change.on) this.replay.forgetUnwantedBinary((member, track) => this.wantsBinary(member, track))
    return true
  }

  textDemand(): MemberWants {
    return this.state.textWants()
  }

  suppressedMembers(): ReadonlySet<string> {
    return this.suppressed
  }

  wantsTextFrom(member: string): boolean {
    return !this.suppressed.has(member) && this.state.wantsTextFrom(member)
  }

  wantsBinary(member: string, track: string): boolean {
    return !this.suppressed.has(member) && binaryWantsCovers(this.binaryWants, member, track)
  }

  relayText(event: RoomDataEnvelope, info: OrderingInfo, serialized: string): void {
    if (info.rewound) this.replay.rewound(null, '', info.seq)
    if (!this.wantsTextFrom(event.from)) return
    const replay = this.replay
    if (replay.holding) {
      const relay = () => this.relayText(event, info, serialized)
      if (replay.held(event.from, null, heldTextWeight(serialized), relay)) return
    }
    if (replay.admit(event.from, info)) this.applyTextMessage(event, info)
  }

  relayBinary(frame: BinaryFrame, info: OrderingInfo): void {
    if (info.rewound) this.replay.rewound(frame.from, frame.track, info.seq)
    if (!this.wantsBinary(frame.from, frame.track)) return
    const { from, track } = frame
    const replay = this.replay
    if (replay.holding) {
      // The track and metadata decoded out of the frame were encoded in the bytes before its payload.
      const weight = heldWeight(frame.payload, frame.payload.byteOffset)
      if (replay.held(from, track, weight, () => this.relayBinary(frame, info))) return
    }
    if (replay.admitBinary(from, track, info)) this.applyBinaryFrame(frame, info)
  }

  hold(change: BinaryWantChange | null): ReplayHold {
    return this.replay.hold(change)
  }

  emitRetainedText(event: RoomDataEnvelope, info: OrderingInfo, _serialized: string, hold: ReplayHold): void {
    if (this.replay.admitRetained(hold, event.from, null, info)) this.applyTextMessage(event, info)
  }

  emitRetainedBinary(frame: BinaryFrame, info: OrderingInfo, _framed: Uint8Array, hold: ReplayHold): void {
    if (this.replay.admitRetained(hold, frame.from, frame.track, info)) this.applyBinaryFrame(frame, info)
  }

  forgetMember(member: string): void {
    this.replay.forgetMember(member)
  }

  private applyTextMessage(event: RoomDataEnvelope, info: OrderingInfo): void {
    this.state.applyData(event, this.publishInfo(info))
  }

  private applyBinaryFrame(frame: BinaryFrame, info: OrderingInfo): void {
    this.state.applyBinary(frame, this.publishInfo(info))
  }

  private publishInfo(info: OrderingInfo): ChannelPublishInfo {
    return { key: this.state.model.roomId, seq: info.seq, timestamp: info.timestamp }
  }
}
