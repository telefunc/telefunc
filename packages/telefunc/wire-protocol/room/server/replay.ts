export { ReplayGate, ReplayHold, LocalHolder, ANNOUNCE_KEY, heldWeight }
export type { LaneHolder, TextWantsChange }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import { makePublishInfo, type ChannelPublishInfo } from '../../channel/channel.js'
import type { WirePublishInfo } from '../../wire/connectionFrame.js'
import { ALL, type BinaryFrame, type BinaryWantChange } from '../binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from './binaryWants.js'
import type { MemberWants } from '../protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import type { RoomState } from '../state.js'
import { toggle } from './sets.js'
import { ROOM_REPLAY_HOLD_BYTES_MAX, ROOM_REPLAY_HOLD_ENTRY_BYTES } from './constants.js'
assertIsNotBrowser()

// Text gate keys: a member's id, and for announcements a key no id takes.
const ANNOUNCE_KEY = '\0announce'

type HeldFrame = { from: string; bytes: number; relay: () => void }

/** What holding a frame retains: the whole storage behind `bytes`, the `decoded` bytes parsed out of it, and the
 *  entry. */
function heldWeight(bytes: Uint8Array, decoded = 0): number {
  return bytes.buffer.byteLength + decoded + ROOM_REPLAY_HOLD_ENTRY_BYTES
}

/** A binary want whose retained frames are being read: it holds the live frames the want covers on lanes its holder has
 *  had none from, until `end()` once the read settled, or `flush()` earlier. */
class ReplayHold {
  held: HeldFrame[] = []
  bytes = 0
  holding = true
  /** Once flushed early: each lane's first live frame the holder got, which a retained frame read later is older than
   *  only if the holder never got it. */
  readonly firstLive = new Map<string, Map<string, WirePublishInfo>>()

  constructor(
    readonly change: BinaryWantChange,
    private readonly _gate: ReplayGate,
  ) {}

  covers(member: string, track: string): boolean {
    const { change } = this
    return (change.member === ALL || change.member === member) && (change.track === ALL || change.track === track)
  }

  /** Lets the held frames go before the read settled: the retained frames it reads later follow them. */
  flush(): void {
    this._gate._flush(this)
  }

  /** The read settled: the held frames follow the retained ones it emitted. */
  end(): void {
    this._gate._flush(this)
    this._gate._end(this)
  }
}

/** One holder's order across retained replay and live frames, per sender: a frame is admitted only if it is newer than
 *  every frame the holder already has from that sender on its lane. A replayed retained frame can win the race against
 *  live frames committed before it, so this drops its live echo and the sender's older frames, and a retained frame
 *  older than the sender's live stream, unless its replay let its held frames go early. Another sender's frames are
 *  never dropped by it. */
class ReplayGate {
  private readonly _high = new Map<string, WirePublishInfo>()
  /** Binary lanes by sender, then track, so a sender's leave forgets only its own. */
  private readonly _binaryHigh = new Map<string, Map<string, WirePublishInfo>>()
  /** The binary replays whose reads haven't settled. */
  private readonly _replays = new Set<ReplayHold>()
  private _holding = 0
  private _heldBytes = 0

  admit(key: string, info: WirePublishInfo): boolean {
    return admitInto(this._high, key, info)
  }

  admitBinary(member: string, track: string, info: WirePublishInfo): boolean {
    const high = getOrCreate(this._binaryHigh, member, () => new Map())
    const first = !high.has(track)
    if (!admitInto(high, track, info)) return false
    if (first) {
      for (const hold of this._replays) {
        if (hold.holding || !hold.covers(member, track)) continue
        const lanes = getOrCreate(hold.firstLive, member, () => new Map())
        if (!lanes.has(track)) lanes.set(track, info)
      }
    }
    return true
  }

  /** A retained frame `hold` read: once the hold let its frames go early, it follows them unless the holder got it
   *  live already. */
  admitRetained(hold: ReplayHold, member: string, track: string, info: WirePublishInfo): boolean {
    if (hold.holding || !this._binaryHigh.get(member)?.has(track)) return this.admitBinary(member, track, info)
    const firstLive = hold.firstLive.get(member)?.get(track)
    return firstLive !== undefined && isNewer(firstLive, info)
  }

  /** The frames held from `member` go with it: its leave reached the holder before them. */
  forgetMember(member: string): void {
    this._high.delete(member)
    this._binaryHigh.delete(member)
    for (const hold of this._replays) {
      for (const frame of hold.held) if (frame.from === member) this._unhold(hold, frame.bytes)
      hold.held = hold.held.filter((frame) => frame.from !== member)
    }
  }

  /** Holds the live frames `change` covers on lanes the holder has had none from: the retained frames read meanwhile
   *  go out first. */
  holdBinary(change: BinaryWantChange): ReplayHold {
    const hold = new ReplayHold(change, this)
    this._replays.add(hold)
    this._holding++
    return hold
  }

  /** Whether some replay may hold a live frame. */
  get holding(): boolean {
    return this._holding > 0
  }

  /** Whether a replay holds the live frame, which retains `bytes` (see `heldWeight()`); `relay` delivers it once that
   *  replay lets its frames go. */
  heldBinary(member: string, track: string, bytes: number, relay: () => void): boolean {
    if (this._binaryHigh.get(member)?.has(track)) return false
    for (const hold of this._replays) {
      if (!hold.holding || !hold.covers(member, track)) continue
      hold.held.push({ from: member, bytes, relay })
      hold.bytes += bytes
      this._heldBytes += bytes
      if (this._heldBytes > ROOM_REPLAY_HOLD_BYTES_MAX) for (const replay of this._replays) replay.flush()
      return true
    }
    return false
  }

  /** The holder is gone: what its replays hold is dropped. */
  dropHolds(): void {
    for (const hold of this._replays) {
      hold.holding = false
      hold.held = []
    }
    this._replays.clear()
    this._holding = 0
    this._heldBytes = 0
  }

  /** @internal */
  _flush(hold: ReplayHold): void {
    if (!hold.holding) return
    hold.holding = false
    this._holding--
    this._unhold(hold, hold.bytes)
    const held = hold.held
    hold.held = []
    for (const frame of held) frame.relay()
  }

  /** @internal */
  _end(hold: ReplayHold): void {
    this._replays.delete(hold)
  }

  private _unhold(hold: ReplayHold, bytes: number): void {
    hold.bytes -= bytes
    this._heldBytes -= bytes
  }
}

/** A later time is newer whatever the seq: a commit never sets its lane's time back,
 *  and a failover may rewind its seq. */
function isNewer(info: WirePublishInfo, than: WirePublishInfo): boolean {
  return info.timestamp > than.timestamp || (info.timestamp === than.timestamp && info.seq > than.seq)
}

function admitInto(high: Map<string, WirePublishInfo>, key: string, info: WirePublishInfo): boolean {
  const last = high.get(key)
  if (last !== undefined && !isNewer(info, last)) return false
  high.set(key, info)
  return true
}

/** A holder's text wants before a change, holding only what changed: its room-wide want, and the own want of each
 *  member whose own want changed. */
type TextWantsChange = { all: boolean; members: Map<string, boolean> }

/** A consumer of a room's lanes that retained frames replay into: a client's stub, or this instance's own listeners. */
interface LaneHolder {
  readonly _binaryWants: BinaryWants
  readonly _wantsAnnounce: boolean
  /** The text the holder needs ingested; its relay filters further. */
  _textDemand(): MemberWants
  _wantsTextFrom(member: string): boolean
  _wantsBinary(member: string, track: string): boolean
  /** Holds the live frames `change` newly covers while its replay reads the retained ones. */
  _holdBinary(change: BinaryWantChange): ReplayHold
  /** The members whose own frames the holder doesn't get back. */
  _suppressedMembers(): ReadonlySet<string>
  /** A retained frame decoded, then in its wire form, which only a holder that forwards it takes. */
  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo, serialized: string): void
  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo, framed: Uint8Array, hold: ReplayHold): void
}

/** This instance's own listeners as one holder: gated like a client's stub, with wants that change only when they
 *  differ, as a client declares them. */
class LocalHolder implements LaneHolder {
  private readonly _replay = new ReplayGate()
  private _textAll = false
  private readonly _textMembers = new Set<string>()
  readonly _binaryWants: BinaryWants = emptyBinaryWants()

  constructor(
    private readonly _state: RoomState,
    private readonly _suppressed: ReadonlySet<string>,
  ) {}

  /** Re-derives the listeners' room-wide text want, and the own want of `member` when its listeners changed; `null`
   *  when neither changed. */
  refreshText(member: string | null): TextWantsChange | null {
    const text = this._state.textWants()
    const before = new Map<string, boolean>()
    if (member !== null) {
      const wantsText = text.members.has(member)
      if (wantsText !== this._textMembers.has(member)) {
        before.set(member, !wantsText)
        toggle(this._textMembers, member, wantsText)
      }
    }
    const change = text.all !== this._textAll || before.size > 0 ? { all: this._textAll, members: before } : null
    this._textAll = text.all
    return change
  }

  applyBinary(change: BinaryWantChange): boolean {
    return applyBinaryWantChange(this._binaryWants, change)
  }

  get _wantsAnnounce(): boolean {
    return this._state.wantsAnnounce
  }

  _textDemand(): MemberWants {
    return { all: this._textAll, members: this._textMembers }
  }

  _suppressedMembers(): ReadonlySet<string> {
    return this._suppressed
  }

  _wantsTextFrom(member: string): boolean {
    return !this._suppressed.has(member) && (this._textAll || this._textMembers.has(member))
  }

  _wantsBinary(member: string, track: string): boolean {
    return !this._suppressed.has(member) && binaryWantsCovers(this._binaryWants, member, track)
  }

  relayText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._wantsTextFrom(event.from) && this._replay.admit(event.from, info)) this._applyText(event, info)
  }

  relayAnnouncement(data: unknown, info: WirePublishInfo): void {
    if (this._state.wantsAnnounce && this._replay.admit(ANNOUNCE_KEY, info)) {
      this._state.applyAnnounce(data, this._publishInfo(info))
    }
  }

  relayBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    if (!this._wantsBinary(frame.from, frame.track)) return
    const { from, track } = frame
    const replay = this._replay
    if (replay.holding) {
      // The track and metadata decoded out of the frame were encoded in the bytes before its payload.
      const weight = heldWeight(frame.payload, frame.payload.byteOffset)
      if (replay.heldBinary(from, track, weight, () => this.relayBinary(frame, info))) return
    }
    if (replay.admitBinary(from, track, info)) this._applyBinary(frame, info)
  }

  _holdBinary(change: BinaryWantChange): ReplayHold {
    return this._replay.holdBinary(change)
  }

  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admit(event.from, info)) this._applyText(event, info)
  }

  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo, _framed: Uint8Array, hold: ReplayHold): void {
    if (this._replay.admitRetained(hold, frame.from, frame.track, info)) this._applyBinary(frame, info)
  }

  forgetMember(member: string): void {
    this._replay.forgetMember(member)
  }

  private _applyText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    this._state.applyData(event, this._publishInfo(info))
  }

  private _applyBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    this._state.applyBinary(frame, this._publishInfo(info))
  }

  private _publishInfo(info: WirePublishInfo): ChannelPublishInfo {
    return makePublishInfo(this._state.roomId, info.seq, info.timestamp)
  }
}
