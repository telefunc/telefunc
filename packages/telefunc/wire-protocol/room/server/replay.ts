export { ReplayGate, LocalHolder, ANNOUNCE_KEY, heldWeight }
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

/** A binary want whose retained frames are being read, and the live frames it holds meanwhile. */
type ReplayHold = { change: BinaryWantChange; held: HeldFrame[]; bytes: number }
type HeldFrame = { from: string; bytes: number; relay: () => void }

/** What holding a frame retains: the whole storage behind `bytes`, the `decoded` bytes parsed out of it, and the
 *  entry. */
function heldWeight(bytes: Uint8Array, decoded = 0): number {
  return bytes.buffer.byteLength + decoded + ROOM_REPLAY_HOLD_ENTRY_BYTES
}

/** One holder's order across retained replay and live frames, per sender: a frame is admitted only if it is newer than
 *  every frame the holder already has from that sender on its lane. A replayed retained frame can win the race against
 *  live frames committed before it, so this drops its live echo and the sender's older frames, and a retained frame
 *  older than the sender's live stream. Another sender's frames are never dropped by it. */
class ReplayGate {
  private readonly _high = new Map<string, WirePublishInfo>()
  /** Binary lanes by sender, then track, so a sender's leave forgets only its own. */
  private readonly _binaryHigh = new Map<string, Map<string, WirePublishInfo>>()
  private readonly _holds = new Set<ReplayHold>()
  private _heldBytes = 0

  admit(key: string, info: WirePublishInfo): boolean {
    return admitInto(this._high, key, info)
  }

  admitBinary(member: string, track: string, info: WirePublishInfo): boolean {
    const high = getOrCreate(this._binaryHigh, member, () => new Map())
    return admitInto(high, track, info)
  }

  /** The frames held from `member` go with it: its leave reached the holder before them. */
  forgetMember(member: string): void {
    this._high.delete(member)
    this._binaryHigh.delete(member)
    for (const hold of this._holds) {
      for (const frame of hold.held) if (frame.from === member) this._unhold(hold, frame.bytes)
      hold.held = hold.held.filter((frame) => frame.from !== member)
    }
  }

  /** Holds the live frames `change` covers on lanes the holder has had none from, until the returned release runs
   *  their relays again: the retained frames read in between go out first. */
  holdBinary(change: BinaryWantChange): () => void {
    const hold: ReplayHold = { change, held: [], bytes: 0 }
    this._holds.add(hold)
    return () => this._release(hold)
  }

  /** Whether some replay may hold a live frame. */
  get holding(): boolean {
    return this._holds.size > 0
  }

  /** Whether a replay holds the live frame, which retains `bytes` (see `heldWeight()`); `relay` delivers it once that
   *  replay ends. */
  heldBinary(member: string, track: string, bytes: number, relay: () => void): boolean {
    if (this._binaryHigh.get(member)?.has(track)) return false
    for (const hold of this._holds) {
      const { change } = hold
      if ((change.member !== ALL && change.member !== member) || (change.track !== ALL && change.track !== track)) {
        continue
      }
      hold.held.push({ from: member, bytes, relay })
      hold.bytes += bytes
      this._heldBytes += bytes
      if (this._heldBytes > ROOM_REPLAY_HOLD_BYTES_MAX) for (const held of this._holds) this._release(held)
      return true
    }
    return false
  }

  /** The holder is gone: what its replays hold is dropped. */
  dropHolds(): void {
    this._holds.clear()
    this._heldBytes = 0
  }

  private _release(hold: ReplayHold): void {
    if (!this._holds.delete(hold)) return
    this._unhold(hold, hold.bytes)
    for (const frame of hold.held) frame.relay()
  }

  private _unhold(hold: ReplayHold, bytes: number): void {
    hold.bytes -= bytes
    this._heldBytes -= bytes
  }
}

/** A later time is newer whatever the seq: a commit never sets its lane's time back,
 *  and a failover may rewind its seq. */
function admitInto(high: Map<string, WirePublishInfo>, key: string, info: WirePublishInfo): boolean {
  const last = high.get(key)
  if (last !== undefined && info.timestamp < last.timestamp) return false
  if (last !== undefined && info.timestamp === last.timestamp && info.seq <= last.seq) return false
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
  /** Holds the live frames `change` newly covers until the returned release, which its replay calls once done. */
  _holdBinary(change: BinaryWantChange): () => void
  /** The members whose own frames the holder doesn't get back. */
  _suppressedMembers(): ReadonlySet<string>
  /** A retained frame decoded, then in its wire form, which only a holder that forwards it takes. */
  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo, serialized: string): void
  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo, framed: Uint8Array): void
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

  _holdBinary(change: BinaryWantChange): () => void {
    return this._replay.holdBinary(change)
  }

  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admit(event.from, info)) this._applyText(event, info)
  }

  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admitBinary(frame.from, frame.track, info)) this._applyBinary(frame, info)
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
