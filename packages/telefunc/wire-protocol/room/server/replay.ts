export { ReplayGate, LocalHolder, ANNOUNCE_KEY }
export type { LaneHolder, TextWantsChange }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import { makePublishInfo } from '../../channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { type BinaryFrame, type BinaryWantChange } from '../binary.js'
import { applyBinaryWantChange, binaryWantsCovers, emptyBinaryWants, type BinaryWants } from './binary-wants.js'
import type { MemberWants } from '../protocol.js'
import type { RoomDataEnvelope } from './protocol.js'
import type { RoomState } from '../state.js'
import { toggle } from './sets.js'
assertIsNotBrowser()

// Text gate keys: a member's id, and for announcements a key no id takes.
const ANNOUNCE_KEY = '\0announce'

/** One holder's order across retained replay and live frames, per sender: a frame is admitted only if it is newer than
 *  every frame the holder already has from that sender on its lane. A replayed retained frame can win the race against
 *  live frames committed before it, so this drops its live echo and the sender's older frames, and a retained frame
 *  older than the sender's live stream. Another sender's frames are never dropped by it. */
class ReplayGate {
  private readonly _high = new Map<string, WirePublishInfo>()
  /** Binary lanes by sender, then track, so a sender's leave forgets only its own. */
  private readonly _binaryHigh = new Map<string, Map<string, WirePublishInfo>>()

  admit(key: string, info: WirePublishInfo): boolean {
    return admitInto(this._high, key, info)
  }

  admitBinary(member: string, track: string, info: WirePublishInfo): boolean {
    const high = getOrCreate(this._binaryHigh, member, () => new Map())
    return admitInto(high, track, info)
  }

  forgetMember(member: string): void {
    this._high.delete(member)
    this._binaryHigh.delete(member)
  }
}

/** A later time is newer whatever the seq: a commit never sets its lane's time back, and a failover may rewind its seq. */
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
  /** The members whose own frames the holder doesn't get back. */
  _suppressedMembers(): ReadonlySet<string>
  // A retained frame decoded, then in its wire form, which only a holder that forwards it takes.
  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo, serialized: string): void
  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo, framed: Uint8Array): void
}

/** This instance's own listeners as one holder: gated like a client's stub, with wants that change only when they differ, as a client declares them. */
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
    if (this._state.wantsAnnounce && this._replay.admit(ANNOUNCE_KEY, info))
      this._state.applyAnnounce(data, this._publishInfo(info))
  }

  relayBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._wantsBinary(frame.from, frame.track) && this._replay.admitBinary(frame.from, frame.track, info))
      this._applyBinary(frame, info)
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

  private _publishInfo(info: WirePublishInfo) {
    return makePublishInfo(this._state.roomId, info.seq, info.timestamp)
  }
}
