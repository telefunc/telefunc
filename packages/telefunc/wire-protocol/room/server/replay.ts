export { ReplayGate, LocalHolder, ANNOUNCE_KEY, binaryLaneKey }
export type { LaneHolder, WantsChange }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { makePublishInfo } from '../../channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import {
  binaryWantsCovers,
  emptyTrackWants,
  laneTrack,
  sameTrackWants,
  type BinaryFrame,
  type BinaryWants,
  type TrackWants,
} from '../binary.js'
import type { MemberWants, RoomDataEnvelope } from '../protocol.js'
import type { RoomState } from '../state.js'
assertIsNotBrowser()

// Text gate keys: a member's id, and for announcements a key no id takes. A binary lane's key is its id NUL track.
const ANNOUNCE_KEY = '\0announce'
function binaryLaneKey(member: string, track: string): string {
  return `${member}\0${track}`
}

/** One holder's order across retained replay and live frames, per sender: a frame is admitted only if it is newer than
 *  every frame the holder already has from that sender on its lane. A replayed retained frame can win the race against
 *  live frames committed before it, so this drops its live echo and the sender's older frames, and a retained frame
 *  older than the sender's live stream. Another sender's frames are never dropped by it. */
class ReplayGate {
  private readonly _high = new Map<string, number>()
  /** Binary lanes by sender, then track, so a sender's leave forgets only its own. */
  private readonly _binaryHigh = new Map<string, Map<string, number>>()

  admit(key: string, seq: number): boolean {
    return admitInto(this._high, key, seq)
  }

  admitBinary(member: string, track: string, seq: number): boolean {
    let high = this._binaryHigh.get(member)
    if (!high) this._binaryHigh.set(member, (high = new Map()))
    return admitInto(high, track, seq)
  }

  forgetMember(member: string): void {
    this._high.delete(member)
    this._binaryHigh.delete(member)
  }
}

function admitInto(high: Map<string, number>, key: string, seq: number): boolean {
  if ((high.get(key) ?? 0) >= seq) return false
  high.set(key, seq)
  return true
}

/** A holder's wants before a change, holding only what changed: its room-wide want of each lane kind that changed, and
 *  the own wants of each member whose own wants changed (`undefined`: none). */
type WantsChange = {
  text?: { all: boolean; members: Map<string, boolean> }
  binary?: { everyMember: TrackWants; members: Map<string, TrackWants | undefined> }
}

/** A consumer of a room's lanes that retained frames replay into: a client's stub, or this instance's own listeners. */
interface LaneHolder {
  readonly _binaryWants: BinaryWants
  readonly _wantsAnnounce: boolean
  /** The text the holder needs ingested; its relay filters further. */
  _textDemand(): MemberWants
  _wantsTextFrom(member: string): boolean
  _wantsBinary(member: string, track: string): boolean
  /** The members whose own frames the holder doesn't get back. */
  _suppressedMembers(): Iterable<string>
  // A retained frame decoded, then in its wire form, which only a holder that forwards it takes.
  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo, serialized: string): void
  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo, framed: Uint8Array): void
}

/** This instance's own listeners as one holder: gated like a client's stub, with wants that change only when they differ, as a client declares them. */
class LocalHolder implements LaneHolder {
  private readonly _replay = new ReplayGate()
  private _textAll = false
  private readonly _textMembers = new Set<string>()
  readonly _binaryWants: BinaryWants = { everyMember: emptyTrackWants(), members: Object.create(null) }

  constructor(
    private readonly _state: RoomState,
    private readonly _suppressed: ReadonlySet<string>,
  ) {}

  /** Re-derives the listeners' room-wide wants, and the own wants of `member` when its listeners changed. */
  refreshWants(member: string | null): WantsChange {
    const text = this._state.textWants()
    const binary = this._state.binaryWants()
    const textBefore = new Map<string, boolean>()
    const binaryBefore = new Map<string, TrackWants | undefined>()
    if (member !== null) {
      const wantsText = text.members.has(member)
      if (wantsText !== this._textMembers.has(member)) {
        textBefore.set(member, !wantsText)
        if (wantsText) this._textMembers.add(member)
        else this._textMembers.delete(member)
      }
      const prev = this._binaryWants.members[member]
      const next = binary.members[member]
      if (!sameTrackWants(prev, next)) {
        binaryBefore.set(member, prev)
        if (next) this._binaryWants.members[member] = next
        else delete this._binaryWants.members[member]
      }
    }
    const change: WantsChange = {}
    if (text.all !== this._textAll || textBefore.size > 0) change.text = { all: this._textAll, members: textBefore }
    this._textAll = text.all
    const everyMember = this._binaryWants.everyMember
    if (!sameTrackWants(everyMember, binary.everyMember) || binaryBefore.size > 0)
      change.binary = { everyMember, members: binaryBefore }
    this._binaryWants.everyMember = binary.everyMember
    return change
  }

  get _wantsAnnounce(): boolean {
    return this._state.wantsAnnounce
  }

  _textDemand(): MemberWants {
    return { all: this._textAll, members: this._textMembers }
  }

  _suppressedMembers(): Iterable<string> {
    return this._suppressed
  }

  _wantsTextFrom(member: string): boolean {
    return !this._suppressed.has(member) && (this._textAll || this._textMembers.has(member))
  }

  _wantsBinary(member: string, track: string): boolean {
    return !this._suppressed.has(member) && binaryWantsCovers(this._binaryWants, member, track)
  }

  relayText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._wantsTextFrom(event.from) && this._replay.admit(event.from, info.seq)) this._applyText(event, info)
  }

  relayAnnouncement(data: unknown, info: WirePublishInfo): void {
    if (this._state.wantsAnnounce && this._replay.admit(ANNOUNCE_KEY, info.seq))
      this._state.applyAnnounce(data, this._publishInfo(info))
  }

  relayBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    const track = laneTrack(frame.track)
    if (this._wantsBinary(frame.from, track) && this._replay.admitBinary(frame.from, track, info.seq))
      this._applyBinary(frame, info)
  }

  _emitRetainedText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admit(event.from, info.seq)) this._applyText(event, info)
  }

  _emitRetainedBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admitBinary(frame.from, laneTrack(frame.track), info.seq)) this._applyBinary(frame, info)
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
