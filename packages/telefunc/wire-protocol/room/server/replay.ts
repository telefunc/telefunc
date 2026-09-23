export { ReplayGate, LocalHolder, TEXT_LANE_KEY, binaryLaneKey }
export type { LaneHolder }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { makePublishInfo } from '../../channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { DEFAULT_TRACK, binaryWantsCovers, emptyTrackWants, type BinaryFrame, type BinaryWants } from '../binary.js'
import type { MemberWants, RoomDataEnvelope } from '../protocol.js'
import type { RoomState } from '../state.js'
assertIsNotBrowser()

const TEXT_LANE_KEY = 'text'
function binaryLaneKey(member: string, track: string): string {
  return `${member}\0${track}`
}

/** One holder's retained/live dedup per lane: a retained frame arrives once, never behind a same-or-newer frame, and its live echo is dropped. */
class ReplayGate {
  private readonly _high = new Map<string, number>()
  private readonly _pendingRetained = new Map<string, number>()

  admitLive(lane: string, seq: number): boolean {
    if (this._pendingRetained.get(lane) === seq) {
      this._pendingRetained.delete(lane)
      return false
    }
    if ((this._high.get(lane) ?? 0) < seq) this._high.set(lane, seq)
    return true
  }

  admitRetained(lane: string, seq: number): boolean {
    if ((this._high.get(lane) ?? 0) >= seq) return false
    this._high.set(lane, seq)
    this._pendingRetained.set(lane, seq)
    return true
  }

  forgetMember(member: string): void {
    const prefix = binaryLaneKey(member, '')
    for (const lanes of [this._high, this._pendingRetained])
      for (const key of lanes.keys()) if (key.startsWith(prefix)) lanes.delete(key)
  }
}

/** A consumer of a room's lanes that retained frames replay into: a client's stub, or this instance's own listeners. */
interface LaneHolder {
  readonly _binaryWants: BinaryWants
  _wantsTextFrom(member: string): boolean
  _wantsBinary(member: string, track: string): boolean
  _emitRetainedText(serialized: string, event: RoomDataEnvelope, info: WirePublishInfo): void
  _emitRetainedBinary(framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void
}

/** This instance's own listeners as one holder: gated like a client's stub, with wants that change only when they differ, as a client declares them. */
class LocalHolder implements LaneHolder {
  private readonly _replay = new ReplayGate()
  private _textWants: MemberWants = { all: false, members: [] }
  _binaryWants: BinaryWants = { everyMember: emptyTrackWants(), members: {} }

  constructor(
    private readonly _state: RoomState,
    private readonly _suppress: (member: string) => boolean,
  ) {}

  /** Re-derive the listeners' wants; each lane kind whose wants changed reports its previous ones. */
  refreshWants(): { prevText: MemberWants | null; prevBinary: BinaryWants | null } {
    const prevText = this._textWants
    const prevBinary = this._binaryWants
    this._textWants = this._state.textWants()
    this._binaryWants = this._state.binaryWants()
    return {
      prevText: JSON.stringify(prevText) === JSON.stringify(this._textWants) ? null : prevText,
      prevBinary: JSON.stringify(prevBinary) === JSON.stringify(this._binaryWants) ? null : prevBinary,
    }
  }

  _wantsTextFrom(member: string): boolean {
    return !this._suppress(member) && (this._textWants.all || this._textWants.members.includes(member))
  }

  _wantsBinary(member: string, track: string): boolean {
    return !this._suppress(member) && binaryWantsCovers(this._binaryWants, member, track)
  }

  relayText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._wantsTextFrom(event.from) && this._replay.admitLive(TEXT_LANE_KEY, info.seq)) this._applyText(event, info)
  }

  relayAnnouncement(data: unknown, info: WirePublishInfo): void {
    if (this._state.wantsAnnounce && this._replay.admitLive(TEXT_LANE_KEY, info.seq))
      this._state.applyAnnounce(data, this._publishInfo(info))
  }

  relayBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    const track = frame.track ?? DEFAULT_TRACK
    if (this._wantsBinary(frame.from, track) && this._replay.admitLive(binaryLaneKey(frame.from, track), info.seq))
      this._applyBinary(frame, info)
  }

  _emitRetainedText(_serialized: string, event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admitRetained(TEXT_LANE_KEY, info.seq)) this._applyText(event, info)
  }

  _emitRetainedBinary(_framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admitRetained(binaryLaneKey(frame.from, frame.track ?? DEFAULT_TRACK), info.seq))
      this._applyBinary(frame, info)
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
