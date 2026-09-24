export { ReplayGate, LocalHolder, TEXT_LANE_KEY, binaryLaneKey }
export type { LaneHolder, WantsChange }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { makePublishInfo } from '../../channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import { binaryWantsCovers, emptyBinaryWants, laneTrack, type BinaryFrame, type BinaryWants } from '../binary.js'
import type { MemberWants, RoomDataEnvelope } from '../protocol.js'
import type { RoomState } from '../state.js'
assertIsNotBrowser()

const TEXT_LANE_KEY = 'text'
function binaryLaneKey(member: string, track: string): string {
  return `${member}\0${track}`
}

/** One holder's lane order across retained replay and live frames: a frame is admitted only if it is newer than every
 *  frame the holder already has on its lane. A replayed retained frame can win the race against live frames committed
 *  before it, so this drops its live echo and those older frames, and a retained frame older than the live stream. */
class ReplayGate {
  private readonly _high = new Map<string, number>()

  admit(lane: string, seq: number): boolean {
    if ((this._high.get(lane) ?? 0) >= seq) return false
    this._high.set(lane, seq)
    return true
  }

  forgetMember(member: string): void {
    const prefix = binaryLaneKey(member, '')
    for (const key of this._high.keys()) if (key.startsWith(prefix)) this._high.delete(key)
  }
}

/** The previous wants of each lane kind whose wants changed. */
type WantsChange = { text?: MemberWants; binary?: BinaryWants }

/** A consumer of a room's lanes that retained frames replay into: a client's stub, or this instance's own listeners. */
interface LaneHolder {
  readonly _binaryWants: BinaryWants
  readonly _wantsAnnounce: boolean
  /** The text the holder needs ingested; its relay filters further. */
  _textDemand(): 'all' | ReadonlySet<string>
  _wantsTextFrom(member: string): boolean
  _wantsBinary(member: string, track: string): boolean
  _emitRetainedText(serialized: string, event: RoomDataEnvelope, info: WirePublishInfo): void
  _emitRetainedBinary(framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void
}

/** This instance's own listeners as one holder: gated like a client's stub, with wants that change only when they differ, as a client declares them. */
class LocalHolder implements LaneHolder {
  private readonly _replay = new ReplayGate()
  private _textWants: MemberWants = { all: false, members: [] }
  _binaryWants: BinaryWants = emptyBinaryWants()

  constructor(
    private readonly _state: RoomState,
    private readonly _suppress: (member: string) => boolean,
  ) {}

  /** Re-derives the listeners' wants. */
  refreshWants(): WantsChange {
    const text = this._textWants
    const binary = this._binaryWants
    this._textWants = this._state.textWants()
    this._binaryWants = this._state.binaryWants()
    return {
      ...(JSON.stringify(text) === JSON.stringify(this._textWants) ? {} : { text }),
      ...(JSON.stringify(binary) === JSON.stringify(this._binaryWants) ? {} : { binary }),
    }
  }

  get _wantsAnnounce(): boolean {
    return this._state.wantsAnnounce
  }

  _textDemand(): 'all' | ReadonlySet<string> {
    return this._textWants.all ? 'all' : new Set(this._textWants.members)
  }

  _wantsTextFrom(member: string): boolean {
    return !this._suppress(member) && (this._textWants.all || this._textWants.members.includes(member))
  }

  _wantsBinary(member: string, track: string): boolean {
    return !this._suppress(member) && binaryWantsCovers(this._binaryWants, member, track)
  }

  relayText(event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._wantsTextFrom(event.from) && this._replay.admit(TEXT_LANE_KEY, info.seq)) this._applyText(event, info)
  }

  relayAnnouncement(data: unknown, info: WirePublishInfo): void {
    if (this._state.wantsAnnounce && this._replay.admit(TEXT_LANE_KEY, info.seq))
      this._state.applyAnnounce(data, this._publishInfo(info))
  }

  relayBinary(frame: BinaryFrame, info: WirePublishInfo): void {
    const track = laneTrack(frame.track)
    if (this._wantsBinary(frame.from, track) && this._replay.admit(binaryLaneKey(frame.from, track), info.seq))
      this._applyBinary(frame, info)
  }

  _emitRetainedText(_serialized: string, event: RoomDataEnvelope, info: WirePublishInfo): void {
    if (this._replay.admit(TEXT_LANE_KEY, info.seq)) this._applyText(event, info)
  }

  _emitRetainedBinary(_framed: Uint8Array, frame: BinaryFrame, info: WirePublishInfo): void {
    if (this._replay.admit(binaryLaneKey(frame.from, laneTrack(frame.track)), info.seq)) this._applyBinary(frame, info)
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
