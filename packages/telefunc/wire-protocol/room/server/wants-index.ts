export { WantsIndex }
export type { Affected }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { DEFAULT_TRACK, type BinaryWantChange } from '../binary.js'
import { type TrackWants } from './binary-wants.js'
import type { LaneHolder } from './replay.js'
assertIsNotBrowser()

/** A holder's text wants as last indexed, and the binary cover keys it is in per scope (`null`: every member). */
type Indexed = {
  textAll: boolean
  announce: boolean
  textMembers: Set<string>
  binary: Map<string | null, Set<string | null>>
}
/** The holders covering each track of a scope; the `null` key covers every track. */
type TrackCover = Map<string | null, Set<LaneHolder>>
/** The members whose binary pairs a change may have changed. */
type Affected = ReadonlySet<string> | 'all'
const NONE: Affected = new Set()

/** Every holder's wants, aggregated as each one changes, so a change costs the size of what it changed. */
class WantsIndex {
  private readonly _indexed = new Map<LaneHolder, Indexed>()
  private _textAll = 0
  private readonly _textMembers = new Map<string, number>()
  private _announce = 0
  private readonly _everyMember: TrackCover = new Map()
  private readonly _members = new Map<string, TrackCover>()
  /** Per named track, the members some holder names it of alone. */
  private readonly _trackMembers = new Map<string, Set<string>>()

  /** `onEveryMemberCover` hears each track whose holders naming it of every member changed. */
  constructor(private readonly _onEveryMemberCover: (track: string) => void) {}

  get wantsText(): boolean {
    return this._textAll > 0 || this._textMembers.size > 0 || this._announce > 0
  }

  get wantsBinary(): boolean {
    return this._everyMember.size > 0 || this._members.size > 0
  }

  /** Indexes all of the holder's current wants (none once it is gone), a text member or a scope per step: its room-wide
   *  ones, and those of `members`, or of every member when omitted. Each step yields the members it affected. */
  *update(holder: LaneHolder, gone = false, members?: Iterable<string>): Generator<Affected> {
    const ids = members === undefined ? undefined : new Set(members)
    for (const _ of this.updateText(holder, gone, ids)) yield NONE
    const indexed = this._indexedOf(holder)
    const wants = gone ? null : holder._binaryWants
    // Read live: a scope listed twice is indexed already when its second step comes.
    for (const scopes of ids ? [[null], ids] : [[null], indexed.binary.keys(), wants?.members.keys() ?? []])
      for (const scope of scopes) {
        const next = coverKeys(scope === null ? wants?.everyMember : wants?.members.get(scope))
        const prev = new Set(indexed.binary.get(scope))
        const affected = new Set<string>()
        let all = false
        for (const key of prev)
          if (!next.has(key)) all = addAffected(this._cover(holder, scope, key, false), affected) || all
        for (const key of next)
          if (!prev.has(key)) all = addAffected(this._cover(holder, scope, key, true), affected) || all
        yield all ? 'all' : affected
      }
    if (gone) this._indexed.delete(holder)
  }

  /** Indexes the holder's current text wants, a member per step: its room-wide ones, and those of `members`, or of every
   *  member when omitted. */
  *updateText(holder: LaneHolder, gone = false, members?: Iterable<string>): Generator<void> {
    const indexed = this._indexedOf(holder)
    const text = gone ? null : holder._textDemand()
    const textAll = text?.all ?? false
    if (textAll !== indexed.textAll) this._textAll += textAll ? 1 : -1
    indexed.textAll = textAll
    const announce = !gone && holder._wantsAnnounce
    if (announce !== indexed.announce) this._announce += announce ? 1 : -1
    indexed.announce = announce
    // Read live: a member listed twice is indexed already when its second step comes.
    for (const ids of members ? [members] : [indexed.textMembers, text?.members ?? []])
      for (const id of ids) {
        const wantsText = text?.members.has(id) ?? false
        if (wantsText !== indexed.textMembers.has(id)) {
          addCount(this._textMembers, id, wantsText ? 1 : -1)
          if (wantsText) indexed.textMembers.add(id)
          else indexed.textMembers.delete(id)
        }
        yield
      }
  }

  /** Indexes one change of the holder's binary wants; the affected members' pairs of its track may have changed. */
  applyBinary(holder: LaneHolder, { member, track, on }: BinaryWantChange): Affected {
    return this._cover(holder, member, track, on)
  }

  /** Whether some holder names a binary want of the member, or wants every track of every member: a track named of
   *  every member is fetched for all of them on one lane. */
  concerns(member: string): boolean {
    return this._everyMember.has(null) || this._members.has(member)
  }

  /** Whether some holder receives every track of the member: a publisher's own suppressed frames are no demand. */
  wantsEveryTrack(member: string): boolean {
    // A holder covering every track wants the default one unless it suppresses the member.
    return this._receives(member, DEFAULT_TRACK, [this._everyMember.get(null), this._members.get(member)?.get(null)])
  }

  /** How many tracks some holder names of every member. */
  get everyMemberTrackCount(): number {
    return this._everyMember.size - Number(this._everyMember.has(null))
  }

  /** The tracks some holder names of every member, `DEFAULT_TRACK` for the default one. */
  *everyMemberTracks(): Generator<string> {
    for (const track of this._everyMember.keys()) if (track !== null) yield track
  }

  /** The tracks some holder names of the member alone and none names of every member. */
  *memberTracksOf(member: string): Generator<string> {
    for (const track of this._members.get(member)?.keys() ?? [])
      if (track !== null && !this._everyMember.has(track)) yield track
  }

  /** The members some holder names `track` of alone. */
  membersNaming(track: string): Iterable<string> {
    return this._trackMembers.get(track) ?? []
  }

  /** Whether some holder names `track` of every member. */
  namesEveryMember(track: string): boolean {
    return this._everyMember.has(track)
  }

  /** Whether some holder receives the member's `track`, named or with every track. */
  receives(member: string, track: string): boolean {
    const own = this._members.get(member)
    return this._receives(member, track, [
      this._everyMember.get(track),
      own?.get(track),
      this._everyMember.get(null),
      own?.get(null),
    ])
  }

  /** Whether some holder that names the track receives it from the member. */
  wantsNamedPair(member: string, track: string): boolean {
    return this._receives(member, track, [this._everyMember.get(track), this._members.get(member)?.get(track)])
  }

  /** After `applyBinary(holder, change)` for a named track, whether `wantsNamedPair` held for `member` before and
   *  after it: the change flipped it only where no other holder receives the pair. */
  namedPairChange(
    holder: LaneHolder,
    { member: scope, track, on }: BinaryWantChange & { track: string },
    member: string,
  ): [before: boolean, after: boolean] {
    const coverers = [this._everyMember.get(track), this._members.get(member)?.get(track)]
    const others = this._receives(member, track, coverers, holder)
    if (others || holder._suppressedMembers().has(member)) return [others, others]
    // The holder may name the track of the member in the other scope too.
    const both =
      this._indexed
        .get(holder)
        ?.binary.get(scope === null ? member : null)
        ?.has(track) === true
    return [both || !on, both || on]
  }

  private _receives(
    member: string,
    track: string,
    coverers: Array<Set<LaneHolder> | undefined>,
    except?: LaneHolder,
  ): boolean {
    // Each coverer suppresses only members it holds, so this stops within a few.
    for (const holders of coverers)
      for (const holder of holders ?? []) if (holder !== except && holder._wantsBinary(member, track)) return true
    return false
  }

  private _indexedOf(holder: LaneHolder): Indexed {
    let indexed = this._indexed.get(holder)
    if (!indexed)
      this._indexed.set(
        holder,
        (indexed = { textAll: false, announce: false, textMembers: new Set(), binary: new Map() }),
      )
    return indexed
  }

  /** Puts the holder in or out of one cover key of a scope. */
  private _cover(holder: LaneHolder, scope: string | null, key: string | null, on: boolean): Affected {
    const indexed = this._indexedOf(holder)
    let keys = indexed.binary.get(scope)
    if ((keys?.has(key) ?? false) === on) return NONE
    if (!keys) indexed.binary.set(scope, (keys = new Set()))
    if (on) keys.add(key)
    else keys.delete(key)
    if (keys.size === 0) indexed.binary.delete(scope)
    if (scope !== null) {
      let cover = this._members.get(scope)
      if (!cover) this._members.set(scope, (cover = new Map()))
      setCover(cover, key, holder, on)
      if (cover.size === 0) this._members.delete(scope)
      if (key !== null) {
        let members = this._trackMembers.get(key)
        if (cover.has(key)) {
          if (!members) this._trackMembers.set(key, (members = new Set()))
          members.add(scope)
        } else if (members?.delete(scope) && members.size === 0) this._trackMembers.delete(key)
      }
      return new Set([scope])
    }
    // A pair a room-wide coverer adds or drops is one every other coverer of the key suppresses, so any one names it.
    if (!on) setCover(this._everyMember, key, holder, false)
    const [other] = this._everyMember.get(key) ?? []
    if (on) setCover(this._everyMember, key, holder, true)
    if (key === null) return other !== undefined ? new Set(other._suppressedMembers()) : 'all'
    this._onEveryMemberCover(key)
    if (other !== undefined) return new Set(other._suppressedMembers())
    // Only the members it is named of alone change lanes; the holder's own may stop or start being received.
    return new Set([...this.membersNaming(key), ...holder._suppressedMembers()])
  }
}

function coverKeys(wants: TrackWants | undefined): Set<string | null> {
  const keys = new Set<string | null>(wants?.tracks)
  if (wants?.all) keys.add(null)
  return keys
}

function setCover(cover: TrackCover, key: string | null, holder: LaneHolder, on: boolean): void {
  let holders = cover.get(key)
  if (on) {
    if (!holders) cover.set(key, (holders = new Set()))
    holders.add(holder)
    return
  }
  holders?.delete(holder)
  if (holders?.size === 0) cover.delete(key)
}

/** Whether `affected` is every member; else its members are added to `into`. */
function addAffected(affected: Affected, into: Set<string>): boolean {
  if (affected === 'all') return true
  for (const id of affected) into.add(id)
  return false
}

function addCount(counts: Map<string, number>, key: string, delta: 1 | -1): void {
  const count = (counts.get(key) ?? 0) + delta
  if (count === 0) counts.delete(key)
  else counts.set(key, count)
}
