export { WantsIndex }
export type { Affected }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { ALL, DEFAULT_TRACK, type All, type BinaryWantChange, type TrackKey } from '../binary.js'
import { type TrackWants } from './binary-wants.js'
import type { LaneHolder } from './replay.js'
import { hasEntry, toggle, toggleEntry } from './sets.js'
assertIsNotBrowser()

/** A holder's text wants as last indexed, and the binary cover keys it is in per scope (`ALL`: every member). */
type Indexed = {
  textAll: boolean
  announce: boolean
  textMembers: Set<string>
  binary: Map<string | All, Set<TrackKey | All>>
}
/** The holders covering each track of a scope; the `ALL` key covers every track. */
type TrackCover = Map<TrackKey | All, Set<LaneHolder>>
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
    const scopeLists: Iterable<string | All>[] = ids
      ? [[ALL], ids]
      : [[ALL], indexed.binary.keys(), wants?.members.keys() ?? []]
    for (const scopes of scopeLists)
      for (const scope of scopes) {
        const next = coverKeys(scope === ALL ? wants?.everyMember : wants?.members.get(scope))
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
          toggle(indexed.textMembers, id, wantsText)
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
    return this._everyMember.has(ALL) || this._members.has(member)
  }

  /** Whether some holder receives every track of the member: a publisher's own suppressed frames are no demand. */
  wantsEveryTrack(member: string): boolean {
    // A holder covering every track wants the default one unless it suppresses the member.
    return this._receives(member, DEFAULT_TRACK, [this._everyMember.get(ALL), this._members.get(member)?.get(ALL)])
  }

  /** How many tracks some holder names of every member. */
  get everyMemberTrackCount(): number {
    return this._everyMember.size - Number(this._everyMember.has(ALL))
  }

  /** The tracks some holder names of every member, `DEFAULT_TRACK` for the default one. */
  *everyMemberTracks(): Generator<TrackKey> {
    for (const track of this._everyMember.keys()) if (track !== ALL) yield track
  }

  /** The tracks some holder names of the member alone and none names of every member. */
  *memberTracksOf(member: string): Generator<TrackKey> {
    for (const track of this._members.get(member)?.keys() ?? [])
      if (track !== ALL && !this._everyMember.has(track)) yield track
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
      this._everyMember.get(ALL),
      own?.get(ALL),
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
    { member: scope, track, on }: BinaryWantChange & { track: TrackKey },
    member: string,
  ): [before: boolean, after: boolean] {
    const coverers = [this._everyMember.get(track), this._members.get(member)?.get(track)]
    const others = this._receives(member, track, coverers, holder)
    if (others || holder._suppressedMembers().has(member)) return [others, others]
    // The holder may name the track of the member in the other scope too.
    const both =
      this._indexed
        .get(holder)
        ?.binary.get(scope === ALL ? member : ALL)
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
  private _cover(holder: LaneHolder, scope: string | All, key: TrackKey | All, on: boolean): Affected {
    const indexed = this._indexedOf(holder)
    if (hasEntry(indexed.binary, scope, key) === on) return NONE
    toggleEntry(indexed.binary, scope, key, on)
    if (scope !== ALL) {
      let cover = this._members.get(scope)
      if (!cover) this._members.set(scope, (cover = new Map()))
      toggleEntry(cover, key, holder, on)
      if (cover.size === 0) this._members.delete(scope)
      if (key !== ALL) toggleEntry(this._trackMembers, key, scope, cover.has(key))
      return new Set([scope])
    }
    // A pair a room-wide coverer adds or drops is one every other coverer of the key suppresses, so any one names it.
    if (!on) toggleEntry(this._everyMember, key, holder, false)
    const [other] = this._everyMember.get(key) ?? []
    if (on) toggleEntry(this._everyMember, key, holder, true)
    if (key === ALL) return other !== undefined ? new Set(other._suppressedMembers()) : 'all'
    this._onEveryMemberCover(key)
    if (other !== undefined) return new Set(other._suppressedMembers())
    // Only the members it is named of alone change lanes; the holder's own may stop or start being received.
    return new Set([...this.membersNaming(key), ...holder._suppressedMembers()])
  }
}

function coverKeys(wants: TrackWants | undefined): Set<TrackKey | All> {
  const keys = new Set<TrackKey | All>(wants?.tracks)
  if (wants?.all) keys.add(ALL)
  return keys
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
