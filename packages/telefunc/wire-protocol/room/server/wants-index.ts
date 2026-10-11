export { WantsIndex }
export type { Affected }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { DEFAULT_TRACK, type BinaryWantChange, type TrackWants } from '../binary.js'
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
type Affected = Set<string> | 'all'

/** Every holder's wants, aggregated as each one changes, so a change costs the size of what it changed. */
class WantsIndex {
  private readonly _indexed = new Map<LaneHolder, Indexed>()
  private _textAll = 0
  private readonly _textMembers = new Map<string, number>()
  private _announce = 0
  private readonly _everyMember: TrackCover = new Map()
  private _everyMemberTracks = 0
  private readonly _members = new Map<string, TrackCover>()

  /** `onEveryMemberTrack` hears each track some holder starts or stops naming of every member. */
  constructor(private readonly _onEveryMemberTrack: (track: string, named: boolean) => void) {}

  get wantsText(): boolean {
    return this._textAll > 0 || this._textMembers.size > 0 || this._announce > 0
  }

  get wantsBinary(): boolean {
    return this._everyMember.size > 0 || this._members.size > 0
  }

  /** Indexes all of the holder's current wants (none once it is gone): its room-wide ones, and those of `members`, or
   *  of every member when omitted. */
  update(holder: LaneHolder, gone = false, members?: Iterable<string>): Affected {
    const indexed = this._indexedOf(holder)
    const ids = members === undefined ? undefined : new Set(members)
    this.updateText(holder, gone, ids)
    const wants = gone ? null : holder._binaryWants
    const scopes = new Set<string | null>([null])
    for (const id of ids ?? [...indexed.binary.keys(), ...(wants?.members.keys() ?? [])]) scopes.add(id)
    const affected = new Set<string>()
    let all = false
    for (const scope of scopes) {
      const next = coverKeys(scope === null ? wants?.everyMember : wants?.members.get(scope))
      const prev = new Set(indexed.binary.get(scope))
      for (const key of prev)
        if (!next.has(key)) all = addAffected(this._cover(holder, scope, key, false), affected) || all
      for (const key of next)
        if (!prev.has(key)) all = addAffected(this._cover(holder, scope, key, true), affected) || all
    }
    if (gone) this._indexed.delete(holder)
    return all ? 'all' : affected
  }

  /** Indexes the holder's current text wants: its room-wide ones, and those of `members`, or of every member when
   *  omitted. */
  updateText(holder: LaneHolder, gone = false, members?: Iterable<string>): void {
    const indexed = this._indexedOf(holder)
    const text = gone ? null : holder._textDemand()
    const textAll = text?.all ?? false
    if (textAll !== indexed.textAll) this._textAll += textAll ? 1 : -1
    indexed.textAll = textAll
    const announce = !gone && holder._wantsAnnounce
    if (announce !== indexed.announce) this._announce += announce ? 1 : -1
    indexed.announce = announce
    for (const id of members ?? new Set([...indexed.textMembers, ...(text?.members ?? [])])) {
      const wantsText = text?.members.has(id) ?? false
      if (wantsText === indexed.textMembers.has(id)) continue
      addCount(this._textMembers, id, wantsText ? 1 : -1)
      if (wantsText) indexed.textMembers.add(id)
      else indexed.textMembers.delete(id)
    }
  }

  /** Indexes one change of the holder's binary wants; the affected members' pairs of its track may have changed. */
  applyBinary(holder: LaneHolder, { member, track, on }: BinaryWantChange): Affected {
    return this._cover(holder, member, track, on)
  }

  /** Whether some holder receives every track of the member: a publisher's own suppressed frames are no demand. */
  wantsEveryTrack(member: string): boolean {
    // A holder covering every track wants the default one unless it suppresses the member.
    return this._receives(member, DEFAULT_TRACK, [this._everyMember.get(null), this._members.get(member)?.get(null)])
  }

  /** The tracks some holder names for the member, `DEFAULT_TRACK` for the default one: those of every member first. */
  *namedTracksOf(member: string): Generator<string> {
    for (const track of this._everyMember.keys()) if (track !== null) yield track
    yield* this.memberTracksOf(member)
  }

  /** The tracks some holder names of the member alone. */
  *memberTracksOf(member: string): Generator<string> {
    for (const track of this._members.get(member)?.keys() ?? [])
      if (track !== null && !this._everyMember.has(track)) yield track
  }

  /** Whether some holder names `track` of every member. */
  namesEveryMember(track: string): boolean {
    return this._everyMember.has(track)
  }

  /** How many tracks some holder names of every member. */
  get everyMemberTracks(): number {
    return this._everyMemberTracks
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
    if ((keys?.has(key) ?? false) === on) return new Set()
    if (!keys) indexed.binary.set(scope, (keys = new Set()))
    if (on) keys.add(key)
    else keys.delete(key)
    if (keys.size === 0) indexed.binary.delete(scope)
    if (scope !== null) {
      let cover = this._members.get(scope)
      if (!cover) this._members.set(scope, (cover = new Map()))
      setCover(cover, key, holder, on)
      if (cover.size === 0) this._members.delete(scope)
      return new Set([scope])
    }
    // A pair a room-wide coverer adds or drops is one every other coverer of the key suppresses, so any one names it.
    if (!on) setCover(this._everyMember, key, holder, false)
    const [other] = this._everyMember.get(key) ?? []
    if (on) setCover(this._everyMember, key, holder, true)
    if (other !== undefined) return new Set(other._suppressedMembers())
    if (key !== null) {
      this._everyMemberTracks += on ? 1 : -1
      this._onEveryMemberTrack(key, on)
    }
    return 'all'
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
