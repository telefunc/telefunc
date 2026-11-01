export { WantsIndex }
export type { Affected, WantsIndexOptions, WantsIndexOwner }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { getOrCreate } from '../../utils/getOrCreate.js'
import {
  EVERY_MEMBER,
  EVERY_TRACK,
  DEFAULT_TRACK,
  type BinaryWantChange,
  type EveryMember,
  type EveryTrack,
  type TrackKey,
} from '../../shared/binary.js'
import type { BinaryWants, TrackWants } from './binaryWants.js'
import type { LaneHolder } from '../lifecycle/replay.js'
import { hasEntry, toggle, toggleEntry } from '../../utils/sets.js'
assertIsNotBrowser()

/** A holder's text wants as last indexed, and the binary cover keys it is in per scope (`EVERY_MEMBER`: every member). */
type Indexed = {
  textAll: boolean
  announce: boolean
  textMembers: Set<string>
  binary: Map<string | EveryMember, Set<TrackKey | EveryTrack>>
}

/** The holders covering each track of a scope; the `EVERY_TRACK` key covers every track. */
type TrackCover = Map<TrackKey | EveryTrack, Set<LaneHolder>>
/** The members whose binary pairs a change may have changed. */
type Affected = ReadonlySet<string> | 'all'
const NONE: Affected = new Set()

/** The plan whose holders' wants the index aggregates. */
interface WantsIndexOwner {
  /** The holders naming `track` of every member changed. */
  onEveryMemberCoverChanged(track: string): void
}

type WantsIndexOptions = { owner: WantsIndexOwner }

/** Every holder's wants, aggregated as each one changes, so a change costs the size of what it changed. */
class WantsIndex {
  private readonly indexed = new Map<LaneHolder, Indexed>()
  private textAll = 0
  private readonly textMembers = new Map<string, number>()
  private announce = 0
  private readonly everyMember: TrackCover = new Map()
  private readonly members = new Map<string, TrackCover>()
  /** Per named track, the members some holder names it of alone. */
  private readonly namedAloneOf = new Map<string, Set<string>>()

  private readonly owner: WantsIndexOwner

  constructor({ owner }: WantsIndexOptions) {
    this.owner = owner
  }

  get wantsText(): boolean {
    return this.textAll > 0 || this.textMembers.size > 0 || this.announce > 0
  }

  get wantsBinary(): boolean {
    return this.everyMember.size > 0 || this.members.size > 0
  }

  /** Indexes all of the holder's current wants (none once it is gone), a text member or a scope per step: its room-wide
   *  ones, and those of `members`, or of every member when omitted. Each step yields the members it affected. */
  *update(holder: LaneHolder, opts?: { gone?: boolean; members?: Iterable<string> }): Generator<Affected> {
    const gone = opts?.gone === true
    const ids = opts?.members === undefined ? undefined : new Set(opts.members)
    for (const _ of this.updateText(holder, { gone, members: ids })) yield NONE
    const indexed = this.indexedOf(holder)
    const wants = gone ? null : holder.binaryWants
    // Read live: a scope listed twice is indexed already when its second step comes.
    const scopeLists: Iterable<string | EveryMember>[] = ids
      ? [[EVERY_MEMBER], ids]
      : [[EVERY_MEMBER], indexed.binary.keys(), wants?.members.keys() ?? []]
    for (const scopes of scopeLists) {
      for (const scope of scopes) yield this.updateScope(holder, indexed, scope, wants)
    }
    this.forgetIfIdle(holder)
  }

  /** Indexes the holder's binary wants of one scope, and the members it affected. */
  private updateScope(
    holder: LaneHolder,
    indexed: Indexed,
    scope: string | EveryMember,
    wants: BinaryWants | null,
  ): Affected {
    const next = coverKeys(scope === EVERY_MEMBER ? wants?.everyMember : wants?.members.get(scope))
    const prev = new Set(indexed.binary.get(scope))
    const affected = new Set<string>()
    let all = false
    for (const key of prev) {
      if (!next.has(key)) all = addAffected(this.cover(holder, scope, key, false), affected) || all
    }
    for (const key of next) {
      if (!prev.has(key)) all = addAffected(this.cover(holder, scope, key, true), affected) || all
    }
    return all ? 'all' : affected
  }

  /** Indexes the holder's current text wants, a member per step: its room-wide ones, and those of `members`, or of
   *  every member when omitted. */
  *updateText(holder: LaneHolder, opts?: { gone?: boolean; members?: Iterable<string> }): Generator<void> {
    const gone = opts?.gone === true
    const members = opts?.members
    const indexed = this.indexedOf(holder)
    const text = gone ? null : holder.textDemand()
    const textAll = text?.all ?? false
    if (textAll !== indexed.textAll) this.textAll += textAll ? 1 : -1
    indexed.textAll = textAll
    const announce = !gone && holder.wantsAnnounce
    if (announce !== indexed.announce) this.announce += announce ? 1 : -1
    indexed.announce = announce
    // Read live: a member listed twice is indexed already when its second step comes.
    for (const ids of members ? [members] : [indexed.textMembers, text?.members ?? []]) {
      for (const id of ids) {
        const wantsText = text?.members.has(id) ?? false
        if (wantsText !== indexed.textMembers.has(id)) {
          addCount(this.textMembers, id, wantsText ? 1 : -1)
          toggle(indexed.textMembers, id, wantsText)
        }
        yield
      }
    }
    this.forgetIfIdle(holder)
  }

  /** Indexes one change of the holder's binary wants; the affected members' pairs of its track may have changed. */
  applyBinary(holder: LaneHolder, { member, track, on }: BinaryWantChange): Affected {
    const affected = this.cover(holder, member, track, on)
    this.forgetIfIdle(holder)
    return affected
  }

  /** Whether some holder names a binary want of the member, or wants every track of every member: a track named of
   *  every member is fetched for all of them on one lane. */
  concerns(member: string): boolean {
    return this.everyMember.has(EVERY_TRACK) || this.members.has(member)
  }

  /** Whether some holder receives every track of the member: a publisher's own suppressed frames are no demand. */
  wantsEveryTrack(member: string): boolean {
    // A holder covering every track wants the default one unless it suppresses the member.
    return this.coveredBy(member, DEFAULT_TRACK, [
      this.everyMember.get(EVERY_TRACK),
      this.members.get(member)?.get(EVERY_TRACK),
    ])
  }

  /** How many tracks some holder names of every member. */
  get everyMemberTrackCount(): number {
    return this.everyMember.size - Number(this.everyMember.has(EVERY_TRACK))
  }

  /** The tracks some holder names of every member, `DEFAULT_TRACK` for the default one. */
  *everyMemberTracks(): Generator<TrackKey> {
    for (const track of this.everyMember.keys()) if (track !== EVERY_TRACK) yield track
  }

  /** The tracks some holder names of the member alone and none names of every member. */
  *memberTracksOf(member: string): Generator<TrackKey> {
    for (const track of this.members.get(member)?.keys() ?? []) {
      if (track !== EVERY_TRACK && !this.everyMember.has(track)) yield track
    }
  }

  /** Whether some holder names `track` of every member. */
  namesEveryMember(track: string): boolean {
    return this.everyMember.has(track)
  }

  /** Whether some holder receives the member's `track`, named or with every track. */
  receives(member: string, track: string): boolean {
    const own = this.members.get(member)
    return this.coveredBy(member, track, [
      this.everyMember.get(track),
      own?.get(track),
      this.everyMember.get(EVERY_TRACK),
      own?.get(EVERY_TRACK),
    ])
  }

  /** Whether some holder that names the track receives it from the member. */
  wantsNamedPair(member: string, track: string): boolean {
    return this.coveredBy(member, track, [this.everyMember.get(track), this.members.get(member)?.get(track)])
  }

  /** After `applyBinary(holder, change)` for a named track, whether `wantsNamedPair` held for `member` before and
   *  after it: the change flipped it only where no other holder receives the pair. */
  namedPairChange(
    holder: LaneHolder,
    { member: scope, track, on }: BinaryWantChange & { track: TrackKey },
    member: string,
  ): [before: boolean, after: boolean] {
    const coverers = [this.everyMember.get(track), this.members.get(member)?.get(track)]
    const others = this.coveredBy(member, track, coverers, holder)
    if (others || holder.suppressedMembers().has(member)) return [others, others]
    // The holder may name the track of the member in the other scope too.
    const indexed = this.indexed.get(holder)
    const both =
      indexed !== undefined && hasEntry(indexed.binary, scope === EVERY_MEMBER ? member : EVERY_MEMBER, track)
    return [both || !on, both || on]
  }

  private coveredBy(
    member: string,
    track: string,
    coverers: Array<Set<LaneHolder> | undefined>,
    except?: LaneHolder,
  ): boolean {
    // Each coverer suppresses only members it holds, so this stops within a few.
    for (const holders of coverers) {
      for (const holder of holders ?? []) if (holder !== except && holder.wantsBinary(member, track)) return true
    }
    return false
  }

  /** A holder that wants nothing is let go, as the index is all that holds a handle no longer listening. */
  private forgetIfIdle(holder: LaneHolder): void {
    const indexed = this.indexed.get(holder)
    if (indexed === undefined || indexed.textAll || indexed.announce) return
    if (indexed.textMembers.size === 0 && indexed.binary.size === 0) this.indexed.delete(holder)
  }

  private indexedOf(holder: LaneHolder): Indexed {
    return getOrCreate(this.indexed, holder, () => ({
      textAll: false,
      announce: false,
      textMembers: new Set(),
      binary: new Map(),
    }))
  }

  /** Puts the holder in or out of one cover key of a scope. */
  private cover(holder: LaneHolder, scope: string | EveryMember, key: TrackKey | EveryTrack, on: boolean): Affected {
    const indexed = this.indexedOf(holder)
    if (hasEntry(indexed.binary, scope, key) === on) return NONE
    toggleEntry(indexed.binary, scope, key, on)
    if (scope !== EVERY_MEMBER) {
      const cover = getOrCreate(this.members, scope, () => new Map())
      toggleEntry(cover, key, holder, on)
      if (cover.size === 0) this.members.delete(scope)
      if (key !== EVERY_TRACK) toggleEntry(this.namedAloneOf, key, scope, cover.has(key))
      return new Set([scope])
    }
    // A pair a room-wide coverer adds or drops is one every other coverer of the key suppresses, so any one names it.
    if (!on) toggleEntry(this.everyMember, key, holder, false)
    const [other] = this.everyMember.get(key) ?? []
    if (on) toggleEntry(this.everyMember, key, holder, true)
    if (key === EVERY_TRACK) return other !== undefined ? new Set(other.suppressedMembers()) : 'all'
    this.owner.onEveryMemberCoverChanged(key)
    if (other !== undefined) return new Set(other.suppressedMembers())
    // Only the members it is named of alone change lanes; the holder's own may stop or start being received.
    const affected = new Set(this.namedAloneOf.get(key))
    for (const member of holder.suppressedMembers()) affected.add(member)
    return affected
  }
}

function coverKeys(wants: TrackWants | undefined): Set<TrackKey | EveryTrack> {
  const keys = new Set<TrackKey | EveryTrack>(wants?.tracks)
  if (wants?.all) keys.add(EVERY_TRACK)
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
