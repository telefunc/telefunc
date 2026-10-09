export { WantsIndex }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { DEFAULT_TRACK, emptyTrackWants, sameTrackWants, type TrackWants } from '../binary.js'
import type { LaneHolder } from './replay.js'
assertIsNotBrowser()

/** A holder's wants as last indexed, owned here so a change is compared against them. */
type Indexed = {
  textAll: boolean
  announce: boolean
  textMembers: Set<string>
  everyMember: TrackWants
  members: Map<string, TrackWants>
}
/** The holders covering each track of a scope; the `null` key covers every track. */
type TrackCover = Map<string | null, Set<LaneHolder>>

/** Every holder's wants, aggregated as each one changes, so a change costs the size of what it changed. */
class WantsIndex {
  private readonly _indexed = new Map<LaneHolder, Indexed>()
  private _textAll = 0
  private readonly _textMembers = new Map<string, number>()
  private _announce = 0
  private readonly _everyMember: TrackCover = new Map()
  private readonly _members = new Map<string, TrackCover>()

  get wantsText(): boolean {
    return this._textAll > 0 || this._textMembers.size > 0 || this._announce > 0
  }

  get wantsBinary(): boolean {
    return this._everyMember.size > 0 || this._members.size > 0
  }

  /** Indexes the holder's current wants (none once it is gone): its room-wide ones, and those of `members`, or of
   *  every member when omitted. Returns the members whose binary pairs may change. */
  update(holder: LaneHolder, gone = false, members?: Iterable<string>): Set<string> | 'all' {
    let indexed = this._indexed.get(holder)
    if (!indexed) this._indexed.set(holder, (indexed = notIndexed()))
    const text = gone ? null : holder._textDemand()
    const binary = gone ? null : holder._binaryWants
    const textAll = text?.all ?? false
    if (textAll !== indexed.textAll) this._textAll += textAll ? 1 : -1
    indexed.textAll = textAll
    const announce = !gone && holder._wantsAnnounce
    if (announce !== indexed.announce) this._announce += announce ? 1 : -1
    indexed.announce = announce
    const affected = new Set<string>()
    const ids =
      members ??
      new Set([
        ...indexed.textMembers,
        ...indexed.members.keys(),
        ...(text?.members ?? []),
        ...Object.keys(binary?.members ?? {}),
      ])
    for (const id of ids) {
      const wantsText = text?.members.has(id) ?? false
      if (wantsText !== indexed.textMembers.has(id)) {
        addCount(this._textMembers, id, wantsText ? 1 : -1)
        if (wantsText) indexed.textMembers.add(id)
        else indexed.textMembers.delete(id)
      }
      const prev = indexed.members.get(id)
      const next = binary?.members[id]
      if (sameTrackWants(prev, next)) continue
      affected.add(id)
      let cover = this._members.get(id)
      if (!cover) this._members.set(id, (cover = new Map()))
      recover(cover, holder, prev, next)
      if (cover.size === 0) this._members.delete(id)
      if (next) indexed.members.set(id, next)
      else indexed.members.delete(id)
    }
    const prevKeys = coverKeys(indexed.everyMember)
    const next = binary?.everyMember ?? emptyTrackWants()
    const nextKeys = coverKeys(next)
    // A pair a room-wide coverer adds or drops is one every other coverer suppresses, so any one of them names it.
    let all = false
    for (const key of nextKeys) if (!prevKeys.has(key)) all = !this._addSuppressedOfCoverer(key, affected) || all
    recover(this._everyMember, holder, indexed.everyMember, next)
    for (const key of prevKeys) if (!nextKeys.has(key)) all = !this._addSuppressedOfCoverer(key, affected) || all
    indexed.everyMember = next
    if (gone) this._indexed.delete(holder)
    return all ? 'all' : affected
  }

  /** The member's tracks some holder wants: a named track, or every one it publishes. */
  tracksOf(member: string, published: () => string[]): string[] {
    const own = this._members.get(member)
    if (this._everyMember.has(null) || own?.has(null)) return [DEFAULT_TRACK, ...published()]
    const tracks = new Set(this._everyMember.keys() as Iterable<string>)
    for (const track of (own?.keys() ?? []) as Iterable<string>) tracks.add(track)
    return [...tracks]
  }

  /** Whether some holder receives the pair: a publisher's own suppressed frames are no demand and need no lane. */
  wantsPair(member: string, track: string): boolean {
    const own = this._members.get(member)
    for (const coverers of [
      this._everyMember.get(null),
      this._everyMember.get(track),
      own?.get(null),
      own?.get(track),
    ]) {
      // Each coverer suppresses only members it holds, so this stops within a few.
      for (const holder of coverers ?? []) if (holder._wantsBinary(member, track)) return true
    }
    return false
  }

  private _addSuppressedOfCoverer(key: string | null, into: Set<string>): boolean {
    const coverers = this._everyMember.get(null) ?? (key === null ? undefined : this._everyMember.get(key))
    const [coverer] = coverers ?? []
    if (coverer === undefined) return false
    for (const id of coverer._suppressedMembers()) into.add(id)
    return true
  }
}

function notIndexed(): Indexed {
  return { textAll: false, announce: false, textMembers: new Set(), everyMember: emptyTrackWants(), members: new Map() }
}

function coverKeys(wants: TrackWants | undefined): Set<string | null> {
  return new Set(wants === undefined ? [] : wants.all ? [null] : wants.tracks)
}

/** Moves the holder from the keys only `prev` covers to the keys only `next` covers. */
function recover(
  cover: TrackCover,
  holder: LaneHolder,
  prev: TrackWants | undefined,
  next: TrackWants | undefined,
): void {
  const prevKeys = coverKeys(prev)
  const nextKeys = coverKeys(next)
  for (const key of prevKeys) {
    if (nextKeys.has(key)) continue
    const holders = cover.get(key)
    holders?.delete(holder)
    if (holders?.size === 0) cover.delete(key)
  }
  for (const key of nextKeys) {
    if (prevKeys.has(key)) continue
    let holders = cover.get(key)
    if (!holders) cover.set(key, (holders = new Set()))
    holders.add(holder)
  }
}

function addCount(counts: Map<string, number>, key: string, delta: 1 | -1): void {
  const count = (counts.get(key) ?? 0) + delta
  if (count === 0) counts.delete(key)
  else counts.set(key, count)
}
