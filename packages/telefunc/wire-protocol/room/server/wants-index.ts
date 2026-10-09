export { WantsIndex }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { DEFAULT_TRACK, emptyBinaryWants, type BinaryWants, type TrackWants } from '../binary.js'
import type { LaneHolder } from './replay.js'
assertIsNotBrowser()

/** A holder's wants as last indexed. */
type Indexed = { textAll: boolean; textMembers: ReadonlySet<string>; announce: boolean; binary: BinaryWants }
/** The holders covering each track of a scope; the `null` key covers every track. */
type TrackCover = Map<string | null, Set<LaneHolder>>

const NOTHING: Indexed = { textAll: false, textMembers: new Set(), announce: false, binary: emptyBinaryWants() }

/** Every holder's wants, aggregated as each one changes, so a change costs the size of that holder's wants. */
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

  /** Indexes the holder's current wants (none once it is gone); returns the members whose binary pairs may change. */
  update(holder: LaneHolder, gone = false): Set<string> | 'all' {
    const prev = this._indexed.get(holder) ?? NOTHING
    const next = gone ? NOTHING : indexedOf(holder)
    const affected = new Set<string>()
    for (const id of new Set([...Object.keys(prev.binary.members), ...Object.keys(next.binary.members)]))
      if (!sameTrackWants(prev.binary.members[id], next.binary.members[id])) affected.add(id)
    const prevKeys = coverKeys(prev.binary.everyMember)
    const nextKeys = coverKeys(next.binary.everyMember)
    // A pair a room-wide coverer adds or drops is one every other coverer suppresses, so any one of them names it.
    let all = false
    for (const key of nextKeys) if (!prevKeys.has(key)) all = !this._addSuppressedOfCoverer(key, affected) || all
    this._apply(holder, prev, -1)
    this._apply(holder, next, 1)
    for (const key of prevKeys) if (!nextKeys.has(key)) all = !this._addSuppressedOfCoverer(key, affected) || all
    if (gone) this._indexed.delete(holder)
    else this._indexed.set(holder, next)
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

  private _apply(holder: LaneHolder, wants: Indexed, delta: 1 | -1): void {
    if (wants.textAll) this._textAll += delta
    if (wants.announce) this._announce += delta
    for (const id of wants.textMembers) addCount(this._textMembers, id, delta)
    applyCover(this._everyMember, wants.binary.everyMember, holder, delta)
    for (const [id, trackWants] of Object.entries(wants.binary.members)) {
      let cover = this._members.get(id)
      if (!cover) this._members.set(id, (cover = new Map()))
      applyCover(cover, trackWants, holder, delta)
      if (cover.size === 0) this._members.delete(id)
    }
  }
}

function indexedOf(holder: LaneHolder): Indexed {
  const text = holder._textDemand()
  return { textAll: text.all, textMembers: text.members, announce: holder._wantsAnnounce, binary: holder._binaryWants }
}

function coverKeys(wants: TrackWants): Set<string | null> {
  return new Set(wants.all ? [null] : wants.tracks)
}

function sameTrackWants(a: TrackWants | undefined, b: TrackWants | undefined): boolean {
  if (a === undefined || b === undefined) return a === b
  const keys = coverKeys(a)
  return a.all === b.all && keys.size === coverKeys(b).size && b.tracks.every((track) => keys.has(track))
}

function applyCover(cover: TrackCover, wants: TrackWants, holder: LaneHolder, delta: 1 | -1): void {
  for (const key of coverKeys(wants)) {
    let holders = cover.get(key)
    if (delta === 1) {
      if (!holders) cover.set(key, (holders = new Set()))
      holders.add(holder)
    } else if (holders) {
      holders.delete(holder)
      if (holders.size === 0) cover.delete(key)
    }
  }
}

function addCount(counts: Map<string, number>, key: string, delta: 1 | -1): void {
  const count = (counts.get(key) ?? 0) + delta
  if (count === 0) counts.delete(key)
  else counts.set(key, count)
}
