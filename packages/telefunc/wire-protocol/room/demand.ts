export { RoomDemand, DemandReport }
export type { DemandGossip }

import { DEFAULT_TRACK } from './binary.js'
import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { RoomCtrlEnvelope } from './protocol.js'

type DemandGossip = Extract<RoomCtrlEnvelope, { __r: 'want' | 'covers' }>
/** Per member, per track; a `null` track is every track the member publishes. */
type Tracks<V, Track = string | null> = Map<string, Map<Track, V>>
/** Each reporter's lease. A remote reporter re-gossips every heartbeat, so a crashed one lapses; this process's never do. */
type Leases = Map<string, number>
/** Each of the members with each of the tracks. */
type Pairs = { members: string[]; tracks: string[] }
/** Reports wanting the same tracks of every member: they tell under one reporter id, so a member one of them covers is
 *  covered for all of those tracks. */
type Group = { id: string; tracks: readonly string[]; reports: number; covers: Map<string, number> }

/** What a handle receives: pairs, the tracks it wants of every member, and the members it receives those from. */
class Receives {
  readonly pairs: Tracks<true> = new Map()
  readonly tracks = new Set<string>()
  readonly members = new Set<string>()
  get empty(): boolean {
    return this.pairs.size === 0 && this.tracks.size === 0 && this.members.size === 0
  }
}

/** What one handle's planner sets, told as it changes to its process's `RoomDemand`, which tells the other processes. */
class DemandReport {
  /** What the planner set; `flush()` tells the changes since the last one. */
  private readonly _planned = new Receives()
  private _changed = new Receives()
  /** A telling a deadline stopped, resumed by the next one. */
  private _telling: Iterator<void> | null = null
  /** What this report told its process. */
  private readonly _told = new Receives()
  /** `_told.tracks` as the process keys them. */
  private _toldKey = tracksKey([])

  /** Its process's demand, read at each telling: it stays the same while this report has told anything. */
  constructor(private readonly _process: () => RoomDemand) {}

  /** Whether this handle receives the pair. */
  setPair(member: string, track: string | null, on: boolean): void {
    if (on === hasEntry(this._planned.pairs, member, track)) return
    if (on) setEntry(this._planned.pairs, member, track, true)
    else deleteEntry(this._planned.pairs, member, track)
    setEntry(this._changed.pairs, member, track, true)
  }

  /** Whether some holder here wants `track` of every member. */
  setTrack(track: string, on: boolean): void {
    if (on === this._planned.tracks.has(track)) return
    toggle(this._planned.tracks, track, on)
    this._changed.tracks.add(track)
  }

  /** Whether this handle receives from the member every track it wants of every member. */
  setCovers(member: string, on: boolean): void {
    if (on === this._planned.members.has(member)) return
    toggle(this._planned.members, member, on)
    this._changed.members.add(member)
  }

  /** Whether this handle receives a pair of the member, or is covered by it. */
  receivesFrom(member: string): boolean {
    return this._planned.pairs.has(member) || this._planned.members.has(member)
  }

  covers(member: string): boolean {
    return this._planned.members.has(member)
  }

  /** The named and default tracks of the member this handle receives as pairs, read live. */
  *localTracks(member: string): Generator<string> {
    for (const track of this._planned.pairs.get(member)?.keys() ?? []) if (track !== null) yield track
  }

  /** Tells the planned changes, each once. */
  flush(): void {
    this.tellUntil(Infinity)
  }

  /** Whether some planned change isn't told yet. */
  get changed(): boolean {
    return this._telling !== null || !this._changed.empty
  }

  /** Tells the planned changes until `deadline`, a `performance.now()` time, and whether all are told; a call after
   *  one the deadline stopped resumes it, so their order holds. */
  tellUntil(deadline: number): boolean {
    const telling = (this._telling ??= this._tellChanges())
    for (let step = telling.next(); !step.done; step = telling.next()) if (performance.now() >= deadline) return false
    this._telling = null
    return true
  }

  /** Ons go before offs; a member stops being covered before a track is wanted of every member and starts after one
   *  stops, so no pair reads as received that neither state receives. It tells the plan as it was when it started: only
   *  a planning round sets pairs and covers, and none runs until the telling ends, so those are read as it goes; tracks,
   *  which a holder's want sets at any time, are read first, and a change meanwhile is told by the next telling. Each
   *  change it reads is a step. */
  private *_tellChanges(): Generator<void, void, void> {
    const { pairs, tracks, members } = this._changed
    this._changed = new Receives()
    const planned = this._planned
    const told = this._told
    const process = this._process()
    const tracksNext = new Set(told.tracks)
    for (const track of tracks) toggle(tracksNext, track, planned.tracks.has(track))
    for (const [member, byTrack] of pairs)
      for (const track of byTrack.keys()) {
        if (hasEntry(planned.pairs, member, track) && !hasEntry(told.pairs, member, track)) {
          setEntry(told.pairs, member, track, true)
          process.tellPair(member, track, true)
        }
        yield
      }
    for (const member of members) {
      if (!planned.members.has(member) && told.members.has(member)) {
        told.members.delete(member)
        process.tellCovers(this._toldKey, member, false)
      }
      yield
    }
    if (tracksNext.size !== told.tracks.size || [...tracksNext].some((track) => !told.tracks.has(track)))
      yield* this._tellTracks(process, tracksNext)
    for (const member of members) {
      if (planned.members.has(member) && !told.members.has(member)) {
        told.members.add(member)
        process.tellCovers(this._toldKey, member, true)
      }
      yield
    }
    for (const [member, byTrack] of pairs)
      for (const track of byTrack.keys()) {
        if (!hasEntry(planned.pairs, member, track) && hasEntry(told.pairs, member, track)) {
          deleteEntry(told.pairs, member, track)
          process.tellPair(member, track, false)
        }
        yield
      }
  }

  /** Moves the members this report covers to the reports wanting `tracks` of every member, ons first: each covered
   *  member receives the tracks of both, the offs having gone first. A step per member. */
  private *_tellTracks(process: RoomDemand, tracks: Set<string>): Generator<void> {
    const before = this._toldKey
    const after = tracksKey(tracks)
    if (tracks.size > 0) process.tellTracks(after, true)
    for (const member of this._told.members) {
      process.tellCovers(after, member, true)
      yield
    }
    for (const member of this._told.members) {
      process.tellCovers(before, member, false)
      yield
    }
    if (this._told.tracks.size > 0) process.tellTracks(before, false)
    this._told.tracks.clear()
    for (const track of tracks) this._told.tracks.add(track)
    this._toldKey = after
  }
}

/** Aggregates binary-track demand across processes: each tells what its handles receive as it changes, under its own
 *  reporter ids, and a member's owner pushes one `wanted` boolean per track. A track wanted of every member is one
 *  entry, whatever the member count, and every entry is one whatever the number of handles telling it. */
class RoomDemand {
  /** The reporter id of this process's pairs; each group's extends it. */
  private readonly _instanceId = crypto.randomUUID()
  private _groupCount = 0
  /** This process's pairs, with the number of reports telling each. */
  private readonly _pairs: Tracks<number> = new Map()
  private readonly _groups = new Map<string, Group>()
  /** Owner side: every reporter's pairs of the members this process owns. */
  private readonly _remote: Tracks<Leases> = new Map()
  /** The tracks reporters want of every member, and the owned members they receive those from. */
  private readonly _remoteTracks = new Map<string, Leases>()
  private readonly _remoteMembers = new Map<string, Leases>()
  /** Owner side: the named tracks each member published on; a want for every track covers them and the default one. */
  private readonly _published = new Map<string, Set<string>>()

  constructor(
    private readonly _publishWant: (event: DemandGossip) => void,
    private readonly _ownsMember: (id: string) => boolean,
    private readonly _deliver: (member: string, track: string, wanted: boolean) => void,
  ) {}

  /** Whether some report here tells anything. */
  get reporting(): boolean {
    return this._pairs.size > 0 || this._groups.size > 0
  }

  /** One report starts or stops receiving the pair. */
  tellPair(member: string, track: string | null, on: boolean): void {
    const count = (this._pairs.get(member)?.get(track) ?? 0) + (on ? 1 : -1)
    if (count > 0) setEntry(this._pairs, member, track, count)
    else deleteEntry(this._pairs, member, track)
    if (count === (on ? 1 : 0)) this._tell({ __r: 'want', member, track, instance: this._instanceId, on })
  }

  /** One report starts or stops wanting the tracks of `key` of every member. */
  tellTracks(key: string, on: boolean): void {
    const group = this._group(key)
    group.reports += on ? 1 : -1
    if (group.reports === (on ? 1 : 0))
      for (const track of group.tracks) this._tell({ __r: 'want', member: null, track, instance: group.id, on })
    this._dropIfEmpty(key, group)
  }

  /** One report wanting the tracks of `key` starts or stops receiving them all from the member. */
  tellCovers(key: string, member: string, on: boolean): void {
    const group = this._group(key)
    const count = (group.covers.get(member) ?? 0) + (on ? 1 : -1)
    if (count > 0) group.covers.set(member, count)
    else group.covers.delete(member)
    if (count === (on ? 1 : 0)) this._tell({ __r: 'covers', member, instance: group.id, on })
    this._dropIfEmpty(key, group)
  }

  /** Owner side: the member publishes on `track`, which its every-track demand covers from now on. */
  published(member: string, track: string): void {
    if (track === DEFAULT_TRACK) return
    let tracks = this._published.get(member)
    if (tracks?.has(track)) return
    if (!tracks) this._published.set(member, (tracks = new Set()))
    this._changing(this._pairsOf(member, track), () => tracks.add(track))
  }

  /** Another process's report. */
  applyWant(event: DemandGossip): void {
    if (event.instance.startsWith(this._instanceId)) return
    this._take(event, Date.now() + ROOM_DEMAND_TTL_MS)
  }

  /** Tells everything this process told again, which renews its leases elsewhere, then drops the leases that lapsed: a
   *  step per entry. */
  *heartbeat(): Generator<void> {
    const instance = this._instanceId
    for (const [member, byTrack] of this._pairs)
      for (const track of byTrack.keys()) {
        this._publishWant({ __r: 'want', member, track, instance, on: true })
        yield
      }
    for (const group of this._groups.values()) {
      if (group.reports > 0)
        for (const track of group.tracks) {
          this._publishWant({ __r: 'want', member: null, track, instance: group.id, on: true })
          yield
        }
      for (const member of group.covers.keys()) {
        this._publishWant({ __r: 'covers', member, instance: group.id, on: true })
        yield
      }
    }
    const now = Date.now()
    for (const [member, byTrack] of this._remote) {
      for (const track of byTrack.keys()) this._expire(byTrack, track, now, () => this._pairsOf(member, track))
      if (byTrack.size === 0) this._remote.delete(member)
      yield
    }
    for (const track of this._remoteTracks.keys()) {
      this._expire(this._remoteTracks, track, now, (reporter) => this._reachedBy(reporter, track))
      yield
    }
    for (const member of this._remoteMembers.keys()) {
      this._expire(this._remoteMembers, member, now, (reporter) => this._wantedBy(reporter, member))
      yield
    }
  }

  /** Owner side: the member's tracks someone wants. */
  wanted(member: string): string[] {
    const tracks = new Set<string>()
    for (const track of this._remote.get(member)?.keys() ?? []) if (track !== null) tracks.add(track)
    if (this._receives(member, null)) for (const track of this._everyTrackOf(member)) tracks.add(track)
    for (const instance of this._remoteMembers.get(member)?.keys() ?? [])
      for (const [track, leases] of this._remoteTracks) if (leases.has(instance)) tracks.add(track)
    return [...tracks].filter((track) => this._wanted(member, track))
  }

  forgetMember(member: string): void {
    this._remote.delete(member)
    this._remoteMembers.delete(member)
    this._published.delete(member)
  }

  /** This process's own entry: told to the others, and leased here for good. */
  private _tell(event: DemandGossip): void {
    this._publishWant(event)
    this._take(event, Infinity)
  }

  /** Only a member's owner aggregates its demand; ownership is fixed from before the member exists anywhere. */
  private _take(event: DemandGossip, expiresAt: number): void {
    const { instance, on } = event
    if (event.__r === 'want' && event.member === null) {
      const { track } = event
      return this._lease(this._remoteTracks, track, instance, on, expiresAt, () => this._reachedBy(instance, track))
    }
    const { member } = event
    if (!this._ownsMember(member)) return
    if (event.__r === 'covers')
      return this._lease(this._remoteMembers, member, instance, on, expiresAt, () => this._wantedBy(instance, member))
    let byTrack = this._remote.get(member)
    if (!byTrack) this._remote.set(member, (byTrack = new Map()))
    this._lease(byTrack, event.track, instance, on, expiresAt, () => this._pairsOf(member, event.track))
    if (byTrack.size === 0) this._remote.delete(member)
  }

  private _group(key: string): Group {
    let group = this._groups.get(key)
    if (!group) {
      const id = `${this._instanceId}/${++this._groupCount}`
      this._groups.set(key, (group = { id, tracks: JSON.parse(key) as string[], reports: 0, covers: new Map() }))
    }
    return group
  }

  private _dropIfEmpty(key: string, group: Group): void {
    if (group.reports === 0 && group.covers.size === 0) this._groups.delete(key)
  }

  /** Owner side: `change` may flip `pairs`, and each one it flipped is pushed. */
  private _changing({ members, tracks }: Pairs, change: () => void): void {
    const before: boolean[] = []
    for (const member of members) for (const track of tracks) before.push(this._wanted(member, track))
    change()
    let i = 0
    for (const member of members)
      for (const track of tracks) {
        const wanted = this._wanted(member, track)
        if (wanted !== before[i++]) this._deliver(member, track, wanted)
      }
  }

  /** Renews `instance`'s lease on `key`, or takes it or drops it, which may flip `pairs`. */
  private _lease<K>(
    leases: Map<K, Leases>,
    key: K,
    instance: string,
    on: boolean,
    expiresAt: number,
    pairs: () => Pairs,
  ): void {
    const held = leases.get(key)
    if (on && held?.has(instance)) {
      held.set(instance, expiresAt)
      return
    }
    if (!on && !held?.has(instance)) return
    this._changing(pairs(), () => {
      if (on) leases.set(key, (held ?? new Map()).set(instance, expiresAt))
      else if (held!.delete(instance) && held!.size === 0) leases.delete(key)
    })
  }

  /** Drops the leases on `key` that lapsed by `now`. */
  private _expire<K>(leases: Map<K, Leases>, key: K, now: number, pairs: (instance: string) => Pairs): void {
    for (const [instance, expiresAt] of leases.get(key) ?? [])
      if (expiresAt <= now) this._lease(leases, key, instance, false, now, () => pairs(instance))
  }

  /** The owned member's tracks a pair names: for every track, each one it published and the default one. */
  private _pairsOf(member: string, track: string | null): Pairs {
    return {
      members: this._ownsMember(member) ? [member] : [],
      tracks: track === null ? this._everyTrackOf(member) : [track],
    }
  }

  /** `track` of each owned member `instance` receives it from. */
  private _reachedBy(instance: string, track: string): Pairs {
    const members = [...this._remoteMembers].filter(([, leases]) => leases.has(instance)).map(([member]) => member)
    return { members, tracks: [track] }
  }

  /** Each track `instance` wants of every member, of the owned `member`. */
  private _wantedBy(instance: string, member: string): Pairs {
    if (!this._ownsMember(member)) return { members: [], tracks: [] }
    const tracks = [...this._remoteTracks].filter(([, leases]) => leases.has(instance)).map(([track]) => track)
    return { members: [member], tracks }
  }

  private _everyTrackOf(member: string): string[] {
    return [DEFAULT_TRACK, ...(this._published.get(member) ?? [])]
  }

  /** Owner side: whether some reporter receives the member's track. */
  private _wanted(member: string, track: string): boolean {
    const published = track === DEFAULT_TRACK || this._published.get(member)?.has(track) === true
    return this._receives(member, track) || (published && this._receives(member, null)) || this._reaches(member, track)
  }

  private _receives(member: string, track: string | null): boolean {
    return hasEntry(this._remote, member, track)
  }

  /** Whether some reporter that wants `track` of every member receives it from the member. */
  private _reaches(member: string, track: string): boolean {
    const leases = this._remoteTracks.get(track)
    const reporters = this._remoteMembers.get(member)
    if (leases === undefined || reporters === undefined) return false
    for (const instance of reporters.keys()) if (leases.has(instance)) return true
    return false
  }
}

/** The reports wanting the same tracks of every member share this key. */
function tracksKey(tracks: Iterable<string>): string {
  return JSON.stringify([...tracks].sort())
}
function toggle<T>(set: Set<T>, value: T, on: boolean): void {
  if (on) set.add(value)
  else set.delete(value)
}
function hasEntry<V, Track>(tracks: Tracks<V, Track>, member: string, track: Track): boolean {
  return tracks.get(member)?.has(track) === true
}
function setEntry<V, Track>(tracks: Tracks<V, Track>, member: string, track: Track, value: V): void {
  let byTrack = tracks.get(member)
  if (!byTrack) tracks.set(member, (byTrack = new Map()))
  byTrack.set(track, value)
}
function deleteEntry<V, Track>(tracks: Tracks<V, Track>, member: string, track: Track): void {
  const byTrack = tracks.get(member)
  byTrack?.delete(track)
  if (byTrack?.size === 0) tracks.delete(member)
}
