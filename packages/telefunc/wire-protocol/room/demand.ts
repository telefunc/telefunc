export { RoomDemand }

import { DEFAULT_TRACK } from './binary.js'
import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { RoomCtrlEnvelope } from './protocol.js'

type DemandGossip = Extract<RoomCtrlEnvelope, { __r: 'want' | 'covers' }>
/** Per member, per track; a `null` track is every track the member publishes. */
type Tracks<V, Track = string | null> = Map<string, Map<Track, V>>
/** Each reporting instance's lease. A reporter re-gossips every heartbeat, so a crashed one lapses. */
type Leases = Map<string, number>
/** Each of the members with each of the tracks. */
type Pairs = { members: string[]; tracks: string[] }

/** What an instance receives: pairs, the tracks it wants of every member, and the members it receives those from. */
class Receives {
  readonly pairs: Tracks<true> = new Map()
  readonly tracks = new Set<string>()
  readonly members = new Set<string>()
  get empty(): boolean {
    return this.pairs.size === 0 && this.tracks.size === 0 && this.members.size === 0
  }
}

/** Aggregates binary-track demand across instances: each gossips what it receives as it changes, and a member's owner
 *  pushes one `wanted` boolean per track. A track wanted of every member is one entry, whatever the member count. */
class RoomDemand {
  private readonly _instanceId = crypto.randomUUID()
  /** What the planner set; `flush()` tells the changes since the last one. */
  private readonly _planned = new Receives()
  private _changed = new Receives()
  /** A telling a deadline stopped, resumed by the next one. */
  private _telling: Iterator<void> | null = null
  /** What the other instances were told, and this one's owner side reads. */
  private readonly _told = new Receives()
  /** Owner side: the other instances' pairs of the members this instance owns. */
  private readonly _remote: Tracks<Leases> = new Map()
  /** The tracks other instances want of every member, and the owned members they receive those from. */
  private readonly _remoteTracks = new Map<string, Leases>()
  private readonly _remoteMembers = new Map<string, Leases>()
  /** Owner side: the named tracks each member published on; a want for every track covers them and the default one. */
  private readonly _published = new Map<string, Set<string>>()

  constructor(
    private readonly _publishWant: (event: DemandGossip) => void,
    private readonly _ownsMember: (id: string) => boolean,
    private readonly _deliver: (member: string, track: string, wanted: boolean) => void,
  ) {}

  /** Whether this instance receives the pair. */
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

  /** Whether this instance receives from the member every track it wants of every member. */
  setCovers(member: string, on: boolean): void {
    if (on === this._planned.members.has(member)) return
    toggle(this._planned.members, member, on)
    this._changed.members.add(member)
  }

  covers(member: string): boolean {
    return this._planned.members.has(member)
  }

  /** The named and default tracks of the member this instance receives as pairs. */
  localTracks(member: string): string[] {
    return [...(this._planned.pairs.get(member)?.keys() ?? [])].filter((track) => track !== null)
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
   *  stops, so no pair reads as received that neither state receives. It tells the plan as it was when it started: a
   *  planner running between two of its slices is told by the next telling. */
  private *_tellChanges(): Generator<void, void, void> {
    const { pairs, tracks, members } = this._changed
    this._changed = new Receives()
    const planned = this._planned
    const told = this._told
    const pairsOn = [...entriesOf(pairs)].map(
      ([member, track]) => [member, track, hasEntry(planned.pairs, member, track)] as const,
    )
    const membersOn = [...members].map((member) => [member, planned.members.has(member)] as const)
    const tracksOn = [...tracks].map((track) => [track, planned.tracks.has(track)] as const)
    for (const [member, track, on] of pairsOn)
      if (on && !hasEntry(told.pairs, member, track)) yield this._tellPair(member, track, true)
    for (const [member, on] of membersOn) if (!on && told.members.has(member)) yield this._tellCovers(member, false)
    for (const [track, on] of tracksOn) if (!on && told.tracks.has(track)) yield this._tellTrack(track, false)
    for (const [track, on] of tracksOn) if (on && !told.tracks.has(track)) yield this._tellTrack(track, true)
    for (const [member, on] of membersOn) if (on && !told.members.has(member)) yield this._tellCovers(member, true)
    for (const [member, track, on] of pairsOn)
      if (!on && hasEntry(told.pairs, member, track)) yield this._tellPair(member, track, false)
  }

  /** Owner side: the member publishes on `track`, which its every-track demand covers from now on. */
  published(member: string, track: string): void {
    if (track === DEFAULT_TRACK) return
    let tracks = this._published.get(member)
    if (tracks?.has(track)) return
    if (!tracks) this._published.set(member, (tracks = new Set()))
    this._changing(this._pairsOf(member, track), () => tracks.add(track))
  }

  /** Only a member's owner aggregates its demand; ownership is fixed from before the member exists anywhere. */
  applyWant(event: DemandGossip): void {
    const { instance, on } = event
    if (instance === this._instanceId) return
    if (event.__r === 'want' && event.member === null) {
      const { track } = event
      return this._lease(this._remoteTracks, track, instance, on, () => this._reachedBy(instance, track))
    }
    const { member } = event
    if (!this._ownsMember(member)) return
    if (event.__r === 'covers')
      return this._lease(this._remoteMembers, member, instance, on, () => this._wantedBy(instance, member))
    let byTrack = this._remote.get(member)
    if (!byTrack) this._remote.set(member, (byTrack = new Map()))
    this._lease(byTrack, event.track, instance, on, () => this._pairsOf(member, event.track))
    if (byTrack.size === 0) this._remote.delete(member)
  }

  heartbeat(): void {
    const instance = this._instanceId
    for (const [member, track] of entriesOf(this._told.pairs))
      this._publishWant({ __r: 'want', member, track, instance, on: true })
    for (const track of this._told.tracks) this._publishWant({ __r: 'want', member: null, track, instance, on: true })
    for (const member of this._told.members) this._publishWant({ __r: 'covers', member, instance, on: true })
    const now = Date.now()
    for (const [member, byTrack] of [...this._remote]) {
      this._expire(byTrack, now, (track) => this._pairsOf(member, track))
      if (byTrack.size === 0) this._remote.delete(member)
    }
    this._expire(this._remoteTracks, now, (track, reporter) => this._reachedBy(reporter, track))
    this._expire(this._remoteMembers, now, (member, reporter) => this._wantedBy(reporter, member))
  }

  /** Whether this instance has demand to keep alive or sweep: a pure observer still renews its leases. */
  isActive(): boolean {
    return !this._told.empty || this._remote.size > 0 || this._remoteTracks.size > 0 || this._remoteMembers.size > 0
  }

  /** Owner side: the member's tracks someone wants. */
  wanted(member: string): string[] {
    const tracks = new Set<string>()
    for (const byTrack of [this._told.pairs.get(member), this._remote.get(member)])
      for (const track of byTrack?.keys() ?? []) if (track !== null) tracks.add(track)
    if (this._receives(member, null)) for (const track of this._everyTrackOf(member)) tracks.add(track)
    if (this._told.members.has(member)) for (const track of this._told.tracks) tracks.add(track)
    for (const instance of this._remoteMembers.get(member)?.keys() ?? [])
      for (const [track, leases] of this._remoteTracks) if (leases.has(instance)) tracks.add(track)
    return [...tracks].filter((track) => this._wanted(member, track))
  }

  forgetMember(member: string): void {
    this._remote.delete(member)
    this._remoteMembers.delete(member)
    this._published.delete(member)
  }

  private _tellPair(member: string, track: string | null, on: boolean): void {
    this._publishWant({ __r: 'want', member, track, instance: this._instanceId, on })
    this._changing(this._pairsOf(member, track), () => {
      if (on) setEntry(this._told.pairs, member, track, true)
      else deleteEntry(this._told.pairs, member, track)
    })
  }

  private _tellTrack(track: string, on: boolean): void {
    this._publishWant({ __r: 'want', member: null, track, instance: this._instanceId, on })
    this._changing(this._reachedBy(null, track), () => toggle(this._told.tracks, track, on))
  }

  private _tellCovers(member: string, on: boolean): void {
    this._publishWant({ __r: 'covers', member, instance: this._instanceId, on })
    this._changing(this._wantedBy(null, member), () => toggle(this._told.members, member, on))
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
  private _lease<K>(leases: Map<K, Leases>, key: K, instance: string, on: boolean, pairs: () => Pairs): void {
    const held = leases.get(key)
    if (on && held?.has(instance)) {
      held.set(instance, Date.now() + ROOM_DEMAND_TTL_MS)
      return
    }
    if (!on && !held?.has(instance)) return
    this._changing(pairs(), () => {
      if (on) leases.set(key, (held ?? new Map()).set(instance, Date.now() + ROOM_DEMAND_TTL_MS))
      else if (held!.delete(instance) && held!.size === 0) leases.delete(key)
    })
  }

  private _expire<K>(leases: Map<K, Leases>, now: number, pairs: (key: K, instance: string) => Pairs): void {
    for (const [key, held] of [...leases])
      for (const [instance, expiresAt] of [...held])
        if (expiresAt <= now) this._lease(leases, key, instance, false, () => pairs(key, instance))
  }

  /** The owned member's tracks a pair names: for every track, each one it published and the default one. */
  private _pairsOf(member: string, track: string | null): Pairs {
    return {
      members: this._ownsMember(member) ? [member] : [],
      tracks: track === null ? this._everyTrackOf(member) : [track],
    }
  }

  /** `track` of each owned member `instance` (this one when `null`) receives it from. */
  private _reachedBy(instance: string | null, track: string): Pairs {
    const members =
      instance === null
        ? [...this._told.members].filter((member) => this._ownsMember(member))
        : [...this._remoteMembers].filter(([, leases]) => leases.has(instance)).map(([member]) => member)
    return { members, tracks: [track] }
  }

  /** Each track `instance` (this one when `null`) wants of every member, of the owned `member`. */
  private _wantedBy(instance: string | null, member: string): Pairs {
    if (!this._ownsMember(member)) return { members: [], tracks: [] }
    const tracks =
      instance === null
        ? [...this._told.tracks]
        : [...this._remoteTracks].filter(([, leases]) => leases.has(instance)).map(([track]) => track)
    return { members: [member], tracks }
  }

  private _everyTrackOf(member: string): string[] {
    return [DEFAULT_TRACK, ...(this._published.get(member) ?? [])]
  }

  /** Owner side: whether some instance receives the member's track. */
  private _wanted(member: string, track: string): boolean {
    const published = track === DEFAULT_TRACK || this._published.get(member)?.has(track) === true
    return this._receives(member, track) || (published && this._receives(member, null)) || this._reaches(member, track)
  }

  private _receives(member: string, track: string | null): boolean {
    return hasEntry(this._told.pairs, member, track) || hasEntry(this._remote, member, track)
  }

  /** Whether some instance that wants `track` of every member receives it from the member. */
  private _reaches(member: string, track: string): boolean {
    if (this._told.members.has(member) && this._told.tracks.has(track)) return true
    const leases = this._remoteTracks.get(track)
    const reporters = this._remoteMembers.get(member)
    if (leases === undefined || reporters === undefined) return false
    for (const instance of reporters.keys()) if (leases.has(instance)) return true
    return false
  }
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
function* entriesOf<V>(tracks: Tracks<V>): Generator<[string, string | null]> {
  for (const [member, byTrack] of [...tracks]) for (const track of [...byTrack.keys()]) yield [member, track]
}
