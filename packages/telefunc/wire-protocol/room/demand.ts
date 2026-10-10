export { RoomDemand }

import { DEFAULT_TRACK } from './binary.js'
import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { RoomCtrlEnvelope } from './protocol.js'

type WantGossip = Omit<Extract<RoomCtrlEnvelope, { __r: 'want' }>, '__r'>
/** Per member, per track; a `null` track is every track the member publishes. */
type Tracks<V, Track = string | null> = Map<string, Map<Track, V>>

/** Aggregates binary-track demand across instances: each gossips its local 0↔>0 transitions, and a member's owner pushes one `wanted` boolean. */
class RoomDemand {
  private readonly _instanceId = crypto.randomUUID()
  private readonly _local: Tracks<true> = new Map()
  /** Owner side: each reporting instance's lease. A reporter re-gossips every heartbeat, so a crashed one lapses. */
  private readonly _remote: Tracks<Map<string, number>> = new Map()
  /** Owner side: what was last pushed, so only changes are pushed. */
  private readonly _pushed: Tracks<true, string> = new Map()
  /** Owner side: the named tracks each member published on; a want for every track covers them and the default one. */
  private readonly _published = new Map<string, Set<string>>()

  constructor(
    private readonly _publishWant: (event: WantGossip) => void,
    private readonly _ownsMember: (id: string) => boolean,
    private readonly _deliver: (member: string, track: string, wanted: boolean) => void,
  ) {}

  /** Whether this instance receives the pair: each change is gossiped once. */
  setLocal(member: string, track: string | null, on: boolean): void {
    if (on === hasTrack(this._local, member, track)) return
    if (on) setTrack(this._local, member, track, true)
    else deleteTrack(this._local, member, track)
    this._transition(member, track, on)
  }

  /** The named and default tracks of the member this instance receives. */
  localTracks(member: string): string[] {
    return [...(this._local.get(member)?.keys() ?? [])].filter((track) => track !== null)
  }

  /** Owner side: the member publishes on `track`, which its every-track demand covers from now on. */
  published(member: string, track: string): void {
    if (track === DEFAULT_TRACK) return
    let tracks = this._published.get(member)
    if (tracks?.has(track)) return
    if (!tracks) this._published.set(member, (tracks = new Set()))
    tracks.add(track)
    this._recompute(member, track)
  }

  /** Only a member's owner aggregates its demand; ownership is fixed from before the member exists anywhere. */
  applyWant({ member, track, instance, on }: WantGossip): void {
    if (instance === this._instanceId || !this._ownsMember(member)) return
    if (on) {
      const leases = this._remote.get(member)?.get(track) ?? new Map<string, number>()
      leases.set(instance, Date.now() + ROOM_DEMAND_TTL_MS)
      setTrack(this._remote, member, track, leases)
    } else {
      const leases = this._remote.get(member)?.get(track)
      leases?.delete(instance)
      if (leases?.size === 0) deleteTrack(this._remote, member, track)
    }
    this._recomputePair(member, track)
  }

  heartbeat(): void {
    for (const [member, track] of pairsOf(this._local))
      this._publishWant({ member, track, instance: this._instanceId, on: true })
    const now = Date.now()
    for (const [member, track, leases] of pairsOf(this._remote)) {
      const before = leases.size
      for (const [instance, expiresAt] of leases) if (expiresAt <= now) leases.delete(instance)
      if (leases.size === before) continue
      if (leases.size === 0) deleteTrack(this._remote, member, track)
      this._recomputePair(member, track)
    }
  }

  /** Whether this instance has demand to keep alive or sweep: a pure observer still renews its leases. */
  isActive(): boolean {
    return this._local.size > 0 || this._remote.size > 0
  }

  /** Owner side: the member's tracks it last pushed as wanted. */
  wanted(member: string): string[] {
    return [...(this._pushed.get(member)?.keys() ?? [])]
  }

  forgetMember(member: string): void {
    this._remote.delete(member)
    this._pushed.delete(member)
    this._published.delete(member)
  }

  private _transition(member: string, track: string | null, on: boolean): void {
    this._publishWant({ member, track, instance: this._instanceId, on })
    if (this._ownsMember(member)) this._recomputePair(member, track)
  }

  /** A want for every track changes each track the member publishes. */
  private _recomputePair(member: string, track: string | null): void {
    if (track !== null) return this._recompute(member, track)
    for (const published of [DEFAULT_TRACK, ...(this._published.get(member) ?? [])]) this._recompute(member, published)
  }

  private _recompute(member: string, track: string): void {
    const published = track === DEFAULT_TRACK || this._published.get(member)?.has(track) === true
    const wanted = this._wants(member, track) || (published && this._wants(member, null))
    if (wanted === hasTrack(this._pushed, member, track)) return
    if (wanted) setTrack(this._pushed, member, track, true)
    else deleteTrack(this._pushed, member, track)
    this._deliver(member, track, wanted)
  }

  private _wants(member: string, track: string | null): boolean {
    return hasTrack(this._remote, member, track) || hasTrack(this._local, member, track)
  }
}

function hasTrack<V, Track>(tracks: Tracks<V, Track>, member: string, track: Track): boolean {
  return tracks.get(member)?.has(track) === true
}
function setTrack<V, Track>(tracks: Tracks<V, Track>, member: string, track: Track, value: V): void {
  let byTrack = tracks.get(member)
  if (!byTrack) tracks.set(member, (byTrack = new Map()))
  byTrack.set(track, value)
}
function deleteTrack<V, Track>(tracks: Tracks<V, Track>, member: string, track: Track): void {
  const byTrack = tracks.get(member)
  byTrack?.delete(track)
  if (byTrack?.size === 0) tracks.delete(member)
}
function* pairsOf<V>(tracks: Tracks<V>): Generator<[string, string | null, V]> {
  for (const [member, byTrack] of [...tracks]) for (const [track, value] of [...byTrack]) yield [member, track, value]
}
