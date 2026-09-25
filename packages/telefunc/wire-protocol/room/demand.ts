export { RoomDemand }

import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { RoomCtrlEnvelope } from './protocol.js'

type WantGossip = Omit<Extract<RoomCtrlEnvelope, { __r: 'want' }>, '__r'>
type Tracks<V> = Map<string, Map<string, V>>

/** Aggregates binary-track demand across instances: each gossips its local 0↔>0 transitions, and a member's owner pushes one `wanted` boolean. */
class RoomDemand {
  private readonly _instanceId = crypto.randomUUID()
  private _local: Tracks<true> = new Map()
  /** Owner side: each reporting instance's lease. A reporter re-gossips every heartbeat, so a crashed one lapses. */
  private readonly _remote: Tracks<Map<string, number>> = new Map()
  /** Owner side: what was last pushed, so only changes are pushed. */
  private readonly _pushed: Tracks<true> = new Map()

  constructor(
    private readonly _publishWant: (event: WantGossip) => void,
    private readonly _ownsMember: (id: string) => boolean,
    private readonly _deliver: (member: string, track: string, wanted: boolean) => void,
  ) {}

  sync(localPairs: ReadonlyArray<readonly [string, string]>): void {
    const prev = this._local
    const next: Tracks<true> = new Map()
    for (const [member, track] of localPairs) setTrack(next, member, track, true)
    this._local = next
    for (const [member, track] of pairsOf(next))
      if (!hasTrack(prev, member, track)) this._transition(member, track, true)
    for (const [member, track] of pairsOf(prev))
      if (!hasTrack(next, member, track)) this._transition(member, track, false)
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
    this._recompute(member, track)
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
      this._recompute(member, track)
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
  }

  private _transition(member: string, track: string, on: boolean): void {
    this._publishWant({ member, track, instance: this._instanceId, on })
    if (this._ownsMember(member)) this._recompute(member, track)
  }

  private _recompute(member: string, track: string): void {
    const wanted = hasTrack(this._remote, member, track) || hasTrack(this._local, member, track)
    if (wanted === hasTrack(this._pushed, member, track)) return
    if (wanted) setTrack(this._pushed, member, track, true)
    else deleteTrack(this._pushed, member, track)
    this._deliver(member, track, wanted)
  }
}

function hasTrack<V>(tracks: Tracks<V>, member: string, track: string): boolean {
  return tracks.get(member)?.has(track) === true
}
function setTrack<V>(tracks: Tracks<V>, member: string, track: string, value: V): void {
  let byTrack = tracks.get(member)
  if (!byTrack) tracks.set(member, (byTrack = new Map()))
  byTrack.set(track, value)
}
function deleteTrack<V>(tracks: Tracks<V>, member: string, track: string): void {
  const byTrack = tracks.get(member)
  byTrack?.delete(track)
  if (byTrack?.size === 0) tracks.delete(member)
}
function* pairsOf<V>(tracks: Tracks<V>): Generator<[string, string, V]> {
  for (const [member, byTrack] of [...tracks]) for (const [track, value] of [...byTrack]) yield [member, track, value]
}
