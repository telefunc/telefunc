export { RoomDemand }
export type { WantGossip }

import { ROOM_DEMAND_TTL_MS } from './constants.js'

/** A demand-gossip event on the room's control lane — node-to-node only, never relayed to clients. */
type WantGossip = { member: string; track: string; node: string; on: boolean }
const DEMAND_SEP = '\u0000'
function demandKey(member: string, track: string): string {
  return member + DEMAND_SEP + track
}
/** Cross-node aggregation of binary-track demand (`onDemand`). Each instance gossips its local 0↔>0 demand transitions per (member, track) on the control lane; a member's owning instance ORs every
 * reporting instance with its own local contribution into a single `wanted` boolean and pushes it — only on change. It's a boolean, not a subscriber count, on purpose: an instance reports "any"
 * (0↔>0), so the owner can only know whether *someone somewhere* wants the track, never how many. The useful fact — `wanted: false` means pause the encoder, `true` means resume — is exactly what it
 * can know. Deliberately decoupled from `ServerRoom`: it reaches the room through three callbacks (gossip-out, ownership test, deliver), so the whole cross-node reasoning lives here behind a narrow
 * seam and is unit-testable on its own.
 */
class RoomDemand {
  /** Tags this instance's gossip so a member's owner can dedupe demand reports across instances. */
  private readonly _instanceId = crypto.randomUUID()
  /** Composite key → [member, track] this instance currently has local demand for. */
  private _localDemand = new Map<string, [string, string]>()
  /** Owner-side: composite key → the OTHER instance ids reporting demand, each with a lease deadline.
   *  A reporter re-gossips every heartbeat to renew its lease; one that crashes lets its lease lapse,
   *  so `heartbeat()` sweeps it (a Set would strand a crashed reporter's demand forever). */
  private readonly _remoteDemand = new Map<string, Map<string, number>>()
  /** Owner-side: composite keys currently pushed to the member as `wanted` (change detection). */
  private readonly _pushedWanted = new Set<string>()
  constructor(
    private readonly _publishWant: (event: WantGossip) => void,
    private readonly _ownsMember: (id: string) => boolean,
    private readonly _deliver: (member: string, track: string, wanted: boolean) => void,
  ) {}
  /** Diff this instance's local demand against `localPairs` (the (member, track) streams it now
   *  wants), gossiping each 0↔>0 transition and recomputing demand for any owned member. Pass an
   *  empty list to drop all local demand (e.g. the room closed). */
  sync(localPairs: ReadonlyArray<readonly [string, string]>): void {
    const prev = this._localDemand
    const next = new Map<string, [string, string]>()
    for (const [member, track] of localPairs) next.set(demandKey(member, track), [member, track])
    this._localDemand = next
    for (const [k, [member, track]] of next) if (!prev.has(k)) this._transition(member, track, true)
    for (const [k, [member, track]] of prev) if (!next.has(k)) this._transition(member, track, false)
  }
  /** A demand gossip from another instance/node. Recorded regardless of ownership (ownership can arrive later); only a member's owning instance pushes the resulting `wanted` to it. */
  applyWant(event: WantGossip): void {
    if (event.node === this._instanceId) return // our own gossip echoed back
    const k = demandKey(event.member, event.track)
    if (event.on) {
      let leases = this._remoteDemand.get(k)
      if (!leases) this._remoteDemand.set(k, (leases = new Map()))
      leases.set(event.node, Date.now() + ROOM_DEMAND_TTL_MS) // (re)new the reporter's lease
    } else {
      const leases = this._remoteDemand.get(k)
      leases?.delete(event.node)
      if (leases && leases.size === 0) this._remoteDemand.delete(k)
    }
    if (this._ownsMember(event.member)) this._recompute(event.member, event.track)
  }
  /** Called on the room heartbeat. Renews this instance's demand lease on every owner by re-gossiping
   *  its live local demand, then sweeps remote demand whose lease lapsed — a reporter that crashed
   *  without sending its 0-transition — recomputing any owned member it thereby stops wanting. */
  heartbeat(): void {
    for (const [, [member, track]] of this._localDemand)
      this._publishWant({ member, track, node: this._instanceId, on: true })
    const now = Date.now()
    for (const [k, leases] of this._remoteDemand) {
      let expired = false
      for (const [node, expiresAt] of leases) {
        if (expiresAt <= now) {
          leases.delete(node)
          expired = true
        }
      }
      if (leases.size === 0) this._remoteDemand.delete(k)
      if (expired) {
        const sep = k.indexOf(DEMAND_SEP)
        const member = k.slice(0, sep)
        if (this._ownsMember(member)) this._recompute(member, k.slice(sep + 1))
      }
    }
  }
  /** Whether this instance has demand to keep alive or sweep — gates the room heartbeat so a pure observer (local demand, no owned members) still refreshes its lease and isn't wrongly reaped. */
  isActive(): boolean {
    return this._localDemand.size > 0 || this._remoteDemand.size > 0
  }
  /** Drop owner-side state for every track of a departed member. */
  forgetMember(member: string): void {
    const prefix = member + DEMAND_SEP
    for (const key of this._remoteDemand.keys()) if (key.startsWith(prefix)) this._remoteDemand.delete(key)
    for (const key of this._pushedWanted) if (key.startsWith(prefix)) this._pushedWanted.delete(key)
  }
  private _transition(member: string, track: string, on: boolean): void {
    this._publishWant({ member, track, node: this._instanceId, on })
    if (this._ownsMember(member)) this._recompute(member, track)
  }
  private _recompute(member: string, track: string): void {
    const k = demandKey(member, track)
    const wanted = (this._remoteDemand.get(k)?.size ?? 0) > 0 || this._localDemand.has(k)
    if (wanted === this._pushedWanted.has(k)) return // no change → no spurious push
    if (wanted) this._pushedWanted.add(k)
    else this._pushedWanted.delete(k)
    this._deliver(member, track, wanted)
  }
}
