export { RoomDemand }
export type { WantGossip }

/** A demand-gossip event on the room's control lane — node-to-node only, never relayed to clients. */
type WantGossip = { member: string; track: string; node: string; on: boolean }

const DEMAND_SEP = '\u0000'
function demandKey(member: string, track: string): string {
  return member + DEMAND_SEP + track
}

/**
 * Cross-node aggregation of binary-track demand (`onDemand`). Each instance gossips its local
 * 0↔>0 demand transitions per (member, track) on the control lane; a member's owning instance ORs
 * every reporting instance with its own local contribution into a single `wanted` boolean and pushes
 * it — only on change. It's a boolean, not a subscriber count, on purpose: an instance reports "any"
 * (0↔>0), so the owner can only know whether *someone somewhere* wants the track, never how many. The
 * useful fact — `wanted: false` means pause the encoder, `true` means resume — is exactly what it can
 * know. Deliberately decoupled from `ServerRoom`: it reaches the room through three callbacks
 * (gossip-out, ownership test, deliver), so the whole cross-node reasoning lives here behind a narrow
 * seam and is unit-testable on its own.
 */
class RoomDemand {
  /** Tags this instance's gossip so a member's owner can dedupe demand reports across instances. */
  private readonly _instanceId = crypto.randomUUID()
  /** Composite key → [member, track] this instance currently has local demand for. */
  private _localDemand = new Map<string, [string, string]>()
  /** Owner-side: composite key → the OTHER instance ids reporting demand. */
  private readonly _remoteDemand = new Map<string, Set<string>>()
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

  /** A demand gossip from another instance/node. Recorded regardless of ownership (ownership can
   *  arrive later); only a member's owning instance pushes the resulting `wanted` to it. */
  applyWant(event: WantGossip): void {
    if (event.node === this._instanceId) return // our own gossip echoed back
    const k = demandKey(event.member, event.track)
    if (event.on) {
      let set = this._remoteDemand.get(k)
      if (!set) this._remoteDemand.set(k, (set = new Set()))
      set.add(event.node)
    } else {
      const set = this._remoteDemand.get(k)
      set?.delete(event.node)
      if (set && set.size === 0) this._remoteDemand.delete(k)
    }
    if (this._ownsMember(event.member)) this._recompute(event.member, event.track)
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
