export {
  HOLD_VIEWS,
  REMOVE_VIEW,
  RENEW_LEASE,
  CLAIM_LEASE,
  RENEW_CLAIM,
  REMOVE_LEASE,
  RoomLease,
  leaseHosting,
  runHosted,
  holdViews,
  removeView,
  renewLease,
  reapLapsedLeases,
  reapLeases,
}
export type { LeaseState }

// A server's lease in one room incarnation, on the store's clock: the members it joined and the pages' views it holds
// name it, its server renews it, and once it lapses one other server claims it and removes them. The lease is a cell
// that lapses `ROOM_LEASE_TTL_MS` after its last write, and the claim one that lapses `ROOM_CLOSE_LEASE_MS` after its
// last write: the store keeps both deadlines on its own clock, and its reads and commits judge them.

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { CellCounter, CellMutation, OperationOptions, OperationTx, RoomOperation } from '../../backend/contract.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { ROOM_CLOSE_LEASE_MS, ROOM_LEASE_TTL_MS } from '../constants.js'
import { afterHoldChange, holdDelta } from '../lifecycle/hold.js'
import type { HoldConfig, RoomIncarnation } from '../protocol.js'
import {
  LEASE_CELL_PREFIX,
  leaseCellKey,
  leaseClaimCellKey,
  leaseOfCellKey,
  leaseMemberCellPrefix,
  leaseViewCellPrefix,
  memberIdOfLeaseMemberKey,
  viewCellKey,
} from './cells.js'
import { readPrefix, readPrefixAt, type CellsAt } from './cellStore.js'
import { eachInFlight, failingAfter } from './inFlight.js'
import { evictMember } from './members.js'
import { runOperation } from './operations.js'
assertIsNotBrowser()

const claimEncoder = new TextEncoder()
const claimDecoder = new TextDecoder()

/** The lease's cell, written again: it lapses `ROOM_LEASE_TTL_MS` from now. */
function leaseMutation(id: string): CellMutation {
  return { key: leaseCellKey(id), bytes: new Uint8Array(), lapsesInMs: ROOM_LEASE_TTL_MS }
}

/** `cleaner`'s claim on lease `id`, a lease on the reap's work as long as a close's, which the reap renews while it
 *  runs. */
function claimMutation(id: string, cleaner: string): CellMutation {
  return { key: leaseClaimCellKey(id), bytes: claimEncoder.encode(cleaner), lapsesInMs: ROOM_CLOSE_LEASE_MS }
}

/** Whether the claim on lease `id` names `cleaner`. */
function claimedBy(tx: OperationTx, id: string, cleaner: string): boolean {
  const raw = tx.cell(leaseClaimCellKey(id))
  return raw !== undefined && claimDecoder.decode(raw) === cleaner
}

/** One per replica. Its id changes once its cell is lost: claimed, or removed after a claim. */
class RoomLease {
  private _id = crypto.randomUUID()
  /** Whether a write hosting something under `_id` committed, so its cell was written. */
  private _created = false

  get id(): string {
    return this._id
  }

  get created(): boolean {
    return this._created
  }

  hosted(id: string): void {
    if (id === this._id) this._created = true
  }

  /** The lease `id` was found lost; what it hosted is its cleaner's. */
  lost(id: string): void {
    if (id !== this._id) return
    this._id = crypto.randomUUID()
    this._created = false
  }
}

/** A lease as a write hosting something under it saw it: its id, and whether a write created its cell. */
type LeaseState = { id: string; created: boolean }

/** What a write hosting something under `lease` adds, given its plan's read of the lease's cell: nothing while the cell
 *  is live, the lease written again while it lapsed unclaimed or wasn't created yet, `null` once the lease is lost: a
 *  claim took its cell. A lease not known as created may still have been, by a write whose reply was lost, so its
 *  claim is read with its cell. */
function leaseHosting({ id, created }: LeaseState, tx: OperationTx): CellMutation[] | null {
  const key = leaseCellKey(id)
  const claimed = !created && tx.cell(leaseClaimCellKey(id)) !== undefined
  if (tx.cell(key) === undefined) return created || claimed ? null : [leaseMutation(id)]
  return tx.lapsed(key) ? [leaseMutation(id)] : []
}

/** Runs `operation`, which hosts something under the lease it takes as `lease`. A lease found lost is replaced, and the
 *  operation runs again under the replacement, whose cell it writes. */
async function runHosted<Args, Value>(
  store: RoomStore,
  room: RoomIncarnation,
  lease: RoomLease,
  operation: RoomOperation<Args & { lease: LeaseState }, Value | 'lease-lost'>,
  args: Args,
  options?: OperationOptions,
): Promise<{ value: Value; counters: Map<string, CellCounter> }> {
  for (;;) {
    const id = lease.id
    const hosted = { ...args, lease: { id, created: lease.created } }
    const { value, counters } = await runOperation(store, room, operation, hosted, options)
    if (value !== 'lease-lost') {
      lease.hosted(id)
      return { value, counters }
    }
    lease.lost(id)
  }
}

type HoldViewsArgs = { lease: LeaseState; ids: string[]; hold: HoldConfig }

/** Writes pages' views under a lease; each holds the room. Writing views already held changes nothing. It reads the
 *  views before the lease, so Redis reads them all in one round trip. */
const HOLD_VIEWS: RoomOperation<HoldViewsArgs, { keys: string[]; added: number } | 'lease-lost'> = {
  name: 'holdViews',
  plan(tx, { lease, ids, hold }) {
    const keys = ids.map((id) => viewCellKey(lease.id, id))
    const added = keys.filter((key) => tx.cell(key) === undefined)
    const leaseWrites = leaseHosting(lease, tx)
    if (leaseWrites === null) return 'lease-lost'
    for (const mutation of leaseWrites) tx.write(mutation)
    for (const key of added) tx.write({ key, bytes: new Uint8Array() })
    for (const count of holdDelta(hold, added.length).counts) tx.count(count)
    return { keys, added: added.length }
  },
}

/** Removes a view; false once it was gone. */
const REMOVE_VIEW: RoomOperation<{ key: string; hold: HoldConfig | null }, boolean> = {
  name: 'removeView',
  plan(tx, { key, hold }) {
    if (tx.cell(key) === undefined) return false
    tx.write({ key, bytes: null })
    for (const count of holdDelta(hold, -1).counts) tx.count(count)
    return true
  },
}

/** Renews a lease; false once it was lost. */
const RENEW_LEASE: RoomOperation<{ id: string }, boolean> = {
  name: 'renewLease',
  plan(tx, { id }) {
    if (tx.cell(leaseCellKey(id)) === undefined) return false
    tx.write(leaseMutation(id))
    return true
  },
}

type LeaseCleanArgs = { lease: string; cleaner: string }

/** Moves lease `lease`, lapsed, to `cleaner`'s claim, or gives `cleaner` a claim that lapsed; false once neither had. */
const CLAIM_LEASE: RoomOperation<LeaseCleanArgs, boolean> = {
  name: 'claimLease',
  plan(tx, { lease, cleaner }) {
    const key = leaseCellKey(lease)
    const claim = leaseClaimCellKey(lease)
    const leased = tx.cell(key) !== undefined
    if (!(leased ? tx.lapsed(key) : tx.cell(claim) !== undefined && tx.lapsed(claim))) return false
    if (leased) tx.write({ key, bytes: null })
    tx.write(claimMutation(lease, cleaner))
    return true
  },
}

/** Writes `cleaner`'s claim again; false once it no longer names `cleaner`. */
const RENEW_CLAIM: RoomOperation<LeaseCleanArgs, boolean> = {
  name: 'renewClaim',
  plan(tx, { lease, cleaner }) {
    if (!claimedBy(tx, lease, cleaner)) return false
    tx.write(claimMutation(lease, cleaner))
    return true
  },
}

/** Removes the claim on lease `lease` while it names `cleaner`. */
const REMOVE_LEASE: RoomOperation<LeaseCleanArgs, void> = {
  name: 'removeLease',
  plan(tx, { lease, cleaner }) {
    if (claimedBy(tx, lease, cleaner)) tx.write({ key: leaseClaimCellKey(lease), bytes: null })
  },
}

/** Writes pages' views under the lease in one write, and returns their cells' keys; a room without hold timeouts keeps
 *  none. */
async function holdViews(store: RoomStore, room: RoomIncarnation, lease: RoomLease, ids: string[]): Promise<string[]> {
  const { hold } = room
  if (hold === null) return []
  const { value, counters } = await runHosted(store, room, lease, HOLD_VIEWS, { ids, hold })
  if (value.added > 0) afterHoldChange(room, holdDelta(hold, value.added).after(counters))
  return value.keys
}

/** Removes a view by the key `holdViews` returned. */
async function removeView(store: RoomStore, room: RoomIncarnation, key: string): Promise<void> {
  const { value: removed, counters } = await runOperation(store, room, REMOVE_VIEW, { key, hold: room.hold })
  if (removed) afterHoldChange(room, holdDelta(room.hold, -1).after(counters))
}

/** Renews the lease once a write created its cell; false once the lease was lost. */
async function renewLease(store: RoomStore, room: RoomIncarnation, lease: RoomLease): Promise<boolean> {
  if (!lease.created) return true
  const id = lease.id
  const { value: renewed } = await runOperation(store, room, RENEW_LEASE, { id })
  if (!renewed) lease.lost(id)
  return renewed
}

/** Claims each lapsed lease and removes what it hosted: its members, each leaving `'disconnected'`, and its pages'
 *  views; then the lease. A claim lapses `ROOM_CLOSE_LEASE_MS` after it was last written, so a claimant that stops or
 *  fails leaves the rest to the next within that. */
async function reapLapsedLeases(store: RoomStore, room: RoomIncarnation): Promise<void> {
  await reapLeases(store, room, await readPrefixAt(store, room.id, room.inc, LEASE_CELL_PREFIX))
}

/** False once none of the leases and claims read had lapsed. */
async function reapLeases(store: RoomStore, room: RoomIncarnation, { lapsed }: CellsAt): Promise<boolean> {
  const leases = [...lapsed].map((key) => leaseOfCellKey(key).lease)
  const cleaner = crypto.randomUUID()
  await eachInFlight(leases, (lease) => reapLease(store, room, lease, cleaner))
  return leases.length > 0
}

async function reapLease(store: RoomStore, room: RoomIncarnation, lease: string, cleaner: string): Promise<void> {
  const claim = new LeaseClaim(store, room, lease, cleaner)
  const { value: claimed } = await runOperation(store, room, CLAIM_LEASE, { lease, cleaner })
  if (claimed) await cleanLease(store, room, lease, claim)
}

/** A reap's claim on a lapsed lease. Before each removal the reap starts, once half the claim's length passed since it
 *  was last written, by this server's clock, the claim is written again; once that answers that another reap holds it,
 *  the reap starts no more removals. */
class LeaseClaim {
  private readonly _store: RoomStore
  private readonly _room: RoomIncarnation
  private readonly _args: LeaseCleanArgs
  /** Taken before the write is sent, so the claim lapses no sooner than `ROOM_CLOSE_LEASE_MS` after it. */
  private _writtenAt = performance.now()
  /** The renewal under way, or the last one's answer once it was false or failed. */
  private _renewal: Promise<boolean> | null = null

  constructor(store: RoomStore, room: RoomIncarnation, lease: string, cleaner: string) {
    this._store = store
    this._room = room
    this._args = { lease, cleaner }
  }

  get cleaner(): string {
    return this._args.cleaner
  }

  /** Whether the reap still holds the claim, written again first once half its length passed. */
  async held(): Promise<boolean> {
    if (this._renewal === null && performance.now() - this._writtenAt < ROOM_CLOSE_LEASE_MS / 2) return true
    this._renewal ??= this.renew()
    return await this._renewal
  }

  private async renew(): Promise<boolean> {
    const writtenAt = performance.now()
    const { value: renewed } = await runOperation(this._store, this._room, RENEW_CLAIM, this._args)
    if (renewed) {
      this._writtenAt = writtenAt
      this._renewal = null
    }
    return renewed
  }
}

async function evictLeaseMembers(
  store: RoomStore,
  room: RoomIncarnation,
  lease: string,
  claim: LeaseClaim,
): Promise<void> {
  const marks = await readPrefix(store, room.id, room.inc, leaseMemberCellPrefix(lease))
  await eachInFlight(marks.keys(), async (key) => {
    if (await claim.held()) await evictMember(store, room, memberIdOfLeaseMemberKey(key), { type: 'disconnected' })
  })
}

async function removeLeaseViews(
  store: RoomStore,
  room: RoomIncarnation,
  lease: string,
  claim: LeaseClaim,
): Promise<void> {
  const views = await readPrefix(store, room.id, room.inc, leaseViewCellPrefix(lease))
  await eachInFlight(views.keys(), async (key) => {
    if (await claim.held()) await removeView(store, room, key)
  })
}

async function cleanLease(store: RoomStore, room: RoomIncarnation, lease: string, claim: LeaseClaim): Promise<void> {
  // A failed eviction is told at once, and the store refuses the removals the views would still hand over; a failed
  // removal is told once the evictions, which wait for each leave's handoff, settled.
  const evicting = evictLeaseMembers(store, room, lease, claim)
  await Promise.all([evicting, failingAfter(evicting, removeLeaseViews(store, room, lease, claim))])
  if (await claim.held()) await runOperation(store, room, REMOVE_LEASE, { lease, cleaner: claim.cleaner })
}
