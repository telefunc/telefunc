export {
  HOLD_VIEWS,
  REMOVE_VIEW,
  RENEW_LEASE,
  CLAIM_LEASE,
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
// name it, its server renews it, and once it lapses one other server claims it and removes them.

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { CellCounter, CellMutation, RoomOperation } from '../../backend/contract.js'
import { ROOM_LEASE_TTL_MS } from '../constants.js'
import { decodeRoomText } from '../lanes/lanes.js'
import { afterHoldChange, holdDelta } from '../lifecycle/hold.js'
import type { HoldConfig, RoomIncarnation } from '../protocol.js'
import {
  LEASE_CELL_PREFIX,
  leaseCellKey,
  leaseMemberCellPrefix,
  leaseViewCellPrefix,
  memberIdOfLeaseMemberKey,
  viewCellKey,
} from './cells.js'
import { eachInFlight, readPrefix, readPrefixAt, type CellsAt } from './cellStore.js'
import { evictMember } from './members.js'
import { runOperation } from './operations.js'
assertIsNotBrowser()

const leaseTextEncoder = new TextEncoder()

/** Lapses at `until`, authority time; `cleaner` names the server that claimed it to remove what it hosted. */
type LeaseRecord = { until: number; cleaner?: string }

function decodeLeaseRecord(raw: Uint8Array): LeaseRecord {
  return JSON.parse(decodeRoomText(raw)) as LeaseRecord
}

/** A lease from `now`, the authority time its write read. */
function leaseRecordBytes(now: number, cleaner?: string): Uint8Array {
  const record: LeaseRecord = { until: now + ROOM_LEASE_TTL_MS, cleaner }
  return leaseTextEncoder.encode(JSON.stringify(record))
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

/** What a write hosting something under `lease` adds, given the lease's cell as it read it: nothing while the cell is
 *  live, a new lease while it is absent or lapsed unclaimed, `null` once the lease is lost. */
function leaseHosting({ id, created }: LeaseState, raw: Uint8Array | undefined, now: number): CellMutation[] | null {
  const key = leaseCellKey(id)
  if (raw === undefined) return created ? null : [{ key, bytes: leaseRecordBytes(now) }]
  const record = decodeLeaseRecord(raw)
  if (record.cleaner !== undefined) return null
  return record.until < now ? [{ key, bytes: leaseRecordBytes(now) }] : []
}

/** Runs `operation`, which hosts something under the lease it takes as `lease`. A lease found lost is replaced, and the
 *  operation runs again under the replacement, whose cell it writes. */
async function runHosted<Args, Value>(
  room: RoomIncarnation,
  lease: RoomLease,
  operation: RoomOperation<Args & { lease: LeaseState }, Value | 'lease-lost'>,
  args: Args,
): Promise<{ value: Value; counters: Map<string, CellCounter> }> {
  for (;;) {
    const id = lease.id
    const { value, counters } = await runOperation(room, operation, { ...args, lease: { id, created: lease.created } })
    if (value !== 'lease-lost') {
      lease.hosted(id)
      return { value: value as Value, counters }
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
    const leaseWrites = leaseHosting(lease, tx.cell(leaseCellKey(lease.id)), tx.now)
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
    const key = leaseCellKey(id)
    const raw = tx.cell(key)
    if (raw === undefined || decodeLeaseRecord(raw).cleaner !== undefined) return false
    tx.write({ key, bytes: leaseRecordBytes(tx.now) })
    return true
  },
}

type LeaseCleanArgs = { key: string; cleaner: string }

/** Names `cleaner` on a lapsed lease; false once it wasn't lapsed. */
const CLAIM_LEASE: RoomOperation<LeaseCleanArgs, boolean> = {
  name: 'claimLease',
  plan(tx, { key, cleaner }) {
    const raw = tx.cell(key)
    if (raw === undefined || decodeLeaseRecord(raw).until >= tx.now) return false
    tx.write({ key, bytes: leaseRecordBytes(tx.now, cleaner) })
    return true
  },
}

/** Removes a lease `cleaner` still names. */
const REMOVE_LEASE: RoomOperation<LeaseCleanArgs, void> = {
  name: 'removeLease',
  plan(tx, { key, cleaner }) {
    const raw = tx.cell(key)
    if (raw !== undefined && decodeLeaseRecord(raw).cleaner === cleaner) tx.write({ key, bytes: null })
  },
}

/** Writes pages' views under the lease in one write, and returns their cells' keys; a room without hold timeouts keeps
 *  none. */
async function holdViews(room: RoomIncarnation, lease: RoomLease, ids: string[]): Promise<string[]> {
  const { hold } = room
  if (hold === null) return []
  const { value, counters } = await runHosted(room, lease, HOLD_VIEWS, { ids, hold })
  if (value.added > 0) afterHoldChange(room, holdDelta(hold, value.added).after(counters))
  return value.keys
}

/** Removes a view by the key `holdViews` returned. */
async function removeView(room: RoomIncarnation, key: string): Promise<void> {
  const { value: removed, counters } = await runOperation(room, REMOVE_VIEW, { key, hold: room.hold })
  if (removed) afterHoldChange(room, holdDelta(room.hold, -1).after(counters))
}

/** Renews the lease once a write created its cell; false once the lease was lost. */
async function renewLease(room: RoomIncarnation, lease: RoomLease): Promise<boolean> {
  if (!lease.created) return true
  const id = lease.id
  const { value: renewed } = await runOperation(room, RENEW_LEASE, { id })
  if (!renewed) lease.lost(id)
  return renewed
}

/** Claims each lapsed lease and removes what it hosted: its members, each leaving `'disconnected'`, and its pages'
 *  views; then the lease. A claim lapses as a lease does, so a claimant that stops leaves the rest to the next. */
async function reapLapsedLeases(room: RoomIncarnation): Promise<void> {
  await reapLeases(room, await readPrefixAt(room.id, room.inc, LEASE_CELL_PREFIX))
}

/** False once none of the leases read had lapsed. */
async function reapLeases(room: RoomIncarnation, { cells, now }: CellsAt): Promise<boolean> {
  const lapsed = Array.from(cells.keys()).filter((key) => decodeLeaseRecord(cells.get(key)!).until < now)
  const cleaner = crypto.randomUUID()
  await eachInFlight(lapsed, (key) => reapLease(room, key, cleaner))
  return lapsed.length > 0
}

async function reapLease(room: RoomIncarnation, key: string, cleaner: string): Promise<void> {
  const { value: claimed } = await runOperation(room, CLAIM_LEASE, { key, cleaner })
  if (claimed) await cleanLease(room, key.slice(LEASE_CELL_PREFIX.length), cleaner)
}

async function evictLeaseMembers(room: RoomIncarnation, lease: string): Promise<void> {
  const marks = await readPrefix(room.id, room.inc, leaseMemberCellPrefix(lease))
  await eachInFlight(marks.keys(), (key) => evictMember(room, memberIdOfLeaseMemberKey(key), { type: 'disconnected' }))
}

async function removeLeaseViews(room: RoomIncarnation, lease: string): Promise<void> {
  const views = await readPrefix(room.id, room.inc, leaseViewCellPrefix(lease))
  await eachInFlight(views.keys(), (key) => removeView(room, key))
}

async function cleanLease(room: RoomIncarnation, lease: string, cleaner: string): Promise<void> {
  await Promise.all([evictLeaseMembers(room, lease), removeLeaseViews(room, lease)])
  await runOperation(room, REMOVE_LEASE, { key: leaseCellKey(lease), cleaner })
}
