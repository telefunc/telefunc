export { RoomLease, mutateHosted, holdViews, removeView, renewLease, finishLeaseCleanups, reapLapsedLeases, reapLeases }

// A server's lease in one room incarnation, on the store's clock: the members it joined and the pages' views it holds
// name it, its server renews it, and once it lapses one other server claims it and removes them.

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import type { CellMutation } from '../../backend/contract.js'
import { ROOM_LEASE_TTL_MS } from '../constants.js'
import { decodeRoomRecord, decodeRoomText } from '../lanes/lanes.js'
import { afterHoldChange, holdDelta } from '../lifecycle/hold.js'
import type { RoomIncarnation } from '../protocol.js'
import {
  CLEANUP_CELL_PREFIX,
  LEASE_CELL_PREFIX,
  leaseCellKey,
  leaseMemberCellKey,
  leaseMemberCellPrefix,
  leaseViewCellPrefix,
  memberIdOfCleanupKey,
  memberIdOfLeaseMemberKey,
  viewCellKey,
} from './cells.js'
import {
  eachInFlight,
  mutateCells,
  readPrefix,
  readPrefixAt,
  type CellCommit,
  type CellPlan,
  type CellsAt,
} from './cellStore.js'
import { completeCleanup, evictMember, type PendingMemberCleanup } from './members.js'
assertIsNotBrowser()

const leaseTextEncoder = new TextEncoder()

/** Lapses at `until`, authority time; `cleaner` names the server that claimed it to remove what it hosted. */
type LeaseRecord = { until: number; cleaner?: string }

function decodeLeaseRecord(raw: Uint8Array): LeaseRecord {
  return JSON.parse(decodeRoomText(raw)) as LeaseRecord
}

/** A lease from `now`, the authority time its exchange read. */
function leaseRecordBytes(now: number, cleaner?: string): Uint8Array {
  const record: LeaseRecord = { until: now + ROOM_LEASE_TTL_MS, cleaner }
  return leaseTextEncoder.encode(JSON.stringify(record))
}

/** One per replica. Its id changes once its cell is lost: claimed, or removed after a claim. */
class RoomLease {
  private _id = crypto.randomUUID()
  /** Whether an exchange hosting something under `_id` committed, so its cell was written. */
  private _created = false
  /** The exchange writing the cell, which the writes that would host something await. */
  private creating: Promise<void> | null = null

  get id(): string {
    return this._id
  }

  get created(): boolean {
    return this._created
  }

  /** What an exchange hosting something under `id` adds, given the cells it read with the lease's: nothing while the
   *  cell is live, a new lease while it is absent or lapsed unclaimed, `null` once the lease is lost. */
  hosting(id: string, cells: ReadonlyMap<string, Uint8Array>, now: number): CellMutation[] | null {
    if (id !== this._id) return null
    const key = leaseCellKey(id)
    const raw = cells.get(key)
    if (raw === undefined) return this._created ? null : [{ key, bytes: leaseRecordBytes(now) }]
    const record = decodeLeaseRecord(raw)
    if (record.cleaner !== undefined) return null
    return record.until < now ? [{ key, bytes: leaseRecordBytes(now) }] : []
  }

  /** Runs `write`, the exchange writing the cell, unless one already runs: the writes waiting to host something await
   *  the same one. */
  createOnce(write: () => Promise<void>): Promise<void> {
    this.creating ??= write().finally(() => {
      this.creating = null
    })
    return this.creating
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

/** An exchange hosting something under a lease found it lost. */
const LEASE_LOST: unique symbol = Symbol.for('telefunc.RoomLeaseLost')

/** Commits what `writesOf` plans under the lease in one exchange with its cell, which it only expects while live: a
 *  lease found lost is replaced, and the write planned again under the new one. */
async function mutateHosted<T>(
  room: RoomIncarnation,
  lease: RoomLease,
  writesOf: (id: string) => { keys: string[]; plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T> },
): Promise<CellCommit<T>> {
  for (;;) {
    if (!lease.created) await createLease(room, lease)
    const id = lease.id
    const { keys, plan } = writesOf(id)
    const committed = await mutateCells<T | typeof LEASE_LOST>(
      room.id,
      room.inc,
      [leaseCellKey(id), ...keys],
      (cells, now) => {
        const leaseWrites = lease.hosting(id, cells, now)
        if (leaseWrites === null) return { value: LEASE_LOST, mutations: [] }
        const planned = plan(cells)
        return leaseWrites.length === 0 ? planned : { ...planned, mutations: [...leaseWrites, ...planned.mutations] }
      },
    )
    if (committed.value !== LEASE_LOST) {
      lease.hosted(id)
      return committed as CellCommit<T>
    }
    lease.lost(id)
  }
}

/** Writes the lease's cell alone, once however many writes wait to host something under it, so they don't conflict
 *  on it. */
function createLease(room: RoomIncarnation, lease: RoomLease): Promise<void> {
  const id = lease.id
  const key = leaseCellKey(id)
  return lease.createOnce(async () => {
    const { value: hosted } = await mutateCells(room.id, room.inc, [key], (cells, now) => {
      const leaseWrites = lease.hosting(id, cells, now)
      return { value: leaseWrites !== null, mutations: leaseWrites ?? [] }
    })
    if (hosted) lease.hosted(id)
    else lease.lost(id)
  })
}

/** Writes pages' views under the lease in one write, and returns their cells' keys; each holds the room, and a room
 *  without hold timeouts keeps none. Writing views already held changes nothing. */
async function holdViews(room: RoomIncarnation, lease: RoomLease, ids: string[]): Promise<string[]> {
  const { hold } = room
  if (hold === null) return []
  const { value, counters } = await mutateHosted(room, lease, (hostId) => {
    const keys = ids.map((id) => viewCellKey(hostId, id))
    const plan = (cells: ReadonlyMap<string, Uint8Array>) => {
      const added = keys.filter((key) => !cells.has(key))
      const views = added.map((key) => ({ key, bytes: new Uint8Array() }))
      const delta = holdDelta(hold, added.length)
      return { value: { keys, added: added.length, delta }, mutations: views, counts: delta.counts }
    }
    return { keys, plan }
  })
  if (value.added > 0) afterHoldChange(room, value.delta.after(counters))
  return value.keys
}

/** Removes a view by the key `holdViews` returned. */
async function removeView(room: RoomIncarnation, key: string): Promise<void> {
  const hold = holdDelta(room.hold, -1)
  const { value: removed, counters } = await mutateCells(room.id, room.inc, [key], (cells) => {
    if (!cells.has(key)) return { value: false, mutations: [] }
    return { value: true, mutations: [{ key, bytes: null }], counts: hold.counts }
  })
  if (removed) afterHoldChange(room, hold.after(counters))
}

/** Renews the lease once a write created its cell; false once the lease was lost. */
async function renewLease(room: RoomIncarnation, lease: RoomLease): Promise<boolean> {
  if (!lease.created) return true
  const id = lease.id
  const key = leaseCellKey(id)
  const { value: renewed } = await mutateCells(room.id, room.inc, [key], (cells, now) => {
    const raw = cells.get(key)
    if (raw === undefined || decodeLeaseRecord(raw).cleaner !== undefined) return { value: false, mutations: [] }
    return { value: true, mutations: [{ key, bytes: leaseRecordBytes(now) }] }
  })
  if (!renewed) lease.lost(id)
  return renewed
}

/** Finishes the cleanups of the lease's members that were pending at the last call too: their evicting server, this
 *  one or another, stopped before finishing them. Returns the ones pending now, for the next call. */
async function finishLeaseCleanups(
  room: RoomIncarnation,
  lease: RoomLease,
  pendingBefore: ReadonlySet<string>,
): Promise<Set<string>> {
  const pending = new Set<string>()
  if (!lease.created) return pending
  const cleanups = await readPrefix(room.id, room.inc, CLEANUP_CELL_PREFIX)
  await eachInFlight(cleanups, async ([key, raw]) => {
    if (decodeRoomRecord<PendingMemberCleanup>(raw).session !== lease.id) return
    if (pendingBefore.has(key)) await completeCleanup(room.id, room.inc, memberIdOfCleanupKey(key), raw)
    else pending.add(key)
  })
  return pending
}

/** Claims each lapsed lease and removes what it hosted: its members, each leaving `'disconnected'`, their cleanups
 *  its server left unfinished, and its pages' views; then the lease. A claim lapses as a lease does, so a claimant
 *  that stops leaves the rest to the next. */
async function reapLapsedLeases(room: RoomIncarnation): Promise<void> {
  await reapLeases(room, await readPrefixAt(room.id, room.inc, LEASE_CELL_PREFIX))
}

/** False once none of the leases read had lapsed. */
async function reapLeases(room: RoomIncarnation, { cells, now }: CellsAt): Promise<boolean> {
  const cleaner = crypto.randomUUID()
  let lapsed = false
  for (const [key, raw] of cells) {
    if (decodeLeaseRecord(raw).until >= now) continue
    lapsed = true
    const { value: claimed } = await mutateCells(room.id, room.inc, [key], (current, at) => {
      const latest = current.get(key)
      if (latest === undefined || decodeLeaseRecord(latest).until >= at) return { value: false, mutations: [] }
      return { value: true, mutations: [{ key, bytes: leaseRecordBytes(at, cleaner) }] }
    })
    if (claimed) await cleanLease(room, key.slice(LEASE_CELL_PREFIX.length), cleaner)
  }
  return lapsed
}

async function cleanLease(room: RoomIncarnation, lease: string, cleaner: string): Promise<void> {
  const marks = await readPrefix(room.id, room.inc, leaseMemberCellPrefix(lease))
  await eachInFlight(marks.keys(), (key) => finishLeaseMember(room, lease, memberIdOfLeaseMemberKey(key)))
  const views = await readPrefix(room.id, room.inc, leaseViewCellPrefix(lease))
  await eachInFlight(views.keys(), (key) => removeView(room, key))
  const key = leaseCellKey(lease)
  await mutateCells(room.id, room.inc, [key], (cells) => {
    const raw = cells.get(key)
    const ours = raw !== undefined && decodeLeaseRecord(raw).cleaner === cleaner
    return { value: undefined, mutations: ours ? [{ key, bytes: null }] : [] }
  })
}

/** A member of a lapsed lease leaves, or finishes leaving. */
async function finishLeaseMember(room: RoomIncarnation, lease: string, id: string): Promise<void> {
  if (await evictMember(room, id, { type: 'disconnected' })) return
  const mark = leaseMemberCellKey(lease, id)
  await mutateCells(room.id, room.inc, [mark], (current) => ({
    value: undefined,
    mutations: current.has(mark) ? [{ key: mark, bytes: null }] : [],
  }))
}
