export {
  HOLD_VIEWS,
  REMOVE_VIEW,
  RENEW_LEASE,
  CLAIM_LEASE,
  RENEW_CLAIM,
  REMOVE_LEASE,
  RoomLease,
  leaseCanHost,
  leaseCellBytes,
  decodeLeaseRecord,
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
import type { CellCounter, CellMutation, OperationOptions, RoomOperation } from '../../backend/contract.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { ROOM_CLOSE_LEASE_MS, ROOM_LEASE_TTL_MS } from '../constants.js'
import { assert } from '../../utils/assert.js'
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
import { readPrefix, readPrefixAt, type CellsAt } from './cellStore.js'
import { eachInFlight, failingAfter } from './inFlight.js'
import { evictMember } from './members.js'
import { runOperation } from './operations.js'
assertIsNotBrowser()

const leaseTextEncoder = new TextEncoder()

/** Lapses at `until`, authority time; `cleaner` names the server that claimed it to remove what it hosted. */
type LeaseRecord = { until: number; cleaner?: string }

/** A lease's cell: its record as JSON, `{"until":<digits>}` or `{"until":<digits>,"cleaner":"<id>"}`, which
 *  `decodeLeaseRecord()` reads by its bytes. */
function leaseCellBytes(record: LeaseRecord): Uint8Array {
  return leaseTextEncoder.encode(JSON.stringify(record))
}

const UNTIL_BYTES = leaseTextEncoder.encode('{"until":')
const CLEANER_BYTES = leaseTextEncoder.encode(',"cleaner":"')
const DIGIT_0 = 0x30
const DIGIT_9 = 0x39
const QUOTE = 0x22
const BACKSLASH = 0x5c
const CLOSING_BRACE = 0x7d
const leaseTextDecoder = new TextDecoder()

/** The record of a lease cell, read as `leaseCellBytes()` writes it: a few byte reads, with no text decoded but a
 *  cleaner's id. A cell in any other form is a bug, named by `at`: the cell's own key, or the key of the record it was
 *  read beside. */
function decodeLeaseRecord(at: string, raw: Uint8Array): LeaseRecord {
  let byte = bytesAt(raw, UNTIL_BYTES, 0)
  assertLeaseForm(byte !== -1, at, raw)
  const digits = byte
  let until = 0
  while (isDigit(raw[byte])) {
    until = until * 10 + ((raw[byte] ?? DIGIT_0) - DIGIT_0)
    byte++
  }
  const length = byte - digits
  assertLeaseForm(length > 0 && (length === 1 || raw[digits] !== DIGIT_0) && Number.isSafeInteger(until), at, raw)
  if (raw[byte] === CLOSING_BRACE && byte + 1 === raw.length) return { until }
  const cleaner = bytesAt(raw, CLEANER_BYTES, byte)
  assertLeaseForm(cleaner !== -1, at, raw)
  let end = cleaner
  while (raw[end] !== undefined && raw[end] !== QUOTE && raw[end] !== BACKSLASH) end++
  const closed = end > cleaner && raw[end] === QUOTE && raw[end + 1] === CLOSING_BRACE && end + 2 === raw.length
  assertLeaseForm(closed, at, raw)
  return { until, cleaner: leaseTextDecoder.decode(raw.subarray(cleaner, end)) }
}

/** A bug unless `form` holds, its message naming where the cell was read and its text, built only then. */
function assertLeaseForm(form: boolean, at: string, raw: Uint8Array): asserts form {
  if (!form)
    assert(false, `the lease cell read at ${at} isn't one leaseCellBytes() writes: ${leaseTextDecoder.decode(raw)}`)
}

function isDigit(byte: number | undefined): boolean {
  return byte !== undefined && byte >= DIGIT_0 && byte <= DIGIT_9
}

/** The index past `expected` in `raw` at `at`, or -1 where `raw` doesn't hold it there. */
function bytesAt(raw: Uint8Array, expected: Uint8Array, at: number): number {
  if (raw.length < at + expected.length) return -1
  for (let i = 0; i < expected.length; i++) if (raw[at + i] !== expected[i]) return -1
  return at + expected.length
}

/** Whether the lease cell `raw`, read at `at` (as for `decodeLeaseRecord()`), can host at `now`: no reap claimed it,
 *  and it hasn't lapsed. */
function leaseCanHost(at: string, raw: Uint8Array, now: number): boolean {
  const record = decodeLeaseRecord(at, raw)
  return record.cleaner === undefined && record.until >= now
}

/** A lease from `now`, the authority time its write read. */
function leaseRecordBytes(now: number): Uint8Array {
  return leaseCellBytes({ until: now + ROOM_LEASE_TTL_MS })
}

/** `cleaner`'s claim from `now`: a lease on the reap's work, as long as a close's, which the reap renews while it runs. */
function claimRecordBytes(now: number, cleaner: string): Uint8Array {
  return leaseCellBytes({ until: now + ROOM_CLOSE_LEASE_MS, cleaner })
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
  const record = decodeLeaseRecord(key, raw)
  if (record.cleaner !== undefined) return null
  return record.until < now ? [{ key, bytes: leaseRecordBytes(now) }] : []
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
    if (raw === undefined || decodeLeaseRecord(key, raw).cleaner !== undefined) return false
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
    if (raw === undefined || decodeLeaseRecord(key, raw).until >= tx.now) return false
    tx.write({ key, bytes: claimRecordBytes(tx.now, cleaner) })
    return true
  },
}

/** Writes `cleaner`'s claim again; false once it no longer names `cleaner`. */
const RENEW_CLAIM: RoomOperation<LeaseCleanArgs, boolean> = {
  name: 'renewClaim',
  plan(tx, { key, cleaner }) {
    const raw = tx.cell(key)
    if (raw === undefined || decodeLeaseRecord(key, raw).cleaner !== cleaner) return false
    tx.write({ key, bytes: claimRecordBytes(tx.now, cleaner) })
    return true
  },
}

/** Removes a lease `cleaner` still names. */
const REMOVE_LEASE: RoomOperation<LeaseCleanArgs, void> = {
  name: 'removeLease',
  plan(tx, { key, cleaner }) {
    const raw = tx.cell(key)
    if (raw !== undefined && decodeLeaseRecord(key, raw).cleaner === cleaner) tx.write({ key, bytes: null })
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

/** False once none of the leases read had lapsed. */
async function reapLeases(store: RoomStore, room: RoomIncarnation, { cells, now }: CellsAt): Promise<boolean> {
  const lapsed: string[] = []
  for (const [key, raw] of cells) if (decodeLeaseRecord(key, raw).until < now) lapsed.push(key)
  const cleaner = crypto.randomUUID()
  await eachInFlight(lapsed, (key) => reapLease(store, room, key, cleaner))
  return lapsed.length > 0
}

async function reapLease(store: RoomStore, room: RoomIncarnation, key: string, cleaner: string): Promise<void> {
  const claim = new LeaseClaim(store, room, key, cleaner)
  const { value: claimed } = await runOperation(store, room, CLAIM_LEASE, { key, cleaner })
  if (claimed) await cleanLease(store, room, key.slice(LEASE_CELL_PREFIX.length), claim)
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

  constructor(store: RoomStore, room: RoomIncarnation, key: string, cleaner: string) {
    this._store = store
    this._room = room
    this._args = { key, cleaner }
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
  if (await claim.held())
    await runOperation(store, room, REMOVE_LEASE, { key: leaseCellKey(lease), cleaner: claim.cleaner })
}
