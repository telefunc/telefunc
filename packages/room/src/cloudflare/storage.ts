/// <reference types="@cloudflare/workers-types" />
export {
  initSchema,
  listWithHead,
  readListedInc,
  readUnsentListing,
  settleListing,
  resendListing,
  relistOver,
  readLiveHead,
  deleteLapsedHead,
  listOrphanIncarnations,
  hasIncarnation,
  nextMaintenanceDeadline,
  compareExchangeHead,
  readCells,
  readExpiringCells,
  writeExpiringCell,
  deleteExpiredCells,
  readCellPage,
  readOrder,
  compareExchangeCells,
  dropCellPrefix,
  advanceOrder,
  dropLaneOrderRows,
  dropIncarnationRows,
  keyRange,
  storeHandoff,
  listHandoffs,
  deleteHandoff,
}

export type { CellPage, Handoff, ListingPlace }

// Room-DO `transactionSync` makes head exchanges, cell batches, and order advance atomic under authority time.

import {
  CELL_PAGE_SIZE,
  type CellCounter,
  type CellsExchange,
  type CellsExchangeResult,
  type HeadExpectation,
  type HeadNext,
  type CellSelector,
  type CellsRead,
  type ExpiringCell,
  type ExpiringCellsRead,
  type StaleIncarnation,
} from '../backend/contract.js'
import { dropLeaseScopes, initLeaseSchema, nextLeaseSweep } from 'telefunc/__internal'
import { incarnationScopes } from './routes.js'
import {
  countCell,
  encodeCellCounter,
  headExpectationHolds,
  isOpenIncarnation,
  materializeHead,
  nextOrdering,
  type StoredHead,
} from '../backend/semantics.js'
import type { OrderingInfo } from 'telefunc/__internal'
import { binaryLaneKeyPrefix, encodeLaneKey } from '../backend/laneKey.js'
import { assert } from '../utils/assert.js'

/** A head exchange's result, with the stored head: the authority hands out its public view. */
type HeadExchangeOutcome = { kind: 'committed'; head: StoredHead } | { kind: 'conflict'; current: StoredHead | null }

/** The `head` row as SQLite hands it back: its BLOB column arrives as an ArrayBuffer. */
type HeadRow = {
  rev: string
  inc: string | null
  state: string
  config: ArrayBuffer
  lease_id: string | null
  lease_until: number | null
  expires_at: number | null
}

function initSchema(sql: SqlStorage): void {
  // The DO is the room: `head` is one row or absent; `gen` is each installed incarnation's last cell version. `listing`
  // is the room's listing, one row or none.
  sql.exec(`
    CREATE TABLE IF NOT EXISTS head
      (id INTEGER PRIMARY KEY CHECK (id = 1), rev TEXT NOT NULL, inc TEXT, state TEXT NOT NULL, config BLOB NOT NULL,
       lease_id TEXT, lease_until INTEGER, expires_at INTEGER);
    CREATE TABLE IF NOT EXISTS gen (inc TEXT PRIMARY KEY, revision INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS cell
      (inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, version INTEGER NOT NULL, PRIMARY KEY (inc, key));
    CREATE TABLE IF NOT EXISTS expiring_cell
      (inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (inc, key));
    CREATE INDEX IF NOT EXISTS expiring_cell_expires_at ON expiring_cell(expires_at);
    CREATE TABLE IF NOT EXISTS ord
      (inc TEXT NOT NULL, domain TEXT NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (inc, domain));
    CREATE TABLE IF NOT EXISTS rt_manifest
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, track TEXT, size INTEGER NOT NULL, seq INTEGER NOT NULL,
       ts INTEGER NOT NULL, owner TEXT, PRIMARY KEY (inc, lane_key));
    CREATE INDEX IF NOT EXISTS rt_manifest_track ON rt_manifest(inc, track, lane_key);
    CREATE TABLE IF NOT EXISTS rt_chunk
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, i INTEGER NOT NULL, bytes BLOB NOT NULL,
       PRIMARY KEY (inc, lane_key, i));
    CREATE TABLE IF NOT EXISTS handoff
      (inc TEXT NOT NULL, seq INTEGER NOT NULL, lane_key TEXT NOT NULL, ts INTEGER NOT NULL, payload BLOB NOT NULL,
       resume_at INTEGER NOT NULL, PRIMARY KEY (inc, lane_key, seq));
    CREATE TABLE IF NOT EXISTS listing
      (id INTEGER PRIMARY KEY CHECK (id = 1), room_id TEXT NOT NULL, inc_tag TEXT, bucket TEXT NOT NULL,
       version INTEGER NOT NULL, due_at INTEGER);
  `)
  initLeaseSchema(sql)
}

/** The room's listing, as its region's directory is to show it: the incarnation its head lists, or none. `version`
 *  counts its changes; `dueAt` is when the alarm sends it, null once the directory has this version. */
type Listing = { roomId: string; incTag: string | null; bucket: string; version: number; dueAt: number | null }

/** Where a room is listed: its id, and the bucket of the call that first lists it, whose region's directory lists it
 *  for good, as the authority itself never moves. */
type ListingPlace = { roomId: string; bucket: string }

type ListingRow = { room_id: string; inc_tag: string | null; bucket: string; version: number; due_at: number | null }

function readListingRow(sql: SqlStorage): Listing | null {
  const row = sql.exec<ListingRow>('SELECT * FROM listing WHERE id = 1').toArray()[0]
  if (row === undefined) return null
  return { roomId: row.room_id, incTag: row.inc_tag, bucket: row.bucket, version: row.version, dueAt: row.due_at }
}

/** Lists what `head` lists, an open or closing incarnation, or nothing: a change is a new version, due `now`. */
function listWithHead(sql: SqlStorage, head: StoredHead | null, now: number, place?: ListingPlace): void {
  const incTag = head !== null && head.state !== 'closed' ? head.currentInc : null
  const listing = readListingRow(sql)
  if ((listing?.incTag ?? null) === incTag) return
  const at = listing ?? place
  // A room never listed has nothing to unlist; one listed has its place.
  if (at === undefined) return
  sql.exec(
    'INSERT OR REPLACE INTO listing (id, room_id, inc_tag, bucket, version, due_at) VALUES (1, ?, ?, ?, ?, ?)',
    at.roomId,
    incTag,
    at.bucket,
    (listing?.version ?? 0) + 1,
    now,
  )
}

/** What the alarm lists once a head lapsed. */
function settleListing(sql: SqlStorage, now: number): void {
  listWithHead(sql, readLiveHead(sql, now), now)
}

function readListedInc(sql: SqlStorage): string | undefined {
  return readListingRow(sql)?.incTag ?? undefined
}

/** The listing while its directory may not have its version: the alarm sends it at `dueAt`, any send before. */
function readUnsentListing(sql: SqlStorage): Listing | null {
  const listing = readListingRow(sql)
  return listing !== null && listing.dueAt !== null ? listing : null
}

/** `version` is due again at `dueAt`, or never (null) once the directory has it; a newer version stays due. */
function resendListing(sql: SqlStorage, version: number, dueAt: number | null): void {
  sql.exec('UPDATE listing SET due_at = ? WHERE id = 1 AND version = ?', dueAt, version)
}

/** A new version of a listing the directory has, due `now`, unless it names `staleInc`: the directory was found
 *  listing that incarnation, as after a call delivered later than its successors. */
function relistOver(sql: SqlStorage, staleInc: string, now: number): void {
  sql.exec(
    'UPDATE listing SET version = version + 1, due_at = ? WHERE id = 1 AND due_at IS NULL AND inc_tag IS NOT ?',
    now,
    staleInc,
  )
}

/** The strings of `column` starting with `prefix`, past `cursor` if given, as a range, so a query seeks its index
 *  instead of scanning. */
function keyRange(column: string, prefix: string, cursor?: string): { where: string; args: string[] } {
  const range = { where: `${column} ${cursor === undefined ? '>=' : '>'} ?`, args: [cursor ?? prefix] }
  const end = prefixEnd(prefix)
  if (end !== null) {
    range.where += ` AND ${column} < ?`
    range.args.push(end)
  }
  return range
}

/** The least string above every string starting with `prefix` (TEXT compares by code point), or null for none. */
function prefixEnd(prefix: string): string | null {
  const codePoints = [...prefix]
  while (codePoints.length > 0) {
    const next = codePoints.pop()!.codePointAt(0)! + 1
    if (next <= 0x10ffff) return codePoints.join('') + String.fromCodePoint(next === 0xd800 ? 0xe000 : next)
  }
  return null
}

/** A lapsed tombstone reads absent; `now` is authority time. */
function readLiveHead(sql: SqlStorage, now: number): StoredHead | null {
  const row = sql.exec<HeadRow>('SELECT * FROM head WHERE id = 1').toArray()[0]
  if (row === undefined) return null
  if (row.expires_at !== null && row.expires_at <= now) return null
  const head: StoredHead = {
    rev: row.rev,
    currentInc: row.inc,
    state: row.state as StoredHead['state'],
    config: new Uint8Array(row.config),
    expiresAt: row.expires_at,
  }
  if (row.lease_id !== null && row.lease_until !== null) head.closeLease = { id: row.lease_id, until: row.lease_until }
  return head
}

/** A lapsed head is reclaimed here: this backend has no native head TTL. */
function deleteLapsedHead(sql: SqlStorage, now: number): void {
  sql.exec('DELETE FROM head WHERE id = 1 AND expires_at IS NOT NULL AND expires_at <= ?', now)
}

/** Installed incarnations other than `currentInc`. */
function listOrphanIncarnations(sql: SqlStorage, currentInc: string | null): string[] {
  return sql
    .exec<{ inc: string }>('SELECT inc FROM gen WHERE inc IS NOT ?', currentInc)
    .toArray()
    .map((row) => row.inc)
}

/** Whether `inc`'s incarnation is stored: `dropIncarnationRows` deletes it with its routes. */
function hasIncarnation(sql: SqlStorage, inc: string): boolean {
  return sql.exec('SELECT 1 FROM gen WHERE inc = ?', inc).toArray().length > 0
}

/** When the alarm's sweep next has work: `now` while an orphaned incarnation waits, else the first lapse of a head or
 *  an expiring cell, the listing's next send, or when a route's session DO may be dropped, or null for none. */
function nextMaintenanceDeadline(sql: SqlStorage, now: number): number | null {
  if (listOrphanIncarnations(sql, readLiveHead(sql, now)?.currentInc ?? null).length > 0) return now
  // Each table takes its own MIN, so the expiring_cell table's seeks its expires_at index.
  const deadline = sql
    .exec<{
      deadline: number | null
    }>(
      'SELECT MIN(deadline) AS deadline FROM (SELECT MIN(expires_at) AS deadline FROM head ' +
        'UNION ALL SELECT MIN(expires_at) FROM expiring_cell UNION ALL SELECT MIN(due_at) FROM listing ' +
        'UNION ALL SELECT MIN(resume_at) FROM handoff)',
    )
    .one().deadline
  const sweep = nextLeaseSweep(sql)
  return deadline === null || sweep === null ? (deadline ?? sweep) : Math.min(deadline, sweep)
}

/** Called inside `transactionSync`; a lost race returns the current head. */
function compareExchangeHead(
  sql: SqlStorage,
  expected: HeadExpectation,
  next: HeadNext,
  now: number,
): HeadExchangeOutcome {
  const current = readLiveHead(sql, now)
  if (!headExpectationHolds(expected, current, now)) return { kind: 'conflict', current }
  return { kind: 'committed', head: storeHead(sql, next, now) }
}

function storeHead(sql: SqlStorage, next: HeadNext, now: number): StoredHead {
  const head = materializeHead(next, now)
  sql.exec(
    'INSERT OR REPLACE INTO head (id, rev, inc, state, config, lease_id, lease_until, expires_at) ' +
      'VALUES (1, ?, ?, ?, ?, ?, ?, ?)',
    head.rev,
    head.currentInc,
    head.state,
    head.config,
    head.closeLease?.id ?? null,
    head.closeLease?.until ?? null,
    head.expiresAt,
  )
  // A new incarnation is registered inside the exchange that names it, and counted there.
  if (head.currentInc !== null) {
    sql.exec('INSERT OR IGNORE INTO gen (inc, revision) VALUES (?, 0)', head.currentInc)
    if (next.counts !== undefined) {
      const counted = compareExchangeCells(
        sql,
        head.currentInc,
        { expect: [], mutations: [], counts: next.counts },
        now,
      )
      assert(counted.kind === 'committed') // the head just stored names it
    }
  }
  return head
}

type CellRow = { key: string; bytes: ArrayBuffer; version: number }

/** A prefix read's page, with the cursor its next page reads after while cells may remain. */
type CellPage = (Extract<CellsRead, { kind: 'read' }> & { cursor?: string }) | StaleIncarnation

function cellPage(rows: CellRow[], now: number): Extract<CellPage, { kind: 'read' }> {
  return {
    kind: 'read',
    cells: new Map(rows.map((row) => [row.key, new Uint8Array(row.bytes)])),
    versions: new Map(rows.map((row) => [row.key, String(row.version)])),
    now,
  }
}

function selectCellRows(sql: SqlStorage, inc: string, sel: CellSelector): CellRow[] {
  if ('keys' in sel) {
    return sel.keys.flatMap((key) =>
      sql.exec<CellRow>('SELECT key, bytes, version FROM cell WHERE inc = ? AND key = ?', inc, key).toArray(),
    )
  }
  const range = keyRange('key', sel.prefix)
  return sql
    .exec<CellRow>(`SELECT key, bytes, version FROM cell WHERE inc = ? AND ${range.where}`, inc, ...range.args)
    .toArray()
}

/** Reads stay available while closing; a stale incarnation is one the head is absent for or doesn't name. */
function readCells(sql: SqlStorage, inc: string, sel: CellSelector, now: number): CellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { kind: 'stale-incarnation' }
  return cellPage(selectCellRows(sql, inc, sel), now)
}

/** The expiring cells of `keys` that haven't expired; the alarm deletes the others. */
function readExpiringCells(sql: SqlStorage, inc: string, keys: readonly string[], now: number): ExpiringCellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { kind: 'stale-incarnation' }
  const cells = new Map<string, Uint8Array>()
  for (const key of keys) {
    const row = sql
      .exec<{ bytes: ArrayBuffer }>(
        'SELECT bytes FROM expiring_cell WHERE inc = ? AND key = ? AND expires_at > ?',
        inc,
        key,
        now,
      )
      .toArray()[0]
    if (row !== undefined) cells.set(key, new Uint8Array(row.bytes))
  }
  return { kind: 'read', cells }
}

function writeExpiringCell(sql: SqlStorage, inc: string, { key, bytes, ttlMs }: ExpiringCell, now: number): void {
  sql.exec(
    'INSERT OR REPLACE INTO expiring_cell (inc, key, bytes, expires_at) VALUES (?, ?, ?, ?)',
    inc,
    key,
    bytes,
    now + ttlMs,
  )
}

function deleteExpiredCells(sql: SqlStorage, now: number): void {
  sql.exec('DELETE FROM expiring_cell WHERE expires_at <= ?', now)
}

function readCellPage(sql: SqlStorage, inc: string, prefix: string, cursor: string | undefined, now: number): CellPage {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { kind: 'stale-incarnation' }
  const range = keyRange('key', prefix, cursor)
  const rows = sql
    .exec<CellRow>(
      `SELECT key, bytes, version FROM cell WHERE inc = ? AND ${range.where} ORDER BY key LIMIT ?`,
      inc,
      ...range.args,
      CELL_PAGE_SIZE,
    )
    .toArray()
  const page = cellPage(rows, now)
  if (rows.length === CELL_PAGE_SIZE) page.cursor = rows[rows.length - 1]!.key
  return page
}

function readRevision(sql: SqlStorage, inc: string): number {
  const row = sql.exec<{ revision: number }>('SELECT revision FROM gen WHERE inc = ?', inc).toArray()[0]
  return row?.revision ?? 0
}

function cellVersion(sql: SqlStorage, inc: string, key: string): string | null {
  const row = sql.exec<{ version: number }>('SELECT version FROM cell WHERE inc = ? AND key = ?', inc, key).toArray()[0]
  return row === undefined ? null : String(row.version)
}

function writeCell(sql: SqlStorage, inc: string, key: string, bytes: Uint8Array | null, version: number): void {
  if (bytes === null) sql.exec('DELETE FROM cell WHERE inc = ? AND key = ?', inc, key)
  else sql.exec('INSERT OR REPLACE INTO cell (inc, key, bytes, version) VALUES (?, ?, ?, ?)', inc, key, bytes, version)
}

/** Deletes the incarnation's cells under `prefix`. */
function dropCellPrefix(sql: SqlStorage, inc: string, prefix: string): void {
  const range = keyRange('key', prefix)
  sql.exec(`DELETE FROM cell WHERE inc = ? AND ${range.where}`, inc, ...range.args)
}

/** Cell writes are all-or-nothing while every expected cell is at its version, and require an open head. */
function compareExchangeCells(sql: SqlStorage, inc: string, exchange: CellsExchange, now: number): CellsExchangeResult {
  const head = readLiveHead(sql, now)
  if (!isOpenIncarnation(head, inc)) return { kind: 'stale-incarnation' }
  if (exchange.expect.some(({ key, version }) => cellVersion(sql, inc, key) !== version)) return { kind: 'conflict' }
  sql.exec('UPDATE gen SET revision = revision + 1 WHERE inc = ?', inc)
  const version = readRevision(sql, inc)
  for (const mutation of exchange.mutations) writeCell(sql, inc, mutation.key, mutation.bytes, version)
  const counters = new Map<string, CellCounter>()
  for (const count of exchange.counts ?? []) {
    const current = selectCellRows(sql, inc, { keys: [count.key] })[0]
    const counted = countCell(current === undefined ? undefined : new Uint8Array(current.bytes), count, now)
    if (counted === null) continue
    if (counted.written) writeCell(sql, inc, count.key, encodeCellCounter(counted.counter), version)
    counters.set(count.key, counted.counter)
  }
  return { kind: 'committed', counters }
}

function readOrder(sql: SqlStorage, inc: string, domain: string): OrderingInfo | null {
  const row = sql
    .exec<{ seq: number; ts: number }>('SELECT seq, ts FROM ord WHERE inc = ? AND domain = ?', inc, domain)
    .toArray()[0]
  return row === undefined ? null : { seq: row.seq, timestamp: row.ts }
}

/** `seq` strictly increases for the lifetime of a domain instance. */
function advanceOrder(sql: SqlStorage, inc: string, domain: string, now: number): OrderingInfo {
  const mark = nextOrdering(readOrder(sql, inc, domain) ?? undefined, now)
  sql.exec(
    'INSERT OR REPLACE INTO ord (inc, domain, seq, ts) VALUES (?, ?, ?, ?)',
    inc,
    domain,
    mark.seq,
    mark.timestamp,
  )
  return mark
}

/** A member's binary lanes are a primary-key range: their keys share its prefix. */
function dropLaneOrderRows(sql: SqlStorage, inc: string, member: string): void {
  const range = keyRange('domain', binaryLaneKeyPrefix(member))
  sql.exec(
    `DELETE FROM ord WHERE inc = ? AND (domain = ? OR (${range.where}))`,
    inc,
    encodeLaneKey({ kind: 'inbox', member }),
    ...range.args,
  )
}

function dropIncarnationRows(sql: SqlStorage, inc: string): void {
  for (const table of ['cell', 'expiring_cell', 'ord', 'rt_manifest', 'rt_chunk', 'gen', 'handoff']) {
    sql.exec(`DELETE FROM ${table} WHERE inc = ?`, inc)
  }
  dropLeaseScopes(sql, incarnationScopes(inc))
}

/** A commit stored with its transaction until it was handed on, so a restart in between hands it on still. */
type Handoff = { inc: string; laneKey: string; seq: number; timestamp: number; payload: Uint8Array }

function storeHandoff(sql: SqlStorage, { inc, laneKey, seq, timestamp, payload }: Handoff, resumeAt: number): void {
  sql.exec(
    'INSERT INTO handoff (inc, seq, lane_key, ts, payload, resume_at) VALUES (?, ?, ?, ?, ?, ?)',
    inc,
    seq,
    laneKey,
    timestamp,
    payload,
    resumeAt,
  )
}

/** Every commit not yet handed on, in the order it was committed. */
function listHandoffs(sql: SqlStorage): Handoff[] {
  return sql
    .exec<{ inc: string; seq: number; lane_key: string; ts: number; payload: ArrayBuffer }>(
      'SELECT inc, seq, lane_key, ts, payload FROM handoff ORDER BY inc, lane_key, seq',
    )
    .toArray()
    .map(({ inc, seq, lane_key, ts, payload }) => ({
      inc,
      laneKey: lane_key,
      seq,
      timestamp: ts,
      payload: new Uint8Array(payload),
    }))
}

function deleteHandoff(sql: SqlStorage, { inc, laneKey, seq }: Pick<Handoff, 'inc' | 'laneKey' | 'seq'>): void {
  sql.exec('DELETE FROM handoff WHERE inc = ? AND lane_key = ? AND seq = ?', inc, laneKey, seq)
}
