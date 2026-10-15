/// <reference types="@cloudflare/workers-types" />
export {
  initSchema,
  directoryPut,
  directoryDelete,
  directoryList,
  directoryDue,
  readLiveHead,
  deleteLapsedHead,
  listOrphanGenerations,
  nextMaintenanceDeadline,
  compareExchangeHead,
  readCells,
  readCellPage,
  readOrder,
  compareExchangeCells,
  advanceOrder,
  dropLaneOrderRows,
  dropGenerationRows,
  keyRange,
}

export type { CellPage }

// Room-DO `transactionSync` makes head CX, cell batches, and order advance atomic under authority time.

import {
  CELL_PAGE_SIZE,
  DIRECTORY_PAGE_SIZE,
  type CellCounter,
  type CellsCx,
  type CxResult,
  type HeadCx,
  type HeadNext,
  type CellSelector,
  type CellsRead,
  type DirectoryPage,
} from '../../../../backend/room/contract.js'
import {
  countCell,
  encodeCellCounter,
  headCxMatches,
  isOpenIncarnation,
  materializeHead,
  nextOrderMark,
  type StoredHead,
} from '../../../../backend/room/semantics.js'
import type { OrderingInfo } from '../../../../ordering-frame.js'
import { binaryLaneKeyPrefix, encodeLaneKey } from '../../../../backend/room/lane-key.js'

type HeadCxOutcome = { head: StoredHead } | { conflict: true; current: StoredHead | null }

// Row shapes as SQLite hands them back (BLOB columns arrive as ArrayBuffer).
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
  // The DO is the room: `head` is one row or absent; `gen` is each installed incarnation's last cell version.
  sql.exec(`
    CREATE TABLE IF NOT EXISTS head
      (id INTEGER PRIMARY KEY CHECK (id = 1), rev TEXT NOT NULL, inc TEXT, state TEXT NOT NULL, config BLOB NOT NULL, lease_id TEXT, lease_until INTEGER, expires_at INTEGER);
    CREATE TABLE IF NOT EXISTS gen (inc TEXT PRIMARY KEY, revision INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS cell
      (inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, version INTEGER NOT NULL, PRIMARY KEY (inc, key));
    CREATE TABLE IF NOT EXISTS ord
      (inc TEXT NOT NULL, domain TEXT NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (inc, domain));
    CREATE TABLE IF NOT EXISTS rt_manifest
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, track TEXT, size INTEGER NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (inc, lane_key));
    CREATE INDEX IF NOT EXISTS rt_manifest_track ON rt_manifest(inc, track, lane_key);
    CREATE TABLE IF NOT EXISTS rt_chunk
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, i INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (inc, lane_key, i));
    CREATE TABLE IF NOT EXISTS route
      (room_id TEXT NOT NULL, inc TEXT NOT NULL, lane_key TEXT NOT NULL, session_do_id TEXT NOT NULL, lease_id TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (inc, lane_key, session_do_id));
    CREATE INDEX IF NOT EXISTS route_expires_at ON route(expires_at);
    CREATE TABLE IF NOT EXISTS directory (room_id TEXT PRIMARY KEY, inc_tag TEXT NOT NULL, check_at INTEGER);
    CREATE INDEX IF NOT EXISTS directory_check_at ON directory(check_at);
  `)
}

// A NULL check_at is never due.
function directoryPut(sql: SqlStorage, roomId: string, incTag: string, checkAt: number): void {
  sql.exec(
    'INSERT OR REPLACE INTO directory (room_id, inc_tag, check_at) VALUES (?, ?, ?)',
    roomId,
    incTag,
    checkAt === Infinity ? null : checkAt,
  )
}

function directoryDelete(sql: SqlStorage, roomId: string, incTag: string): void {
  // Deletes iff the stored tag matches (a stale tag is a no-op).
  sql.exec('DELETE FROM directory WHERE room_id = ? AND inc_tag = ?', roomId, incTag)
}

function directoryList(sql: SqlStorage, prefix: string, cursor?: string): DirectoryPage {
  const range = keyRange('room_id', prefix, cursor)
  const matching = sql
    .exec<{ room_id: string; inc_tag: string }>(
      `SELECT room_id, inc_tag FROM directory WHERE ${range.where} ORDER BY room_id LIMIT ?`,
      ...range.args,
      DIRECTORY_PAGE_SIZE + 1,
    )
    .toArray()
  const page = matching.slice(0, DIRECTORY_PAGE_SIZE)
  const entries = page.map((row) => ({ roomId: row.room_id, incTag: row.inc_tag }))
  const last = page[page.length - 1]
  const more = last !== undefined && matching.length > DIRECTORY_PAGE_SIZE
  return more ? { entries, cursor: last.room_id } : { entries }
}

// A check_at range, so a page seeks its index instead of scanning every room.
function directoryDue(sql: SqlStorage, now: number): DirectoryPage['entries'] {
  return sql
    .exec<{ room_id: string; inc_tag: string }>(
      'SELECT room_id, inc_tag FROM directory WHERE check_at <= ? ORDER BY check_at LIMIT ?',
      now,
      DIRECTORY_PAGE_SIZE,
    )
    .toArray()
    .map((row) => ({ roomId: row.room_id, incTag: row.inc_tag }))
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

// A lapsed tombstone reads absent; `now` is authority time.
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
function listOrphanGenerations(sql: SqlStorage, currentInc: string | null): string[] {
  return sql
    .exec<{ inc: string }>('SELECT inc FROM gen WHERE inc IS NOT ?', currentInc)
    .toArray()
    .map((row) => row.inc)
}

/** When the alarm's sweep next has work: `now` while an orphaned generation waits, else the first lapse of a head or a
 *  route, or null for none. */
function nextMaintenanceDeadline(sql: SqlStorage, now: number): number | null {
  if (listOrphanGenerations(sql, readLiveHead(sql, now)?.currentInc ?? null).length > 0) return now
  // Each table takes its own MIN, so the route table's seeks its expires_at index.
  return sql
    .exec<{ deadline: number | null }>(
      'SELECT MIN(deadline) AS deadline FROM (SELECT MIN(expires_at) AS deadline FROM head UNION ALL SELECT MIN(expires_at) FROM route)',
    )
    .toArray()[0]!.deadline
}

// Called inside `transactionSync`; a lost race returns the current head.
function compareExchangeHead(sql: SqlStorage, cx: HeadCx, next: HeadNext, now: number): HeadCxOutcome {
  const current = readLiveHead(sql, now)
  if (!headCxMatches(cx, current, now)) return { conflict: true, current }
  return { head: storeHead(sql, next, now) }
}

function storeHead(sql: SqlStorage, next: HeadNext, now: number): StoredHead {
  const head = materializeHead(next, now, crypto.randomUUID())
  sql.exec(
    'INSERT OR REPLACE INTO head (id, rev, inc, state, config, lease_id, lease_until, expires_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?)',
    head.rev,
    head.currentInc,
    head.state,
    head.config,
    head.closeLease?.id ?? null,
    head.closeLease?.until ?? null,
    head.expiresAt,
  )
  // A new incarnation's generation is registered inside the CX that names it.
  if (head.currentInc !== null) {
    sql.exec('INSERT OR IGNORE INTO gen (inc, revision) VALUES (?, 0)', head.currentInc)
  }
  return head
}

type CellRow = { key: string; bytes: ArrayBuffer; version: number }
function selectCellRows(sql: SqlStorage, inc: string, sel: CellSelector): CellRow[] {
  if ('keys' in sel)
    return sel.keys.flatMap((key) =>
      sql.exec<CellRow>('SELECT key, bytes, version FROM cell WHERE inc = ? AND key = ?', inc, key).toArray(),
    )
  const range = keyRange('key', sel.prefix)
  return sql
    .exec<CellRow>(`SELECT key, bytes, version FROM cell WHERE inc = ? AND ${range.where}`, inc, ...range.args)
    .toArray()
}

// Reads stay available while closing; staleInc means the head is absent or names another incarnation.
function readCells(sql: SqlStorage, inc: string, sel: CellSelector, now: number): CellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { staleInc: true }
  const rows = selectCellRows(sql, inc, sel)
  return {
    cells: new Map(rows.map((row) => [row.key, new Uint8Array(row.bytes)])),
    versions: new Map(rows.map((row) => [row.key, String(row.version)])),
    now,
  }
}

/** A prefix read's page, with the cursor its next page reads after while cells may remain. */
type CellPage =
  | { cells: Map<string, Uint8Array>; versions: Map<string, string>; now: number; cursor?: string }
  | { staleInc: true }

function readCellPage(sql: SqlStorage, inc: string, prefix: string, cursor: string | undefined, now: number): CellPage {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { staleInc: true }
  const range = keyRange('key', prefix, cursor)
  const rows = sql
    .exec<CellRow>(
      `SELECT key, bytes, version FROM cell WHERE inc = ? AND ${range.where} ORDER BY key LIMIT ?`,
      inc,
      ...range.args,
      CELL_PAGE_SIZE,
    )
    .toArray()
  const page: Exclude<CellPage, { staleInc: true }> = {
    cells: new Map(rows.map((row) => [row.key, new Uint8Array(row.bytes)])),
    versions: new Map(rows.map((row) => [row.key, String(row.version)])),
    now,
  }
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

// Cell writes are all-or-nothing while every expected cell is at its version, and require an open head.
function compareExchangeCells(sql: SqlStorage, inc: string, cx: CellsCx, now: number): CxResult {
  const head = readLiveHead(sql, now)
  if (!isOpenIncarnation(head, inc)) return 'stale-inc'
  if (cx.expect.some(({ key, version }) => cellVersion(sql, inc, key) !== version)) return 'conflict'
  sql.exec('UPDATE gen SET revision = revision + 1 WHERE inc = ?', inc)
  const version = readRevision(sql, inc)
  for (const mutation of cx.mutations) writeCell(sql, inc, mutation.key, mutation.bytes, version)
  const counters = new Map<string, CellCounter>()
  for (const count of cx.counts ?? []) {
    const current = selectCellRows(sql, inc, { keys: [count.key] })[0]
    const counted = countCell(current === undefined ? undefined : new Uint8Array(current.bytes), count, now)
    if (counted === null) continue
    if (counted.written) writeCell(sql, inc, count.key, encodeCellCounter(counted.counter), version)
    counters.set(count.key, counted.counter)
  }
  return { committed: counters }
}

function readOrder(sql: SqlStorage, inc: string, domain: string): OrderingInfo | null {
  const row = sql
    .exec<{ seq: number; ts: number }>('SELECT seq, ts FROM ord WHERE inc = ? AND domain = ?', inc, domain)
    .toArray()[0]
  return row === undefined ? null : { seq: row.seq, timestamp: row.ts }
}

// `seq` strictly increases for the lifetime of a domain instance.
function advanceOrder(sql: SqlStorage, inc: string, domain: string, now: number): OrderingInfo {
  const mark = nextOrderMark(readOrder(sql, inc, domain) ?? undefined, now)
  sql.exec(
    'INSERT OR REPLACE INTO ord (inc, domain, seq, ts) VALUES (?, ?, ?, ?)',
    inc,
    domain,
    mark.seq,
    mark.timestamp,
  )
  return mark
}

// A member's binary lanes are a primary-key range: their keys share its prefix.
function dropLaneOrderRows(sql: SqlStorage, inc: string, member: string): void {
  const range = keyRange('domain', binaryLaneKeyPrefix(member))
  sql.exec(
    `DELETE FROM ord WHERE inc = ? AND (domain = ? OR (${range.where}))`,
    inc,
    encodeLaneKey({ kind: 'inbox', member }),
    ...range.args,
  )
}

function dropGenerationRows(sql: SqlStorage, inc: string): void {
  for (const table of ['cell', 'ord', 'rt_manifest', 'rt_chunk', 'route', 'gen']) {
    sql.exec(`DELETE FROM ${table} WHERE inc = ?`, inc)
  }
}
