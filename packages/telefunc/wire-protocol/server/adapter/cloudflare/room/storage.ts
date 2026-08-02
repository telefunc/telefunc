/// <reference types="@cloudflare/workers-types" />
// Room-DO `transactionSync` makes head CX, cell batches, and order advance atomic under authority time.

import type { CellMutation, CxResult, HeadCx, HeadNext, RoomHead } from '../../../../backend/room/contract.js'
import {
  assertHeadDeleteLegal,
  assertHeadNextWellFormed,
  assertHeadTransition,
} from '../../../../backend/room/head-transitions.js'

type HeadWriteNext = Extract<HeadNext, { head: unknown }>
export type StoredHead = RoomHead & { expiresAt: number | null }

type HeadCxOutcome =
  | { ok: true; head: StoredHead }
  | { ok: true; deleted: true }
  | { conflict: true; current: StoredHead | null }

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

export function toBytes(value: ArrayBuffer | Uint8Array): Uint8Array {
  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

function rowExists(sql: SqlStorage, query: string, ...bindings: Array<string | number>): boolean {
  return sql.exec(query, ...bindings).toArray().length === 1
}

export function initSchema(sql: SqlStorage): void {
  // The DO is the room: `head` is one row or absent; `gen` guards fresh incarnations and cell revisions.
  sql.exec(`
    CREATE TABLE IF NOT EXISTS head
      (id INTEGER PRIMARY KEY CHECK (id = 1), rev TEXT NOT NULL, inc TEXT, state TEXT NOT NULL, config BLOB NOT NULL, lease_id TEXT, lease_until INTEGER, expires_at INTEGER);
    CREATE TABLE IF NOT EXISTS gen (inc TEXT PRIMARY KEY, token TEXT NOT NULL, revision INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS cell
      (inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (inc, key));
    CREATE TABLE IF NOT EXISTS ord
      (inc TEXT NOT NULL, domain TEXT NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (inc, domain));
    CREATE TABLE IF NOT EXISTS rt_manifest
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, size INTEGER NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, lane_kind TEXT NOT NULL, lane_member TEXT, lane_track TEXT, PRIMARY KEY (inc, lane_key));
    CREATE TABLE IF NOT EXISTS rt_chunk
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, i INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (inc, lane_key, i));
    CREATE TABLE IF NOT EXISTS route
      (room_id TEXT NOT NULL, inc TEXT NOT NULL, lane_key TEXT NOT NULL, subscriber_do_id TEXT NOT NULL, lease_id TEXT NOT NULL, generation_token TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (inc, lane_key, subscriber_do_id));
    CREATE INDEX IF NOT EXISTS route_expires_at ON route(expires_at);
    CREATE TABLE IF NOT EXISTS directory (room_id TEXT PRIMARY KEY, inc_tag TEXT NOT NULL);
  `)
}

const DIRECTORY_PAGE_SIZE = 100

export function directoryPut(sql: SqlStorage, roomId: string, incTag: string): void {
  sql.exec('INSERT OR REPLACE INTO directory (room_id, inc_tag) VALUES (?, ?)', roomId, incTag)
}

export function directoryDelete(sql: SqlStorage, roomId: string, incTag: string): void {
  // Deletes iff the stored tag matches — a stale tag is a no-op.
  sql.exec('DELETE FROM directory WHERE room_id = ? AND inc_tag = ?', roomId, incTag)
}

export function directoryList(
  sql: SqlStorage,
  prefix: string,
  cursor?: string,
): { entries: { roomId: string; incTag: string }[]; cursor?: string } {
  const query =
    cursor === undefined
      ? 'SELECT room_id, inc_tag FROM directory WHERE substr(room_id, 1, length(?)) = ? ORDER BY room_id LIMIT ?'
      : 'SELECT room_id, inc_tag FROM directory WHERE substr(room_id, 1, length(?)) = ? AND room_id > ? ORDER BY room_id LIMIT ?'
  const matching =
    cursor === undefined
      ? sql.exec<{ room_id: string; inc_tag: string }>(query, prefix, prefix, DIRECTORY_PAGE_SIZE + 1).toArray()
      : sql.exec<{ room_id: string; inc_tag: string }>(query, prefix, prefix, cursor, DIRECTORY_PAGE_SIZE + 1).toArray()
  const page = matching.slice(0, DIRECTORY_PAGE_SIZE)
  const entries = page.map((row) => ({ roomId: row.room_id, incTag: row.inc_tag }))
  const last = page[page.length - 1]
  const more = last !== undefined && matching.length > DIRECTORY_PAGE_SIZE
  return more ? { entries, cursor: last.room_id } : { entries }
}

// A lapsed tombstone reads absent; `now` is authority time.
export function readLiveHead(sql: SqlStorage, now: number): StoredHead | null {
  const rows = sql.exec<HeadRow>('SELECT * FROM head WHERE id = 1').toArray()
  const row = rows[0]
  if (row === undefined) return null
  if (row.expires_at !== null && row.expires_at <= now) return null
  const head: StoredHead = {
    rev: row.rev,
    currentInc: row.inc,
    state: row.state as StoredHead['state'],
    config: toBytes(row.config),
    expiresAt: row.expires_at,
  }
  if (row.lease_id !== null && row.lease_until !== null) head.closeLease = { id: row.lease_id, until: row.lease_until }
  return head
}

export function hasGeneration(sql: SqlStorage, inc: string): boolean {
  return sql.exec('SELECT 1 FROM gen WHERE inc = ? LIMIT 1', inc).toArray().length > 0
}

// A generation token prevents stale work from authorizing a lease after an incarnation string is reused.
export function readGenerationToken(sql: SqlStorage, inc: string): string | null {
  return sql.exec<{ token: string }>('SELECT token FROM gen WHERE inc = ?', inc).toArray()[0]?.token ?? null
}

export function listGenerations(sql: SqlStorage): string[] {
  return sql
    .exec<{ inc: string }>('SELECT inc FROM gen')
    .toArray()
    .map((row) => row.inc)
}

function headCxMatches(sql: SqlStorage, cx: HeadCx, current: StoredHead | null, now: number): boolean {
  if (cx.expect === 'absent') return current === null
  if (current === null) return false
  const expect = cx.expect
  if ('closingLeaseExpired' in expect) {
    return rowExists(
      sql,
      "SELECT 1 FROM head WHERE id = 1 AND rev = ? AND state = 'closing' AND lease_until IS NOT NULL AND lease_until < ?",
      expect.rev,
      now,
    )
  }
  if ('closingLease' in expect) {
    return rowExists(
      sql,
      "SELECT 1 FROM head WHERE id = 1 AND rev = ? AND state = 'closing' AND lease_id = ?",
      expect.rev,
      expect.closingLease,
    )
  }
  return rowExists(sql, 'SELECT 1 FROM head WHERE id = 1 AND rev = ?', expect.rev)
}

// Called inside `transactionSync`; invalid transitions throw, while lost races return the current head.
export function compareExchangeHead(
  sql: SqlStorage,
  cx: HeadCx,
  next: HeadNext,
  now: number,
  mintRev: () => string,
): HeadCxOutcome {
  assertHeadNextWellFormed(next)
  const current = readLiveHead(sql, now)
  // Delete legality precedes comparison; other transitions validate the head that actually matched.
  assertHeadDeleteLegal(next, current)
  if (!headCxMatches(sql, cx, current, now)) return { conflict: true, current }
  if ('delete' in next) {
    sql.exec('DELETE FROM head WHERE id = 1')
    return { ok: true, deleted: true }
  }
  assertHeadTransition(cx, next as HeadWriteNext, current, (inc) => hasGeneration(sql, inc))
  return { ok: true, head: storeHead(sql, next as HeadWriteNext, now, mintRev) }
}

function storeHead(sql: SqlStorage, next: HeadWriteNext, now: number, mintRev: () => string): StoredHead {
  const rev = mintRev()
  const expiresAt = next.ttlMs === undefined ? null : now + next.ttlMs
  // The lease deadline is minted here, inside the CX, from authority time — never supplied by a caller.
  const lease = next.head.closeLease
  const leaseUntil = lease === undefined ? null : now + lease.durationMs
  const leaseId = lease === undefined ? null : lease.id
  sql.exec(
    'INSERT OR REPLACE INTO head (id, rev, inc, state, config, lease_id, lease_until, expires_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?)',
    rev,
    next.head.currentInc,
    next.head.state,
    next.head.config,
    leaseId,
    leaseUntil,
    expiresAt,
  )
  // Registering the generation inside this CX makes the fresh-inc guard deterministic.
  if (next.head.currentInc !== null) {
    sql.exec('INSERT OR IGNORE INTO gen (inc, token, revision) VALUES (?, ?, 0)', next.head.currentInc, rev)
  }
  const stored: StoredHead = {
    rev,
    currentInc: next.head.currentInc,
    state: next.head.state,
    config: next.head.config,
    expiresAt,
  }
  if (leaseId !== null && leaseUntil !== null) stored.closeLease = { id: leaseId, until: leaseUntil }
  return stored
}

type CellsRead = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }
type CellRow = { key: string; bytes: ArrayBuffer }
function selectCellRows(sql: SqlStorage, inc: string, sel: { keys: string[] } | { prefix: string }): CellRow[] {
  if ('keys' in sel)
    return sel.keys.flatMap((key) => {
      const row = sql
        .exec<Omit<CellRow, 'key'>>('SELECT bytes FROM cell WHERE inc = ? AND key = ?', inc, key)
        .toArray()[0]
      return row === undefined ? [] : [{ key, ...row }]
    })
  const query = 'SELECT key, bytes FROM cell WHERE inc = ? AND substr(key, 1, length(?)) = ?'
  return sql.exec<CellRow>(query, inc, sel.prefix, sel.prefix).toArray()
}

// Reads stay available while closing; staleInc means the head is absent or names another incarnation.
export function readCells(
  sql: SqlStorage,
  inc: string,
  sel: { keys: string[] } | { prefix: string },
  now: number,
): CellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { staleInc: true }
  const revision = String(readRevision(sql, inc))
  return { revision, cells: new Map(selectCellRows(sql, inc, sel).map((row) => [row.key, toBytes(row.bytes)])) }
}

function readRevision(sql: SqlStorage, inc: string): number {
  const row = sql.exec<{ revision: number }>('SELECT revision FROM gen WHERE inc = ?', inc).toArray()[0]
  return row?.revision ?? 0
}

// Cell writes are all-or-nothing under the read-set revision and require an open head.
export function compareExchangeCells(
  sql: SqlStorage,
  inc: string,
  revision: string,
  mutations: CellMutation[],
  now: number,
): CxResult {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc || head.state !== 'open') return 'stale-inc'
  if (String(readRevision(sql, inc)) !== revision) return 'conflict'
  for (const mutation of mutations) {
    if (mutation.set === undefined) {
      sql.exec('DELETE FROM cell WHERE inc = ? AND key = ?', inc, mutation.key)
    } else {
      sql.exec('INSERT OR REPLACE INTO cell (inc, key, bytes) VALUES (?, ?, ?)', inc, mutation.key, mutation.set.bytes)
    }
  }
  sql.exec('UPDATE gen SET revision = revision + 1 WHERE inc = ?', inc)
  return 'committed'
}

export type OrderMark = { seq: number; timestamp: number }

// seq strictly increases for the lifetime of a domain instance; timestamp is clamped independently.
export function advanceOrder(sql: SqlStorage, inc: string, domain: string, now: number): OrderMark {
  const row = sql
    .exec<{ seq: number; ts: number }>('SELECT seq, ts FROM ord WHERE inc = ? AND domain = ?', inc, domain)
    .toArray()[0]
  if (row?.seq === Number.MAX_SAFE_INTEGER) {
    throw new Error('commitLane: sequence exhausted for the ordering domain')
  }
  const mark: OrderMark = { seq: (row?.seq ?? 0) + 1, timestamp: Math.max(now, row?.ts ?? 0) }
  if (!Number.isSafeInteger(mark.seq) || mark.seq <= 0 || !Number.isSafeInteger(mark.timestamp)) {
    throw new Error('commitLane: invalid ordering position')
  }
  sql.exec(
    'INSERT OR REPLACE INTO ord (inc, domain, seq, ts) VALUES (?, ?, ?, ?)',
    inc,
    domain,
    mark.seq,
    mark.timestamp,
  )
  return mark
}

// Drops every generation row; the DO separately refuses its live incarnation.
export function dropGenerationRows(sql: SqlStorage, inc: string): void {
  for (const table of ['cell', 'ord', 'rt_manifest', 'rt_chunk', 'route', 'gen']) {
    sql.exec(`DELETE FROM ${table} WHERE inc = ?`, inc)
  }
}
