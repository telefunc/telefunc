/// <reference types="@cloudflare/workers-types" />
// Room-DO `transactionSync` makes head CX, cell batches, and order advance atomic under authority time.

import type { CellMutation, CxResult, HeadCx, HeadNext, RoomHead } from '../../../../backend/room/contract.js'
import { headCxMatches, nextOrderMark, type OrderMark } from '../../../../backend/room/semantics.js'
export type { OrderMark }
export type StoredHead = RoomHead & { expiresAt: number | null }

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

export function toBytes(value: ArrayBuffer | Uint8Array): Uint8Array {
  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

export function initSchema(sql: SqlStorage): void {
  // The DO is the room: `head` is one row or absent; `gen` is each installed incarnation's cell revision.
  sql.exec(`
    CREATE TABLE IF NOT EXISTS head
      (id INTEGER PRIMARY KEY CHECK (id = 1), rev TEXT NOT NULL, inc TEXT, state TEXT NOT NULL, config BLOB NOT NULL, lease_id TEXT, lease_until INTEGER, expires_at INTEGER);
    CREATE TABLE IF NOT EXISTS gen (inc TEXT PRIMARY KEY, revision INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS cell
      (inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (inc, key));
    CREATE TABLE IF NOT EXISTS ord
      (inc TEXT NOT NULL, domain TEXT NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (inc, domain));
    CREATE TABLE IF NOT EXISTS rt_manifest
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, size INTEGER NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (inc, lane_key));
    CREATE TABLE IF NOT EXISTS rt_chunk
      (inc TEXT NOT NULL, lane_key TEXT NOT NULL, i INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (inc, lane_key, i));
    CREATE TABLE IF NOT EXISTS route
      (room_id TEXT NOT NULL, inc TEXT NOT NULL, lane_key TEXT NOT NULL, session_do_id TEXT NOT NULL, lease_id TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (inc, lane_key, session_do_id));
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
  const after = cursor ?? null
  const matching = sql
    .exec<{ room_id: string; inc_tag: string }>(
      'SELECT room_id, inc_tag FROM directory WHERE substr(room_id, 1, length(?)) = ? AND (? IS NULL OR room_id > ?) ORDER BY room_id LIMIT ?',
      prefix,
      prefix,
      after,
      after,
      DIRECTORY_PAGE_SIZE + 1,
    )
    .toArray()
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
  return sql.exec('SELECT 1 FROM gen WHERE inc = ?', inc).toArray().length > 0
}

export function hasOrphanGeneration(sql: SqlStorage, currentInc: string | null): boolean {
  return sql.exec('SELECT 1 FROM gen WHERE inc IS NOT ? LIMIT 1', currentInc).toArray().length > 0
}

/** A lapsed tombstone is reclaimed here: this backend has no native head TTL. */
export function deleteLapsedTombstone(sql: SqlStorage, now: number): void {
  sql.exec("DELETE FROM head WHERE id = 1 AND state = 'closed' AND expires_at IS NOT NULL AND expires_at <= ?", now)
}

/** Installed incarnations other than `currentInc`. */
export function listOrphanGenerations(sql: SqlStorage, currentInc: string | null): string[] {
  return sql
    .exec<{ inc: string }>('SELECT inc FROM gen WHERE inc IS NOT ?', currentInc)
    .toArray()
    .map((row) => row.inc)
}

// Called inside `transactionSync`; a lost race returns the current head.
export function compareExchangeHead(
  sql: SqlStorage,
  cx: HeadCx,
  next: HeadNext,
  now: number,
  mintRev: () => string,
): HeadCxOutcome {
  const current = readLiveHead(sql, now)
  if (!headCxMatches(cx, current, now)) return { conflict: true, current }
  return { head: storeHead(sql, next, now, mintRev) }
}

function storeHead(sql: SqlStorage, next: HeadNext, now: number, mintRev: () => string): StoredHead {
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
  // A new incarnation's generation is registered inside the CX that names it.
  if (next.head.currentInc !== null) {
    sql.exec('INSERT OR IGNORE INTO gen (inc, revision) VALUES (?, 0)', next.head.currentInc)
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
    if (mutation.bytes === null) {
      sql.exec('DELETE FROM cell WHERE inc = ? AND key = ?', inc, mutation.key)
    } else {
      sql.exec('INSERT OR REPLACE INTO cell (inc, key, bytes) VALUES (?, ?, ?)', inc, mutation.key, mutation.bytes)
    }
  }
  sql.exec('UPDATE gen SET revision = revision + 1 WHERE inc = ?', inc)
  return 'committed'
}

// `seq` strictly increases for the lifetime of a domain instance.
export function advanceOrder(sql: SqlStorage, inc: string, domain: string, now: number): OrderMark {
  const row = sql
    .exec<{ seq: number; ts: number }>('SELECT seq, ts FROM ord WHERE inc = ? AND domain = ?', inc, domain)
    .toArray()[0]
  const mark = nextOrderMark(row === undefined ? undefined : { seq: row.seq, timestamp: row.ts }, now)
  sql.exec(
    'INSERT OR REPLACE INTO ord (inc, domain, seq, ts) VALUES (?, ?, ?, ?)',
    inc,
    domain,
    mark.seq,
    mark.timestamp,
  )
  return mark
}

// Drops every generation row.
export function dropGenerationRows(sql: SqlStorage, inc: string): void {
  for (const table of ['cell', 'ord', 'rt_manifest', 'rt_chunk', 'route', 'gen']) {
    sql.exec(`DELETE FROM ${table} WHERE inc = ?`, inc)
  }
}
