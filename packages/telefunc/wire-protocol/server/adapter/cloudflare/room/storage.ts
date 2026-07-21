/// <reference types="@cloudflare/workers-types" />
// The atomicity invariant (I1/I3/I4): SQL schema + head CX + cell CX + order rows. Every mutating verb
// here runs inside the room DO's single `transactionSync`, so a single-object serialization gives head
// linearizability, all-or-nothing cell batches, and atomic order advance. Time is always the room DO's
// own authority clock, passed in as `now`.

import type { CellMutation, CxResult, HeadCx, HeadNext } from '../../../../backend/spi.js'
import {
  assertDeleteLegal,
  assertHeadNextWellFormed,
  assertTransitionAllowed,
  type HeadSnapshot,
  type HeadWriteNext,
} from './transitions.js'

export type StoredHead = HeadSnapshot & { rev: string; expiresAt: number | null }

export const GEN_ORPHAN_GRACE_MS = 60_000

export type HeadCxOutcome =
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

function toBytes(value: ArrayBuffer | Uint8Array): Uint8Array {
  return value instanceof Uint8Array ? value : new Uint8Array(value)
}

export function initSchema(sql: SqlStorage): void {
  // One DO per room, so nothing is keyed by roomId — the DO IS the room. `head` is a single row (id=1)
  // or absent. `gen` is the generation registry (listGenerations + the fresh-inc guard) and also carries
  // each generation's monotonic cell revision.
  sql.exec(`
    CREATE TABLE IF NOT EXISTS head (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      rev TEXT NOT NULL, inc TEXT, state TEXT NOT NULL, config BLOB NOT NULL,
      lease_id TEXT, lease_until INTEGER, expires_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS gen (
      inc TEXT PRIMARY KEY, revision INTEGER NOT NULL, orphan_since INTEGER
    );
    CREATE TABLE IF NOT EXISTS cell (
      inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, expires_at INTEGER,
      PRIMARY KEY (inc, key)
    );
    CREATE TABLE IF NOT EXISTS ord (
      inc TEXT NOT NULL, domain TEXT NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, expires_at INTEGER,
      PRIMARY KEY (inc, domain)
    );
    CREATE TABLE IF NOT EXISTS rt_manifest (
      inc TEXT NOT NULL, lane_key TEXT NOT NULL, gen INTEGER NOT NULL, size INTEGER NOT NULL,
      chunks INTEGER NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL,
      lane_kind TEXT NOT NULL, lane_member TEXT, lane_track TEXT,
      PRIMARY KEY (inc, lane_key)
    );
    CREATE TABLE IF NOT EXISTS rt_chunk (
      inc TEXT NOT NULL, lane_key TEXT NOT NULL, gen INTEGER NOT NULL, i INTEGER NOT NULL, bytes BLOB NOT NULL,
      PRIMARY KEY (inc, lane_key, gen, i)
    );
    CREATE TABLE IF NOT EXISTS route (
      room_id TEXT NOT NULL, inc TEXT NOT NULL, lane_key TEXT NOT NULL, subscriber TEXT NOT NULL,
      lease_id TEXT NOT NULL, bucket TEXT, expires_at INTEGER NOT NULL, failures INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (inc, lane_key, subscriber)
    );
    CREATE TABLE IF NOT EXISTS directory (room_id TEXT PRIMARY KEY, inc_tag TEXT NOT NULL);
  `)
}

// ── directory (best-effort projection; hosted on a singleton DO — one row per roomId, tag-guarded) ──

export const DIRECTORY_PAGE_SIZE = 100

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
  const matching = sql
    .exec<{ room_id: string; inc_tag: string }>(
      'SELECT room_id, inc_tag FROM directory WHERE substr(room_id, 1, length(?)) = ? ORDER BY room_id',
      prefix,
      prefix,
    )
    .toArray()
  const start = cursor === undefined ? 0 : matching.findIndex((row) => row.room_id > cursor)
  if (start < 0) return { entries: [] }
  const page = matching.slice(start, start + DIRECTORY_PAGE_SIZE)
  const entries = page.map((row) => ({ roomId: row.room_id, incTag: row.inc_tag }))
  const last = page[page.length - 1]
  const more = last !== undefined && start + page.length < matching.length
  return more ? { entries, cursor: last.room_id } : { entries }
}

// The live head: a lapsed tombstone reads as absent (which reopens the absence epoch, I1). Reads never
// consult a caller clock — `now` is authority time.
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
    return (
      sql
        .exec(
          "SELECT 1 FROM head WHERE id = 1 AND rev = ? AND state = 'closing' AND lease_until IS NOT NULL AND lease_until < ?",
          expect.rev,
          now,
        )
        .toArray().length === 1
    )
  }
  if ('closingLease' in expect) {
    return (
      sql
        .exec(
          "SELECT 1 FROM head WHERE id = 1 AND rev = ? AND state = 'closing' AND lease_id = ?",
          expect.rev,
          expect.closingLease,
        )
        .toArray().length === 1
    )
  }
  return sql.exec('SELECT 1 FROM head WHERE id = 1 AND rev = ?', expect.rev).toArray().length === 1
}

// Head CX — the whole compare-and-store, meant to be called INSIDE `transactionSync`. Validation throws
// (programming error, never a conflict); a lost race returns a conflict with the current head.
export function compareExchangeHead(
  sql: SqlStorage,
  cx: HeadCx,
  next: HeadNext,
  now: number,
  mintRev: () => string,
): HeadCxOutcome {
  assertHeadNextWellFormed(next)
  const current = readLiveHead(sql, now)
  // Operation legality precedes the compare for the delete path only; every other transition is validated
  // against the head the compare actually matched, so a genuine race still conflicts.
  assertDeleteLegal(next, current)
  if (!headCxMatches(sql, cx, current, now)) return { conflict: true, current }
  if ('delete' in next) {
    sql.exec('DELETE FROM head WHERE id = 1')
    return { ok: true, deleted: true }
  }
  assertTransitionAllowed(cx, next as HeadWriteNext, current, (inc) => hasGeneration(sql, inc))
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
  // A generation exists from the moment its inc is installed (registered inside this same CX) — that is
  // what makes the fresh-inc guard deterministic. Empty until written; never re-zeroed on re-store.
  if (next.head.currentInc !== null) {
    sql.exec('INSERT OR IGNORE INTO gen (inc, revision, orphan_since) VALUES (?, 0, NULL)', next.head.currentInc)
    sql.exec('UPDATE gen SET orphan_since = NULL WHERE inc = ?', next.head.currentInc)
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

// ── generation cells ──

export type CellsRead = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }

// Reads require only generation existence (staleInc iff head absent or currentInc ≠ inc); they stay
// available while 'closing' because the closer's tail needs them.
export function readCells(
  sql: SqlStorage,
  inc: string,
  sel: { keys: string[] } | { prefix: string },
  now: number,
): CellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { staleInc: true }
  const revision = String(readRevision(sql, inc))
  const cells = new Map<string, Uint8Array>()
  if ('keys' in sel) {
    for (const key of sel.keys) {
      const row = sql
        .exec<{ bytes: ArrayBuffer; expires_at: number | null }>(
          'SELECT bytes, expires_at FROM cell WHERE inc = ? AND key = ?',
          inc,
          key,
        )
        .toArray()[0]
      if (row === undefined || (row.expires_at !== null && row.expires_at <= now)) continue
      cells.set(key, toBytes(row.bytes))
    }
  } else {
    const rows = sql
      .exec<{ key: string; bytes: ArrayBuffer; expires_at: number | null }>(
        'SELECT key, bytes, expires_at FROM cell WHERE inc = ? AND substr(key, 1, length(?)) = ?',
        inc,
        sel.prefix,
        sel.prefix,
      )
      .toArray()
    for (const row of rows) {
      if (row.expires_at !== null && row.expires_at <= now) continue
      cells.set(row.key, toBytes(row.bytes))
    }
  }
  return { revision, cells }
}

function readRevision(sql: SqlStorage, inc: string): number {
  const row = sql.exec<{ revision: number }>('SELECT revision FROM gen WHERE inc = ?', inc).toArray()[0]
  return row?.revision ?? 0
}

// Cell CX — all-or-nothing under the read-set revision, inside `transactionSync`. Writes additionally
// require an OPEN head (unlike reads).
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
      const expiresAt = mutation.set.ttlMs === undefined ? null : now + mutation.set.ttlMs
      sql.exec(
        'INSERT OR REPLACE INTO cell (inc, key, bytes, expires_at) VALUES (?, ?, ?, ?)',
        inc,
        mutation.key,
        mutation.set.bytes,
        expiresAt,
      )
    }
  }
  sql.exec('UPDATE gen SET revision = revision + 1 WHERE inc = ?', inc)
  return 'committed'
}

// ── order domains ──

export type OrderMark = { seq: number; timestamp: number }

// seq strictly increases; the timestamp is clamped so it never moves backwards within a domain. A lapsed
// order row restarts the domain — matching the memory reference's lazy expiry.
export function advanceOrder(
  sql: SqlStorage,
  inc: string,
  domain: string,
  now: number,
  ttlMs: number | undefined,
): OrderMark {
  const row = sql
    .exec<{ seq: number; ts: number; expires_at: number | null }>(
      'SELECT seq, ts, expires_at FROM ord WHERE inc = ? AND domain = ?',
      inc,
      domain,
    )
    .toArray()[0]
  const live = row !== undefined && !(row.expires_at !== null && row.expires_at <= now) ? row : undefined
  const mark: OrderMark = { seq: (live?.seq ?? 0) + 1, timestamp: Math.max(now, live?.ts ?? 0) }
  const expiresAt = ttlMs === undefined ? null : now + ttlMs
  sql.exec(
    'INSERT OR REPLACE INTO ord (inc, domain, seq, ts, expires_at) VALUES (?, ?, ?, ?, ?)',
    inc,
    domain,
    mark.seq,
    mark.timestamp,
    expiresAt,
  )
  return mark
}

// Drop every trace of one generation (cells, order, retained, routes, registry row). Refuses the current
// incarnation is enforced by the caller (the DO), which holds the live head.
export function dropGenerationRows(sql: SqlStorage, inc: string): void {
  sql.exec('DELETE FROM cell WHERE inc = ?', inc)
  sql.exec('DELETE FROM ord WHERE inc = ?', inc)
  sql.exec('DELETE FROM rt_manifest WHERE inc = ?', inc)
  sql.exec('DELETE FROM rt_chunk WHERE inc = ?', inc)
  sql.exec('DELETE FROM route WHERE inc = ?', inc)
  sql.exec('DELETE FROM gen WHERE inc = ?', inc)
}

// Orphan age is observation-based: the close path does not write cleanup metadata. A mechanical sweep
// first stamps every non-current generation it sees, then returns only entries whose full grace window
// has elapsed. The caller deletes those entries with dropGenerationRows(), preserving physical scoping.
export function observeAndListGraceAgedOrphans(
  sql: SqlStorage,
  currentInc: string | null,
  now: number,
  graceMs: number,
): string[] {
  if (currentInc !== null) {
    sql.exec('UPDATE gen SET orphan_since = NULL WHERE inc = ?', currentInc)
    sql.exec('UPDATE gen SET orphan_since = ? WHERE inc != ? AND orphan_since IS NULL', now, currentInc)
    return sql
      .exec<{ inc: string }>(
        'SELECT inc FROM gen WHERE inc != ? AND orphan_since IS NOT NULL AND orphan_since + ? <= ?',
        currentInc,
        graceMs,
        now,
      )
      .toArray()
      .map((row) => row.inc)
  }
  sql.exec('UPDATE gen SET orphan_since = ? WHERE orphan_since IS NULL', now)
  return sql
    .exec<{ inc: string }>('SELECT inc FROM gen WHERE orphan_since IS NOT NULL AND orphan_since + ? <= ?', graceMs, now)
    .toArray()
    .map((row) => row.inc)
}
