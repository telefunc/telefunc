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
  heldCell,
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
  storeAnswer,
  readStoredAnswer,
  recordAnswerLoss,
  deleteExpiredAnswers,
}

export type { CellPage, Handoff, ListingPlace, AnswerKey, KeptAnswer }

// Room-DO `transactionSync` makes head exchanges, cell batches, and order advance atomic under authority time.

import {
  CELL_PAGE_SIZE,
  type CellCounter,
  type CellsExchange,
  type CellsExchangeResult,
  type HeadExpectation,
  type HeadNext,
  type CellSelector,
  type CellsBeside,
  type CellsRead,
  type ExpiringCell,
  type ExpiringCellsRead,
  type StaleIncarnation,
} from '../backend/contract.js'
import {
  blobColumn,
  dropLeaseScopes,
  initLeaseSchema,
  integerColumn,
  nextLeaseSweep,
  nullableIntegerColumn,
  nullableTextColumn,
  textColumn,
  type LeaseOrder,
  type SqlRow,
  type SqlStore,
} from 'telefunc/__internal'
import { incarnationScopes } from './routes.js'
import {
  countCell,
  encodeCellCounter,
  hasLapsed,
  type HeldCell,
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

function initSchema(sql: SqlStore): void {
  // The DO is the room: `head` is one row or absent; `gen` is each installed incarnation's last cell version. `listing`
  // is the room's listing, one row or none.
  sql.exec(`
    CREATE TABLE IF NOT EXISTS head
      (id INTEGER PRIMARY KEY CHECK (id = 1), rev TEXT NOT NULL, inc TEXT, state TEXT NOT NULL, config BLOB NOT NULL,
       lease_id TEXT, lease_until INTEGER, expires_at INTEGER);
    CREATE TABLE IF NOT EXISTS gen (inc TEXT PRIMARY KEY, revision INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS cell
      (inc TEXT NOT NULL, key TEXT NOT NULL, bytes BLOB NOT NULL, version INTEGER NOT NULL, lapses_at INTEGER,
       PRIMARY KEY (inc, key));
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
    CREATE TABLE IF NOT EXISTS stored_answer
      (scope TEXT NOT NULL, holder TEXT NOT NULL, grant_id INTEGER NOT NULL, seq INTEGER NOT NULL, inc TEXT NOT NULL,
       commit_seq INTEGER, answer BLOB NOT NULL, lost TEXT, kept_until INTEGER NOT NULL,
       PRIMARY KEY (scope, holder, grant_id, seq));
    CREATE INDEX IF NOT EXISTS stored_answer_kept_until ON stored_answer(kept_until);
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

function readListingRow(sql: SqlStore): Listing | null {
  const row = sql.exec('SELECT room_id, inc_tag, bucket, version, due_at FROM listing WHERE id = 1').toArray()[0]
  if (row === undefined) return null
  return {
    roomId: textColumn(row, 'room_id'),
    incTag: nullableTextColumn(row, 'inc_tag'),
    bucket: textColumn(row, 'bucket'),
    version: integerColumn(row, 'version'),
    dueAt: nullableIntegerColumn(row, 'due_at'),
  }
}

/** Lists what `head` lists, an open or closing incarnation, or nothing: a change is a new version, due `now`. */
function listWithHead(sql: SqlStore, head: StoredHead | null, now: number, place?: ListingPlace): void {
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
function settleListing(sql: SqlStore, now: number): void {
  listWithHead(sql, readLiveHead(sql, now), now)
}

function readListedInc(sql: SqlStore): string | undefined {
  return readListingRow(sql)?.incTag ?? undefined
}

/** The listing while its directory may not have its version: the alarm sends it at `dueAt`, any send before. */
function readUnsentListing(sql: SqlStore): Listing | null {
  const listing = readListingRow(sql)
  return listing !== null && listing.dueAt !== null ? listing : null
}

/** `version` is due again at `dueAt`, or never (null) once the directory has it; a newer version stays due. */
function resendListing(sql: SqlStore, version: number, dueAt: number | null): void {
  sql.exec('UPDATE listing SET due_at = ? WHERE id = 1 AND version = ?', dueAt, version)
}

/** A new version of a listing the directory has, due `now`, unless it names `staleInc`: the directory was found
 *  listing that incarnation, as after a call delivered later than its successors. */
function relistOver(sql: SqlStore, staleInc: string, now: number): void {
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
function readLiveHead(sql: SqlStore, now: number): StoredHead | null {
  const row = sql
    .exec('SELECT rev, inc, state, config, lease_id, lease_until, expires_at FROM head WHERE id = 1')
    .toArray()[0]
  if (row === undefined) return null
  const expiresAt = nullableIntegerColumn(row, 'expires_at')
  if (expiresAt !== null && expiresAt <= now) return null
  const head: StoredHead = {
    rev: textColumn(row, 'rev'),
    currentInc: nullableTextColumn(row, 'inc'),
    state: headState(row),
    config: new Uint8Array(blobColumn(row, 'config')),
    expiresAt,
  }
  const leaseId = nullableTextColumn(row, 'lease_id')
  const leaseUntil = nullableIntegerColumn(row, 'lease_until')
  if (leaseId !== null && leaseUntil !== null) head.closeLease = { id: leaseId, until: leaseUntil }
  return head
}

function headState(row: SqlRow): StoredHead['state'] {
  const state = textColumn(row, 'state')
  if (state === 'open' || state === 'closing' || state === 'closed') return state
  throw new Error(`The head row holds the state '${state}', which no head has`)
}

/** A lapsed head is reclaimed here: this backend has no native head TTL. */
function deleteLapsedHead(sql: SqlStore, now: number): void {
  sql.exec('DELETE FROM head WHERE id = 1 AND expires_at IS NOT NULL AND expires_at <= ?', now)
}

/** Installed incarnations other than `currentInc`. */
function listOrphanIncarnations(sql: SqlStore, currentInc: string | null): string[] {
  return sql
    .exec('SELECT inc FROM gen WHERE inc IS NOT ?', currentInc)
    .toArray()
    .map((row) => textColumn(row, 'inc'))
}

/** Whether `inc`'s incarnation is stored: `dropIncarnationRows` deletes it with its routes. */
function hasIncarnation(sql: SqlStore, inc: string): boolean {
  return sql.exec('SELECT 1 FROM gen WHERE inc = ?', inc).toArray().length > 0
}

/** When the alarm's sweep next has work: `now` while an orphaned incarnation waits, else the first lapse of a head, an
 *  expiring cell or a stored answer, the listing's next send, or when a route's session DO may be dropped, or null for
 *  none. */
function nextMaintenanceDeadline(sql: SqlStore, now: number): number | null {
  if (listOrphanIncarnations(sql, readLiveHead(sql, now)?.currentInc ?? null).length > 0) return now
  // Each table takes its own MIN, so the expiring_cell table's seeks its expires_at index.
  const deadline = nullableIntegerColumn(
    sql
      .exec(
        'SELECT MIN(deadline) AS deadline FROM (SELECT MIN(expires_at) AS deadline FROM head ' +
          'UNION ALL SELECT MIN(expires_at) FROM expiring_cell UNION ALL SELECT MIN(due_at) FROM listing ' +
          'UNION ALL SELECT MIN(resume_at) FROM handoff UNION ALL SELECT MIN(kept_until) FROM stored_answer)',
      )
      .one(),
    'deadline',
  )
  const sweep = nextLeaseSweep(sql)
  return deadline === null || sweep === null ? (deadline ?? sweep) : Math.min(deadline, sweep)
}

/** Called inside `transactionSync`; a lost race returns the current head. */
function compareExchangeHead(
  sql: SqlStore,
  expected: HeadExpectation,
  next: HeadNext,
  now: number,
): HeadExchangeOutcome {
  const current = readLiveHead(sql, now)
  if (!headExpectationHolds(expected, current, now)) return { kind: 'conflict', current }
  return { kind: 'committed', head: storeHead(sql, next, now) }
}

function storeHead(sql: SqlStore, next: HeadNext, now: number): StoredHead {
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

/** `lapsesAt`: the authority time a cell written with `lapsesInMs` lapses at. */
type CellRow = { key: string; bytes: ArrayBuffer; version: number; lapsesAt: number | null }

const CELL_COLUMNS = 'key, bytes, version, lapses_at'

function cellRecord(row: SqlRow): CellRow {
  return {
    key: textColumn(row, 'key'),
    bytes: blobColumn(row, 'bytes'),
    version: integerColumn(row, 'version'),
    lapsesAt: nullableIntegerColumn(row, 'lapses_at'),
  }
}

/** A prefix read's page, with the cursor its next page reads after while cells may remain. */
type CellPage = (Extract<CellsRead, { kind: 'read' }> & { cursor?: string }) | StaleIncarnation

function cellPage(
  sql: SqlStore,
  inc: string,
  rows: CellRow[],
  now: number,
  beside?: CellsBeside,
): Extract<CellPage, { kind: 'read' }> {
  const page: Extract<CellPage, { kind: 'read' }> = {
    kind: 'read',
    cells: new Map(rows.map((row) => [row.key, new Uint8Array(row.bytes)])),
    versions: new Map(rows.map((row) => [row.key, String(row.version)])),
    now,
    lapsed: new Set(rows.filter((row) => rowLapsed(row, now)).map((row) => row.key)),
  }
  if (beside !== undefined) page.beside = cellsBeside(sql, inc, rows, beside, now)
  return page
}

/** By the key of each row under `beside.of` whose reference is there, the cell it names, `null` where it is absent or
 *  lapsed. */
function cellsBeside(
  sql: SqlStore,
  inc: string,
  rows: CellRow[],
  beside: CellsBeside,
  now: number,
): Map<string, Uint8Array | null> {
  const besides = new Map<string, Uint8Array | null>()
  for (const { key } of rows) {
    if (!key.startsWith(beside.of)) continue
    const reference = cellRow(sql, inc, beside.reference + key.slice(beside.of.length))
    if (reference === undefined) continue
    const target = cellRow(sql, inc, beside.target + referenceDecoder.decode(reference.bytes))
    besides.set(key, target === undefined || rowLapsed(target, now) ? null : new Uint8Array(target.bytes))
  }
  return besides
}

const referenceDecoder = new TextDecoder()

function rowLapsed(row: CellRow, now: number): boolean {
  return hasLapsed(row.lapsesAt ?? undefined, now)
}

function cellRow(sql: SqlStore, inc: string, key: string): CellRow | undefined {
  const row = sql.exec(`SELECT ${CELL_COLUMNS} FROM cell WHERE inc = ? AND key = ?`, inc, key).toArray()[0]
  return row === undefined ? undefined : cellRecord(row)
}

/** The cell `key` as a commit's check of its required cells finds it (`unheldRequiredCell()`). */
function heldCell(sql: SqlStore, inc: string, key: string, now: number): HeldCell | undefined {
  const row = cellRow(sql, inc, key)
  return row && { bytes: new Uint8Array(row.bytes), lapsed: rowLapsed(row, now) }
}

function selectCellRows(sql: SqlStore, inc: string, sel: CellSelector): CellRow[] {
  if ('keys' in sel) return sel.keys.flatMap((key) => cellRow(sql, inc, key) ?? [])
  const range = keyRange('key', sel.prefix)
  return sql
    .exec(`SELECT ${CELL_COLUMNS} FROM cell WHERE inc = ? AND ${range.where}`, inc, ...range.args)
    .toArray()
    .map(cellRecord)
}

/** Reads stay available while closing; a stale incarnation is one the head is absent for or doesn't name. */
function readCells(sql: SqlStore, inc: string, sel: CellSelector, now: number): CellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { kind: 'stale-incarnation' }
  return cellPage(sql, inc, selectCellRows(sql, inc, sel), now, sel.beside)
}

/** The expiring cells of `keys` that haven't expired; the alarm deletes the others. */
function readExpiringCells(sql: SqlStore, inc: string, keys: readonly string[], now: number): ExpiringCellsRead {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { kind: 'stale-incarnation' }
  const cells = new Map<string, Uint8Array>()
  for (const key of keys) {
    const row = sql
      .exec('SELECT bytes FROM expiring_cell WHERE inc = ? AND key = ? AND expires_at > ?', inc, key, now)
      .toArray()[0]
    if (row !== undefined) cells.set(key, new Uint8Array(blobColumn(row, 'bytes')))
  }
  return { kind: 'read', cells }
}

function writeExpiringCell(sql: SqlStore, inc: string, { key, bytes, ttlMs }: ExpiringCell, now: number): void {
  sql.exec(
    'INSERT OR REPLACE INTO expiring_cell (inc, key, bytes, expires_at) VALUES (?, ?, ?, ?)',
    inc,
    key,
    bytes,
    now + ttlMs,
  )
}

function deleteExpiredCells(sql: SqlStore, now: number): void {
  sql.exec('DELETE FROM expiring_cell WHERE expires_at <= ?', now)
}

function readCellPage(
  sql: SqlStore,
  inc: string,
  prefix: string,
  cursor: string | undefined,
  now: number,
  beside?: CellsBeside,
): CellPage {
  const head = readLiveHead(sql, now)
  if (head === null || head.currentInc !== inc) return { kind: 'stale-incarnation' }
  const range = keyRange('key', prefix, cursor)
  const rows = sql
    .exec(
      `SELECT ${CELL_COLUMNS} FROM cell WHERE inc = ? AND ${range.where} ORDER BY key LIMIT ?`,
      inc,
      ...range.args,
      CELL_PAGE_SIZE,
    )
    .toArray()
    .map(cellRecord)
  const page = cellPage(sql, inc, rows, now, beside)
  const last = rows[CELL_PAGE_SIZE - 1]
  if (last !== undefined) page.cursor = last.key
  return page
}

function readRevision(sql: SqlStore, inc: string): number {
  const row = sql.exec('SELECT revision FROM gen WHERE inc = ?', inc).toArray()[0]
  return row === undefined ? 0 : integerColumn(row, 'revision')
}

function cellVersion(sql: SqlStore, inc: string, key: string): string | null {
  const row = sql.exec('SELECT version FROM cell WHERE inc = ? AND key = ?', inc, key).toArray()[0]
  return row === undefined ? null : String(integerColumn(row, 'version'))
}

/** `lapsesAt`: null for a cell that never lapses. */
function writeCell(
  sql: SqlStore,
  inc: string,
  key: string,
  bytes: Uint8Array | null,
  version: number,
  lapsesAt: number | null,
): void {
  if (bytes === null) sql.exec('DELETE FROM cell WHERE inc = ? AND key = ?', inc, key)
  else {
    sql.exec(
      'INSERT OR REPLACE INTO cell (inc, key, bytes, version, lapses_at) VALUES (?, ?, ?, ?, ?)',
      inc,
      key,
      bytes,
      version,
      lapsesAt,
    )
  }
}

/** Deletes the incarnation's cells under `prefix`. */
function dropCellPrefix(sql: SqlStore, inc: string, prefix: string): void {
  const range = keyRange('key', prefix)
  sql.exec(`DELETE FROM cell WHERE inc = ? AND ${range.where}`, inc, ...range.args)
}

/** Cell writes are all-or-nothing while every expected cell is at its version, and require an open head. */
function compareExchangeCells(sql: SqlStore, inc: string, exchange: CellsExchange, now: number): CellsExchangeResult {
  const head = readLiveHead(sql, now)
  if (!isOpenIncarnation(head, inc)) return { kind: 'stale-incarnation' }
  if (exchange.expect.some(({ key, version }) => cellVersion(sql, inc, key) !== version)) return { kind: 'conflict' }
  sql.exec('UPDATE gen SET revision = revision + 1 WHERE inc = ?', inc)
  const version = readRevision(sql, inc)
  for (const { key, bytes, lapsesInMs } of exchange.mutations) {
    writeCell(sql, inc, key, bytes, version, lapsesInMs === undefined ? null : now + lapsesInMs)
  }
  const counters = new Map<string, CellCounter>()
  for (const count of exchange.counts ?? []) {
    const current = selectCellRows(sql, inc, { keys: [count.key] })[0]
    const counted = countCell(current === undefined ? undefined : new Uint8Array(current.bytes), count, now)
    if (counted === null) continue
    if (counted.written) writeCell(sql, inc, count.key, encodeCellCounter(counted.counter), version, null)
    counters.set(count.key, counted.counter)
  }
  return { kind: 'committed', counters }
}

function readOrder(sql: SqlStore, inc: string, domain: string): OrderingInfo | null {
  const row = sql.exec('SELECT seq, ts FROM ord WHERE inc = ? AND domain = ?', inc, domain).toArray()[0]
  return row === undefined ? null : { seq: integerColumn(row, 'seq'), timestamp: integerColumn(row, 'ts') }
}

/** `seq` strictly increases for the lifetime of a domain instance. */
function advanceOrder(sql: SqlStore, inc: string, domain: string, now: number): OrderingInfo {
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
function dropLaneOrderRows(sql: SqlStore, inc: string, member: string): void {
  const range = keyRange('domain', binaryLaneKeyPrefix(member))
  sql.exec(
    `DELETE FROM ord WHERE inc = ? AND (domain = ? OR (${range.where}))`,
    inc,
    encodeLaneKey({ kind: 'inbox', member }),
    ...range.args,
  )
}

function dropIncarnationRows(sql: SqlStore, inc: string): void {
  for (const table of ['cell', 'expiring_cell', 'ord', 'rt_manifest', 'rt_chunk', 'gen', 'handoff']) {
    sql.exec(`DELETE FROM ${table} WHERE inc = ?`, inc)
  }
  dropLeaseScopes(sql, incarnationScopes(inc))
}

/** A commit stored with its transaction until it was handed on, so a restart in between hands it on still. */
type Handoff = { inc: string; laneKey: string; seq: number; timestamp: number; payload: Uint8Array }

function storeHandoff(sql: SqlStore, { inc, laneKey, seq, timestamp, payload }: Handoff, resumeAt: number): void {
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
function listHandoffs(sql: SqlStore): Handoff[] {
  return sql
    .exec('SELECT inc, seq, lane_key, ts, payload FROM handoff ORDER BY inc, lane_key, seq')
    .toArray()
    .map((row) => ({
      inc: textColumn(row, 'inc'),
      laneKey: textColumn(row, 'lane_key'),
      seq: integerColumn(row, 'seq'),
      timestamp: integerColumn(row, 'ts'),
      payload: new Uint8Array(blobColumn(row, 'payload')),
    }))
}

function deleteHandoff(sql: SqlStore, { inc, laneKey, seq }: Pick<Handoff, 'inc' | 'laneKey' | 'seq'>): void {
  sql.exec('DELETE FROM handoff WHERE inc = ? AND lane_key = ? AND seq = ?', inc, laneKey, seq)
}

/** The write that registered an inbox lease in an operation's transaction: the lease's scope and holder, and the order
 *  the holder sent it under. */
type AnswerKey = { scope: string; holder: string; order: LeaseOrder }
/** What that operation answered, its commit's `seq` if it committed, and why the commit's hand-on lost it, if it did. */
type KeptAnswer = { answer: Uint8Array; lost: string | null }

function storeAnswer(
  sql: SqlStore,
  { scope, holder, order }: AnswerKey,
  inc: string,
  { answer, commitSeq }: { answer: Uint8Array; commitSeq: number | null },
  keptUntil: number,
): void {
  sql.exec(
    'INSERT INTO stored_answer (scope, holder, grant_id, seq, inc, commit_seq, answer, kept_until) ' +
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    scope,
    holder,
    order.grant,
    order.seq,
    inc,
    commitSeq,
    answer,
    keptUntil,
  )
}

function readStoredAnswer(sql: SqlStore, { scope, holder, order }: AnswerKey): KeptAnswer | null {
  const row = sql
    .exec(
      'SELECT answer, lost FROM stored_answer WHERE scope = ? AND holder = ? AND grant_id = ? AND seq = ?',
      scope,
      holder,
      order.grant,
      order.seq,
    )
    .toArray()[0]
  return row === undefined
    ? null
    : { answer: new Uint8Array(blobColumn(row, 'answer')), lost: nullableTextColumn(row, 'lost') }
}

/** The hand-on of `inc`'s commit `commitSeq` lost it: the answer of the operation that committed it says so. */
function recordAnswerLoss(sql: SqlStore, inc: string, commitSeq: number, lost: string): void {
  sql.exec('UPDATE stored_answer SET lost = ? WHERE inc = ? AND commit_seq = ?', lost, inc, commitSeq)
}

function deleteExpiredAnswers(sql: SqlStore, now: number): void {
  sql.exec('DELETE FROM stored_answer WHERE kept_until <= ?', now)
}
