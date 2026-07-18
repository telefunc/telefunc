export { captureMutation, emitSafely }
export type { CaptureSink }

import { type Column, SQL, type Table, getTableColumns, getTableName, is, isTable } from 'drizzle-orm'
import { dialectOf, driverOf, isSingleSession } from '../binding/database.js'
import { primaryKeyOf } from '../extract/columns.js'
import { ingestWrite } from './dbRuntime.js'
import type { Row, TableChange } from '../router/events.js'

// The write-capture engine. `reactiveDrizzle`'s proxy routes insert/update/delete here. The write runs as
// plain Drizzle; the terminal is intercepted to capture the changed rows and feed a `ChangeBatch` to the
// db's graphs (via the sink). Under decision #3's "new + old-PK only" contract:
//   INSERT  → { kind:'insert', new: full row }
//   DELETE  → { kind:'delete', key: PK }        (retraction by old PK)
//   UPDATE  → { kind:'update', new: full row, key: PK }   (key = old PK = new PK; non-PK-changing only)
//
// PRECISION is gated + fails closed (emit one {table, kind:'coarse'}) — safe over-fire, never a wrong row:
//   - PG/SQLite only (MySQL has no RETURNING → precise MySQL needs a pre-write SELECT + a live MySQL test
//     lane that does not exist in this package yet → deferred; MySQL stays sound-coarse);
//   - single-session only (decision #6: pooled connections can't prove session authority → coarse);
//   - a resolvable PK (single OR composite); a table with no PK → coarse (a retraction can't be keyed);
//   - not an UPSERT / ON CONFLICT, not a raw-SQL/insert-from-select write;
//   - not a PK-CHANGING update (SET touches the PK column → the old PK can't be recovered from RETURNING
//     without a pre-write SELECT, which decision #3 forbids on PG/SQLite → coarse);
//   - for a write with NO caller `.returning()`, the plain driver result must be FAITHFULLY reconstructible
//     from a hidden full RETURNING (verified: PGlite → `{rows:[],fields:[],affectedRows:N}`); other drivers
//     + SQLite (its `lastInsertRowid` is not recoverable for update/delete) → coarse. A caller's own
//     `.returning()` needs no reconstruction, but must cover every column to yield a full image, else coarse.

type Op = 'insert' | 'update' | 'delete'
/** Where a captured batch goes: straight to the db's graphs (autocommit) or a transaction buffer. */
type CaptureSink = (changes: TableChange[]) => void

function captureMutation(
  op: Op,
  baseMethod: (...a: unknown[]) => unknown,
  db: object,
  emit?: CaptureSink,
): (...a: unknown[]) => unknown {
  const sink: CaptureSink = emit ?? ((changes) => ingestWrite(db, { changes }))
  return (...args: unknown[]) => {
    const table = args[0]
    // insert/update/delete all take the target table as their first argument.
    if (!isTable(table)) return baseMethod(...args)
    return wrapWrite(baseMethod(...args), table, op, db, sink)
  }
}

/** Wrap a mutation builder so its terminal (`await` / `.execute()`) runs the write and captures its change;
 *  chain methods (`values`/`set`/`where`/`returning`/…) re-wrap so the terminal stays captured. */
function wrapWrite(builder: unknown, table: Table, op: Op, db: object, sink: CaptureSink): unknown {
  return new Proxy(builder as object, {
    get(target, prop, receiver) {
      if (prop === 'then') {
        return (onFulfilled?: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) =>
          runWrite(target, table, op, db, sink).then(onFulfilled, onRejected)
      }
      if (prop === 'execute') {
        return (...args: unknown[]) => runWrite(target, table, op, db, sink, args)
      }
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        const next = (value as (...a: unknown[]) => unknown).apply(target, args)
        return isWriteBuilder(next) ? wrapWrite(next, table, op, db, sink) : next
      }
    },
  })
}

async function runWrite(
  builder: unknown,
  table: Table,
  op: Op,
  db: object,
  sink: CaptureSink,
  executeArgs?: unknown[],
): Promise<unknown> {
  const tableName = getTableName(table)
  const plan = planCapture(builder, table, op, db)

  if (plan.mode === 'coarse') {
    const result = await runBase(builder, executeArgs) // run the caller's write untouched
    emitSafely(sink, [{ table: tableName, kind: 'coarse' }]) // fail-closed: over-invalidate, never guess
    return result
  }

  if (plan.callerReturning) {
    // The caller asked for rows — run their write, and capture only if the rows carry the FULL image.
    const rows = (await runBase(builder, executeArgs)) as Row[]
    const changes = coversAllColumns(rows, plan.columns) ? changesOf(op, tableName, rows, plan.pk) : [coarse(tableName)]
    emitSafely(sink, changes)
    return rows
  }

  // No caller `.returning()`: run a HIDDEN full-row returning to capture, and hand the caller back the plain
  // driver result reconstructed from it (verified faithful for this driver).
  const full = (builder as { returning: () => unknown }).returning()
  const rows = (await runBase(full, executeArgs)) as Row[]
  emitSafely(sink, changesOf(op, tableName, rows, plan.pk))
  return plan.reconstruct(rows)
}

// ── capture-fault isolation ─────────────────────────────────────────

/** Emit captured changes WITHOUT ever failing the caller's write. By the time we get here the DATABASE HAS
 *  ALREADY COMMITTED, so a sink/router/transport fault must never turn a committed write into a caller-visible
 *  rejection — the caller's result and failure behaviour stay exactly plain Drizzle's. A precise feed that
 *  throws part-way is retried ONCE as a single coarse marker, so the graphs end up soundly over-fired rather
 *  than half-applied; if even that fails the fault is reported and dropped (live queries on the table may be
 *  stale until the next write, which is the honest degradation). */
function emitSafely(sink: CaptureSink, changes: TableChange[]): void {
  try {
    sink(changes)
  } catch (error) {
    const tables = [...new Set(changes.map((change) => change.table))]
    reportCaptureFault(error, tables.join(', '))
    if (changes.every((change) => change.kind === 'coarse')) return // the coarse fallback itself failed
    try {
      sink(tables.map(coarse)) // degrade: coarsen EVERY touched table rather than leave a feed half-applied
    } catch (fallbackError) {
      reportCaptureFault(fallbackError, tables.join(', '))
    }
  }
}

/** The diagnostics seam for a contained capture fault. */
function reportCaptureFault(error: unknown, tableName: string): void {
  console.error(
    `[telefunc] live write-capture failed for table "${tableName}". The write COMMITTED and its result is unaffected; live queries on this table may be stale until the next write.`,
    error,
  )
}

// ── capture planning ────────────────────────────────────────────────

type Plan =
  | { mode: 'coarse' }
  | { mode: 'precise'; callerReturning: true; pk: string[]; columns: string[] }
  | {
      mode: 'precise'
      callerReturning: false
      pk: string[]
      columns: string[]
      reconstruct: (rows: Row[]) => unknown
    }

/** Decide precise vs coarse for one write — every ambiguity fails closed to coarse. */
function planCapture(builder: unknown, table: Table, op: Op, db: object): Plan {
  const dialect = dialectOf(db)
  if (dialect === 'mysql') return { mode: 'coarse' } // no RETURNING; precise MySQL (pre-write SELECT) deferred
  if (!isSingleSession(db)) return { mode: 'coarse' } // decision #6: pooled → coarse
  const pk = pkFieldsOf(table)
  if (pk.length === 0) return { mode: 'coarse' } // no resolvable PK → a retraction can't be keyed → coarse
  const config = writeConfigOf(builder)
  if (!config) return { mode: 'coarse' } // unrecognized builder shape (version drift) → coarse
  if (hasOnConflict(config, dialect)) return { mode: 'coarse' } // UPSERT / ON CONFLICT
  if (op === 'insert' && isRawInsert(config)) return { mode: 'coarse' } // raw-SQL values / insert-from-select
  if (op === 'update' && setTouchesPk(config, pk)) return { mode: 'coarse' } // PK-changing update (fork #2)

  const columns = Object.keys(getTableColumns(table))
  if (config.returning !== undefined) return { mode: 'precise', callerReturning: true, pk, columns }

  // no caller returning → reconstruction must be provably faithful for THIS driver
  if (dialect === 'pg' && driverOf(db) === 'PgliteDatabase') {
    return {
      mode: 'precise',
      callerReturning: false,
      pk,
      columns,
      // PGlite's plain no-returning result is `{ rows: [], fields: [], affectedRows: N }`; affectedRows =
      // the RETURNING row count. Verified empirically against PGlite 18.3.
      reconstruct: (rows) => ({ rows: [], fields: [], affectedRows: rows.length }),
    }
  }
  // SQLite's `lastInsertRowid` is not recoverable for update/delete, and other drivers are unverified → coarse.
  return { mode: 'coarse' }
}

// ── change construction ─────────────────────────────────────────────

function changesOf(op: Op, table: string, rows: Row[], pk: string[]): TableChange[] {
  return rows.map((row) => {
    if (op === 'insert') return { table, kind: 'insert', new: row }
    if (op === 'delete') return { table, kind: 'delete', key: keyOf(row, pk) }
    return { table, kind: 'update', new: row, key: keyOf(row, pk) }
  })
}

function keyOf(row: Row, pk: string[]): Row {
  const key: Row = {}
  for (const field of pk) key[field] = row[field]
  return key
}

const coarse = (table: string): TableChange => ({ table, kind: 'coarse' })

/** Whether the returned rows carry every column (a full image). Zero rows = nothing to capture (precise). */
function coversAllColumns(rows: Row[], columns: string[]): boolean {
  return rows.length === 0 || columns.every((c) => c in rows[0]!)
}

// ── builder introspection (version-brittle, guarded — mirrors drizzleShape.ts) ───────────────────────

/** The PK columns' FIELD names (the keys `.returning()` rows use) — single OR composite. `primaryKeyOf`
 *  resolves the PK's DATABASE column names (composite keys live in the table's extra-config builder); those
 *  are translated back to field names via the table's column map, since a `.returning()` row is keyed by
 *  field, not db column. A PK column that doesn't resolve to a field (or no PK at all) yields `[]` → the
 *  caller coarsens (fail-closed). */
function pkFieldsOf(table: Table): string[] {
  const columnNames = primaryKeyOf(table)
  if (columnNames.length === 0) return []
  const fieldByColumnName = new Map<string, string>()
  for (const [field, column] of Object.entries(getTableColumns(table))) {
    fieldByColumnName.set((column as Column).name, field)
  }
  const fields = columnNames.map((name) => fieldByColumnName.get(name))
  return fields.every((field): field is string => field !== undefined) ? (fields as string[]) : []
}

type WriteConfig = {
  values?: unknown
  set?: Record<string, unknown>
  onConflict?: unknown
  returning?: unknown
  select?: unknown
}

/** Read a write builder's `config` (typed-protected on PG/MySQL, runtime-only on SQLite — cast either way),
 *  or `null` when the shape isn't the pinned one → caller falls back to coarse. */
function writeConfigOf(builder: unknown): WriteConfig | null {
  const config = (builder as { config?: unknown }).config
  return config !== null && typeof config === 'object' ? (config as WriteConfig) : null
}

/** ON CONFLICT / UPSERT: PG/MySQL carry a single `SQL`; SQLite a non-empty `SQL[]`. */
function hasOnConflict(config: WriteConfig, dialect: 'pg' | 'sqlite'): boolean {
  if (dialect === 'sqlite') return Array.isArray(config.onConflict) && config.onConflict.length > 0
  return config.onConflict !== undefined
}

/** A fully-raw values clause (`insert(t).values(sql\`…\`)`) or an insert-from-SELECT — capture can't map it
 *  to per-row images, so coarsen. Per-column raw values are fine (the RETURNING row is still the real row). */
function isRawInsert(config: WriteConfig): boolean {
  return is(config.values, SQL) || config.select === true
}

/** Whether an UPDATE's SET assigns any PK column — a PK-changing update, whose old PK a RETURNING (new)
 *  can't recover on PG/SQLite (fork #2 → coarse). */
function setTouchesPk(config: WriteConfig, pk: string[]): boolean {
  return config.set !== undefined && pk.some((field) => field in config.set!)
}

// ── terminal execution ──────────────────────────────────────────────

function runBase(builder: unknown, executeArgs?: unknown[]): Promise<unknown> {
  return executeArgs
    ? (builder as { execute: (...a: unknown[]) => Promise<unknown> }).execute(...executeArgs)
    : Promise.resolve(builder as PromiseLike<unknown>)
}

/** A drizzle write query builder, distinguished from a plain method return (a `toSQL()` object, a Promise). */
function isWriteBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}
