export { captureNodeSqliteFile, wrappedFile }

import { builtin } from '../builtin.js'
import type { LiveDatabase, LiveOptions } from '../liveDatabase.js'
import { processWide } from '../processWide.js'
import { captureSyncSqlite, type RowsKind } from './syncSqlite.js'

// The parts of node:sqlite used here.
type Prepared = {
  all(...params: unknown[]): unknown[]
  get(...params: unknown[]): unknown
  run(...params: unknown[]): unknown
  iterate(...params: unknown[]): Iterator<unknown>
  setReturnArrays(on: boolean): void
  setReadBigInts(on: boolean): void
}
type DatabaseSync = {
  prepare(text: string, ...options: unknown[]): Prepared
  exec(text: string): void
  location(): string | null
  readonly isOpen: boolean
  readonly isTransaction: boolean
}
type Wrapped = { live: LiveDatabase; options: LiveOptions }
type Capture = ReturnType<typeof captureConnection>

/** The live database of each file whose connections are captured, by the path `location()` gives. */
const files = processWide('nodeSqliteFiles', () => new Map<string, Wrapped>())
/** The connections seen on none of `files`, since a file was last wrapped. */
const unwrapped = processWide('nodeSqliteUnwrapped', () => ({ connections: new WeakSet<object>() }))

/** The live database following the connections on `path`, if one does. */
function wrappedFile(path: string): Wrapped | undefined {
  return files.get(locationOf(path))
}

/**
 * Captures every node:sqlite connection on the file at `path`, from now on, whoever opens it: an ORM may open its own
 * and keep them (Prisma opens one per transaction). `DatabaseSync.prototype` is hooked once per process; a connection
 * on another file runs as before.
 */
function captureNodeSqliteFile(path: string, wrapped: Wrapped): void {
  files.set(locationOf(path), wrapped)
  // A connection already open on the file is captured from its next statement.
  unwrapped.connections = new WeakSet()
  processWide('nodeSqliteHook', hookDatabaseSync)
}

function hookDatabaseSync(): true {
  const { DatabaseSync } = builtin('node:sqlite') as { DatabaseSync: { prototype: DatabaseSync } }
  const proto = DatabaseSync.prototype
  const { prepare, exec } = proto
  /** The capture of each connection on a file a live database follows, made at its first statement. */
  const connections = new WeakMap<DatabaseSync, Capture>()
  const captureOf = (db: DatabaseSync): Capture | null => {
    // A closed connection fails as it would.
    if (!db.isOpen || unwrapped.connections.has(db)) return null
    let capture = connections.get(db)
    if (capture) return capture
    const wrapped = files.get(db.location() ?? '')
    if (!wrapped) {
      unwrapped.connections.add(db)
      return null
    }
    capture = captureConnection(db, wrapped.live, prepare, exec)
    connections.set(db, capture)
    return capture
  }
  proto.prepare = function (this: DatabaseSync, text: string, ...options: unknown[]) {
    const capture = captureOf(this)
    const prepared = prepare.call(this, text, ...options)
    return capture ? capture.statement(prepared, text) : prepared
  }
  proto.exec = function (this: DatabaseSync, text: string) {
    const capture = captureOf(this)
    if (!capture) return exec.call(this, text)
    capture.exec(text, [], () => exec.call(this, text))
  }
  return true
}

/** Captures `db`, one connection; `prepare` and `exec` are node:sqlite's own. Transactions are its `exec('BEGIN')`. */
function captureConnection(
  db: DatabaseSync,
  live: LiveDatabase,
  prepare: DatabaseSync['prepare'],
  exec: DatabaseSync['exec'],
) {
  const ours = (text: string) => prepare.call(db, text)
  // How the connection returns rows unless a statement says otherwise: `new DatabaseSync(path, options)` chose.
  const sample = ours('SELECT 1 AS "x"').get() as Record<string, unknown> | unknown[]
  const defaults = { arrays: Array.isArray(sample), bigInts: typeof Object.values(sample)[0] === 'bigint' }
  const capture = captureSyncSqlite(
    {
      db,
      inTransaction: () => db.isTransaction,
      rows: (text, values) => {
        const prepared = ours(text)
        prepared.setReturnArrays(true)
        return prepared.all(...values) as (string | null)[][]
      },
      exec: (text) => exec.call(db, text),
    },
    live,
  )
  return {
    exec: capture.exec,
    /** `prepared`, the app's statement of `text`, with its methods captured. */
    statement(prepared: Prepared, text: string): Prepared {
      const modes = { ...defaults }
      const setReturnArrays = prepared.setReturnArrays.bind(prepared)
      const setReadBigInts = prepared.setReadBigInts.bind(prepared)
      prepared.setReturnArrays = (on) => {
        setReturnArrays(on)
        modes.arrays = on
      }
      prepared.setReadBigInts = (on) => {
        setReadBigInts(on)
        modes.bigInts = on
      }
      const statement = {
        text,
        prepare: (text: string) => {
          const sent = ours(text)
          sent.setReturnArrays(modes.arrays)
          sent.setReadBigInts(modes.bigInts)
          return sent as never
        },
      }
      const rows = (): RowsKind => (modes.arrays ? 'arrays' : 'objects')
      const all = prepared.all.bind(prepared)
      prepared.all = capture.method(statement, all, 'all', rows) as Prepared['all']
      prepared.get = capture.method(statement, prepared.get.bind(prepared), 'get', rows)
      prepared.run = capture.method(statement, prepared.run.bind(prepared), 'run', () => 'other')
      prepared.iterate = capture.iterate(statement, all, rows)
      return prepared
    },
  }
}

/** The path SQLite's `location()` gives for `path`: absolute, its links resolved (the file may not exist yet). */
function locationOf(path: string): string {
  const { realpathSync } = builtin('node:fs')
  const { basename, dirname, join, resolve } = builtin('node:path')
  const absolute = resolve(path)
  try {
    return realpathSync(absolute)
  } catch {}
  try {
    return join(realpathSync(dirname(absolute)), basename(absolute))
  } catch {
    return absolute
  }
}
