export { captureBetterSqlite3, isBetterSqlite3 }
export type { BetterSqlite3 }

import type { LiveDatabase } from '../liveDatabase.js'
import { captureSyncSqlite, type RowsKind } from './syncSqlite.js'

// The parts of better-sqlite3 used here, so that `better-sqlite3` stays the app's dependency.
type Prepared = {
  run(...params: unknown[]): unknown
  get(...params: unknown[]): unknown
  all(...params: unknown[]): unknown[]
  iterate(...params: unknown[]): IterableIterator<unknown>
  raw(toggle?: boolean): Prepared
  pluck(toggle?: boolean): Prepared
  expand(toggle?: boolean): Prepared
  safeIntegers(toggle?: boolean): Prepared
}
type TransactionFunction = ((...args: unknown[]) => unknown) & Record<Behavior, (...args: unknown[]) => unknown>
type Behavior = 'default' | 'deferred' | 'immediate' | 'exclusive'
type BetterSqlite3 = {
  prepare(text: string): Prepared
  exec(text: string): unknown
  transaction(fn: (...args: unknown[]) => unknown): TransactionFunction
  defaultSafeIntegers(toggle?: boolean): BetterSqlite3
  readonly inTransaction: boolean
  readonly memory: boolean
}

const CAPTURED = Symbol.for('@telefunc/live/captured')
const BEHAVIORS: Behavior[] = ['default', 'deferred', 'immediate', 'exclusive']
const MODES = ['raw', 'pluck', 'expand', 'safeIntegers'] as const

function isBetterSqlite3(client: object): client is BetterSqlite3 {
  return 'prepare' in client && 'transaction' in client && 'inTransaction' in client && 'memory' in client
}

/** Captures every statement of a better-sqlite3 database, which is its one connection. */
function captureBetterSqlite3(db: BetterSqlite3, live: LiveDatabase): void {
  if (CAPTURED in db) return
  const prepare = db.prepare.bind(db)
  const exec = db.exec.bind(db)
  const transaction = db.transaction.bind(db)
  const defaultSafeIntegers = db.defaultSafeIntegers.bind(db)
  // The mode a statement gets as it's prepared: `new Database()` or `defaultSafeIntegers()` set it.
  let safeByDefault = typeof prepare('SELECT 1').pluck().get() === 'bigint'
  const capture = captureSyncSqlite(
    {
      db,
      inTransaction: () => db.inTransaction,
      rows: (text, values) =>
        prepare(text)
          .raw()
          .all(...values) as (string | null)[][],
      exec,
    },
    live,
  )
  Object.defineProperty(db, CAPTURED, { value: true, configurable: true })

  db.prepare = (text: string) => {
    const prepared = prepare(text)
    const modes = { raw: false, pluck: false, expand: false, safeIntegers: safeByDefault }
    for (const mode of MODES) {
      const toggle = prepared[mode].bind(prepared)
      prepared[mode] = (on = true) => {
        modes[mode] = on
        toggle(on)
        return prepared
      }
    }
    const statement = {
      text,
      prepare: (ours: string) => prepare(ours).raw(modes.raw).safeIntegers(modes.safeIntegers) as never,
    }
    // Rows come back one column (`pluck`) or nested (`expand`): no hidden columns.
    const rows = (): RowsKind => (modes.pluck || modes.expand ? 'other' : modes.raw ? 'arrays' : 'objects')
    prepared.all = capture.method(statement, prepared.all.bind(prepared), 'all', rows) as Prepared['all']
    prepared.get = capture.method(statement, prepared.get.bind(prepared), 'get', rows)
    prepared.run = capture.method(statement, prepared.run.bind(prepared), 'run', () => 'other')
    prepared.iterate = capture.iterate(statement, prepared.iterate.bind(prepared), rows) as Prepared['iterate']
    return prepared
  }

  db.defaultSafeIntegers = (on = true) => {
    safeByDefault = on
    defaultSafeIntegers(on)
    return db
  }

  db.exec = (text: string) => {
    capture.exec(text, [], exec)
    return db
  }

  // `db.transaction(fn)` runs `BEGIN` (or a savepoint, inside another) and its end past `prepare()`.
  db.transaction = (fn) => {
    const native = transaction(fn)
    const wrapped = capture.transaction(native) as TransactionFunction
    for (const behavior of BEHAVIORS) wrapped[behavior] = capture.transaction(native[behavior])
    return wrapped
  }
}
