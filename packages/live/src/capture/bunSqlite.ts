export { captureBunSqlite, isBunSqlite }
export type { BunSqlite }

import type { LiveDatabase } from '../liveDatabase.js'
import { captureSyncSqlite, type RowsKind } from './syncSqlite.js'

// The parts of bun:sqlite used here.
type Prepared = {
  run(...params: unknown[]): unknown
  get(...params: unknown[]): unknown
  all(...params: unknown[]): unknown[]
  values(...params: unknown[]): unknown[][]
  raw(...params: unknown[]): unknown[][]
  iterate(...params: unknown[]): IterableIterator<unknown>
  as(Class: new () => unknown): Prepared
  safeIntegers(toggle?: boolean): Prepared
}
type TransactionFunction = ((...args: unknown[]) => unknown) & Record<Behavior, (...args: unknown[]) => unknown>
type Behavior = 'deferred' | 'immediate' | 'exclusive'
type BunSqlite = {
  prepare(text: string, ...params: unknown[]): Prepared
  query(text: string): Prepared
  run(text: string, ...params: unknown[]): unknown
  exec(text: string, ...params: unknown[]): unknown
  transaction(fn: (...args: unknown[]) => unknown): TransactionFunction
  readonly inTransaction: boolean
}

const CAPTURED = Symbol.for('@telefunc/live/captured')
const BEHAVIORS: Behavior[] = ['deferred', 'immediate', 'exclusive']

function isBunSqlite(client: object): client is BunSqlite {
  return 'query' in client && 'prepare' in client && 'transaction' in client && 'fileControl' in client
}

/** Captures every statement of a bun:sqlite database, which is its one connection. */
function captureBunSqlite(db: BunSqlite, live: LiveDatabase): void {
  if (CAPTURED in db) return
  const prepare = db.prepare.bind(db)
  const query = db.query.bind(db)
  const run = db.run.bind(db)
  const transaction = db.transaction.bind(db)
  const capture = captureSyncSqlite(
    {
      db,
      inTransaction: () => db.inTransaction,
      rows: (text, values) => prepare(text).values(...values) as (string | null)[][],
      exec: (text) => run(text),
    },
    live,
  )
  Object.defineProperty(db, CAPTURED, { value: true, configurable: true })

  const captured = (prepared: Prepared, text: string) => {
    // `query()` returns the statement it cached for the same text.
    if (CAPTURED in prepared) return prepared
    Object.defineProperty(prepared, CAPTURED, { value: true })
    const modes: { as?: new () => unknown; safeIntegers: boolean } = { safeIntegers: false }
    const as = prepared.as.bind(prepared)
    const safeIntegers = prepared.safeIntegers.bind(prepared)
    prepared.as = (Class) => {
      modes.as = Class
      return as(Class)
    }
    prepared.safeIntegers = (on = true) => {
      modes.safeIntegers = on
      return safeIntegers(on)
    }
    const statement = {
      text,
      prepare: (ours: string) => {
        const sent = prepare(ours).safeIntegers(modes.safeIntegers)
        return (modes.as ? sent.as(modes.as) : sent) as never
      },
    }
    // Rows come back as bytes (`raw`) or as they're read (`iterate`): no hidden columns.
    const method = (name: keyof Prepared, rows: RowsKind) =>
      capture.method(statement, (prepared[name] as (...params: unknown[]) => unknown).bind(prepared), name, () => rows)
    prepared.all = method('all', 'objects') as Prepared['all']
    prepared.get = method('get', 'objects')
    prepared.values = method('values', 'arrays') as Prepared['values']
    prepared.raw = method('raw', 'other') as Prepared['raw']
    prepared.iterate = method('iterate', 'other') as Prepared['iterate']
    prepared.run = method('run', 'other')
    return prepared
  }
  db.prepare = (text, ...params) => captured(prepare(text, ...params), text)
  db.query = (text) => captured(query(text), text)
  db.run = db.exec = (text, ...params) => capture.exec(text, params, () => run(text, ...params))

  // `db.transaction(fn)` runs `BEGIN` (or a savepoint, inside another) and its end past `prepare()`.
  db.transaction = (fn) => {
    const native = transaction(fn)
    const wrapped = capture.transaction(native) as TransactionFunction
    for (const behavior of BEHAVIORS) wrapped[behavior] = capture.transaction(native[behavior])
    return wrapped
  }
}
