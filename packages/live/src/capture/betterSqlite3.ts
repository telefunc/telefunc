export { captureBetterSqlite3, isBetterSqlite3 }
export type { BetterSqlite3 }

import { assertUsage } from '../assert.js'
import type { FollowUp, LiveDatabase, Statement } from '../liveDatabase.js'
import { takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

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
  readonly reader: boolean
}
type TransactionFunction = ((...args: unknown[]) => unknown) & Record<Behavior, (...args: unknown[]) => unknown>
type Behavior = 'default' | 'deferred' | 'immediate' | 'exclusive'
type BetterSqlite3 = {
  prepare(text: string): Prepared
  exec(text: string): unknown
  transaction(fn: (...args: unknown[]) => unknown): TransactionFunction
  readonly inTransaction: boolean
  readonly memory: boolean
}

const CAPTURED = Symbol.for('@telefunc/live/captured')
const BEHAVIORS: Behavior[] = ['default', 'deferred', 'immediate', 'exclusive']

/** Modes a prepared statement returns its rows in. */
type Modes = { raw: boolean; pluck: boolean; expand: boolean; safeIntegers: boolean }

function isBetterSqlite3(client: object): client is BetterSqlite3 {
  return 'prepare' in client && 'transaction' in client && 'inTransaction' in client && 'memory' in client
}

/** Captures every statement of a better-sqlite3 database, which is its one connection. Its catalog loads now: the
 *  driver answers at once, so no statement waits for it. */
function captureBetterSqlite3(db: BetterSqlite3, live: LiveDatabase): void {
  if (CAPTURED in db) return
  assertUsage(!db.inTransaction, 'Wrap the better-sqlite3 database outside of a transaction.')
  Object.defineProperty(db, CAPTURED, { value: true, configurable: true })
  const prepare = db.prepare.bind(db)
  const exec = db.exec.bind(db)
  const transaction = db.transaction.bind(db)
  const ready = () => {
    if (live.isReady()) return
    for (const statement of live.readySync((text) => prepare(text).pluck().get() as string)) exec(statement)
  }
  const runFollowUp = ({ text, values, done }: FollowUp) => {
    let rows: (string | null)[][] | undefined
    try {
      rows = prepare(text)
        .raw()
        .all(...values) as (string | null)[][]
    } catch {
      rows = undefined
    }
    done(rows)
  }
  /** Runs a statement of the app's through `send` once `statement` planned it, then what it needs next. */
  const run = <T>(statement: Statement, send: () => T, hiddenRows: (result: T) => (string | null)[][] = () => []) => {
    let result: T
    try {
      result = send()
    } catch (err) {
      statement.failed()
      throw err
    }
    for (const followUp of statement.succeeded(hiddenRows(result))) runFollowUp(followUp)
    return result
  }
  /** A transaction statement of ours, for one better-sqlite3 runs without a statement of the app's. */
  const control = (text: string) => run(live.statementFor(db, text, []), () => undefined)
  ready()

  db.prepare = (text: string) => {
    const prepared = prepare(text)
    const modes: Modes = { raw: false, pluck: false, expand: false, safeIntegers: false }
    for (const mode of ['raw', 'pluck', 'expand', 'safeIntegers'] as const) {
      const toggle = prepared[mode].bind(prepared)
      prepared[mode] = (on = true) => {
        modes[mode] = on
        toggle(on)
        return prepared
      }
    }
    const execute = (method: 'all' | 'get' | 'run' | 'iterate') => {
      const native = (prepared[method] as (...params: unknown[]) => unknown).bind(prepared)
      return (...params: unknown[]) => {
        ready()
        const values = positional(params)
        // Rows come back one column (`pluck`), nested (`expand`) or as they're read (`iterate`): no hidden columns.
        const rewrite = values !== undefined && (method === 'all' || method === 'get') && !modes.pluck && !modes.expand
        const statement = live.statementFor(db, text, values ?? [], { rewrite })
        if (statement.text === text) return run(statement, () => native(...params))
        const sent = withModes(prepare(statement.text), modes)
        const hidden = Array.from({ length: statement.hiddenColumns }, (_, i) => `${HIDDEN_PREFIX}${i}`)
        const rows = (result: unknown) => {
          const list = method === 'all' ? (result as unknown[]) : result === undefined ? [] : [result]
          const hiddenRows = takeHiddenValues(list, hidden, modes.raw)
          if (!statement.returnsRows) list.length = 0
          return hiddenRows
        }
        return run(statement, () => sent[method](...(statement.values ?? values!)), rows)
      }
    }
    prepared.all = execute('all') as Prepared['all']
    prepared.get = execute('get')
    prepared.run = execute('run')
    prepared.iterate = execute('iterate') as Prepared['iterate']
    return prepared
  }

  db.exec = (text: string) => {
    ready()
    // Several statements, possibly: run as written.
    const statement = live.statementFor(db, text, [], { rewrite: false })
    run(statement, () => exec(text))
    return db
  }

  // `db.transaction(fn)` runs `BEGIN` (or a savepoint, inside another) and its end past `prepare()`.
  db.transaction = (fn) => {
    const native = transaction(fn)
    const door =
      (call: (...args: unknown[]) => unknown) =>
      (...args: unknown[]) => {
        const nested = db.inTransaction
        control(nested ? 'SAVEPOINT "__live"' : 'BEGIN')
        let result: unknown
        try {
          result = call(...args)
        } catch (err) {
          if (nested) control('ROLLBACK TO "__live"')
          control(nested ? 'RELEASE "__live"' : 'ROLLBACK')
          throw err
        }
        control(nested ? 'RELEASE "__live"' : 'COMMIT')
        return result
      }
    const wrapped = door(native) as TransactionFunction
    for (const behavior of BEHAVIORS) wrapped[behavior] = door(native[behavior])
    return wrapped
  }
}

/** The values bound to `?` in order: better-sqlite3 spreads arrays among its arguments. `undefined` for named
 *  parameters (an object). */
function positional(params: unknown[]): unknown[] | undefined {
  const values: unknown[] = []
  for (const param of params) {
    if (Array.isArray(param)) values.push(...param)
    else if (param !== null && typeof param === 'object' && Object.getPrototypeOf(param) === Object.prototype)
      return undefined
    else values.push(param)
  }
  return values
}

function withModes(prepared: Prepared, modes: Modes): Prepared {
  return prepared.raw(modes.raw).safeIntegers(modes.safeIntegers)
}
