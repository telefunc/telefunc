export { captureSyncSqlite }
export type { RowsKind }

import { assertUsage } from '../assert.js'
import type { FollowUp, LiveDatabase, Statement } from '../liveDatabase.js'
import { paramValues } from '../sqlite/dialect.js'
import { takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

/** A SQLite driver that answers at once on its one connection (better-sqlite3, bun:sqlite), as its capture sees it. */
type SyncSqlite = {
  readonly db: object
  readonly inTransaction: () => boolean
  /** Runs our statement and returns its rows as arrays of text. */
  readonly rows: (text: string, values: unknown[]) => (string | null)[][]
  readonly exec: (text: string) => void
}

/** A statement the app prepared. */
type SyncStatement = {
  readonly text: string
  /** Prepares `text` (ours) in the modes the app's statement is in. */
  readonly prepare: (text: string) => Record<string, (...params: unknown[]) => unknown>
}

/** How a method returns rows: as objects, as arrays, or in a form whose last columns can't be taken (one value per
 *  row, nested by table, as bytes, or as they're read). */
type RowsKind = 'objects' | 'arrays' | 'other'

/** Captures a synchronous SQLite driver; its catalog loads now: the driver answers at once, so no statement waits. */
function captureSyncSqlite(driver: SyncSqlite, live: LiveDatabase) {
  assertUsage(!driver.inTransaction(), 'Wrap the SQLite database outside of a transaction.')
  const { db } = driver
  const ready = () => {
    for (const statement of live.readySync(db, (text) => driver.rows(text, [])[0]![0]!)) driver.exec(statement)
  }
  const runFollowUp = ({ text, values, done }: FollowUp) => {
    let rows: (string | null)[][] | undefined
    try {
      rows = driver.rows(text, values)
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
      live.transactionIs(db, driver.inTransaction(), 'rollback')
      throw err
    }
    const followUps = statement.succeeded(hiddenRows(result))
    live.transactionIs(db, driver.inTransaction(), 'commit')
    for (const followUp of followUps) runFollowUp(followUp)
    return result
  }
  /** A transaction statement of ours, for one the driver runs without a statement of the app's: told as it runs. */
  const control = (text: string) => void live.statementFor(db, text, []).succeeded([])
  /** The app's statement method `name`, which returns rows as `rowsKind()` says when called. */
  const method =
    (statement: SyncStatement, native: (...params: unknown[]) => unknown, name: string, rowsKind: () => RowsKind) =>
    (...params: unknown[]) => {
      ready()
      const values = valuesOf(statement.text, params)
      const rows = rowsKind()
      const rewrite = values !== undefined && rows !== 'other'
      const planned = live.statementFor(db, statement.text, values ?? [], { rewrite })
      if (planned.text === statement.text) return run(planned, () => native(...params))
      const sent = statement.prepare(planned.text)
      const hidden = Array.from({ length: planned.hiddenColumns }, (_, i) => `${HIDDEN_PREFIX}${i}`)
      const hiddenRows = (result: unknown) => {
        const list = Array.isArray(result) ? result : result === undefined || result === null ? [] : [result]
        const taken = takeHiddenValues(list, hidden, rows === 'arrays')
        if (!planned.returnsRows) list.length = 0
        return taken
      }
      return run(planned, () => sent[name]!(...(planned.values ?? values!)), hiddenRows)
    }
  ready()

  return {
    method,
    /** The app's statement method `iterate`, run to its end at once through its `all`: a write's changes are logged
     *  when it returns, not once its rows are read. */
    iterate(statement: SyncStatement, all: (...params: unknown[]) => unknown[], rowsKind: () => RowsKind) {
      const runAll = method(statement, all, 'all', rowsKind)
      return (...params: unknown[]) => (runAll(...params) as unknown[])[Symbol.iterator]()
    },
    /** Text that may hold several statements, run as written with the app's `params`. */
    exec(text: string, params: unknown[], send: () => unknown) {
      ready()
      return run(live.statementFor(db, text, valuesOf(text, params) ?? [], { rewrite: false }), send)
    },
    /** A transaction function: it runs `BEGIN` (or a savepoint, inside another) and its end past the statements the
     *  capture sees. */
    transaction(call: (...args: unknown[]) => unknown) {
      return (...args: unknown[]) => {
        const nested = driver.inTransaction()
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
    },
  }
}

/** The values of `text`'s parameters by number, from the app's arguments: an array among them is spread, an object
 *  holds the named ones (`paramValues()`). */
function valuesOf(text: string, params: unknown[]): unknown[] | undefined {
  const anonymous: unknown[] = []
  let named: Record<string, unknown> | undefined
  for (const param of params) {
    if (Array.isArray(param)) anonymous.push(...param)
    else if (param !== null && typeof param === 'object' && Object.getPrototypeOf(param) === Object.prototype)
      named = param as Record<string, unknown>
    else anonymous.push(param)
  }
  return named ? paramValues(text, anonymous, named) : anonymous
}
