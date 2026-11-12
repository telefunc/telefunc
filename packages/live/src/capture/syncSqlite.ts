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
  /** Sends a statement of the app's through `send` once `statement` planned it; what it needs next if it failed. */
  const sent = <T>(statement: Statement, send: () => T): T => {
    if (statement.readsFirst) runFollowUp(statement.readsFirst)
    try {
      return send()
    } catch (err) {
      settle(statement.failed(), 'rollback')
      throw err
    }
  }
  /** Runs a statement of the app's through `send` once `statement` planned it, then what it needs next. */
  const run = <T>(statement: Statement, send: () => T, hiddenRows: (result: T) => (string | null)[][] = () => []) => {
    const result = sent(statement, send)
    settle(statement.succeeded(hiddenRows(result)), 'commit')
    return result
  }
  /** After a statement of the app's: the transaction the driver is in, then the queries of ours the statement needs. */
  const settle = (followUps: FollowUp[], ended: 'commit' | 'rollback') => {
    live.transactionIs(db, driver.inTransaction(), ended)
    for (const followUp of followUps) runFollowUp(followUp)
  }
  /** A transaction statement of ours, for one the driver runs without a statement of the app's: told as it runs. */
  const control = (text: string) => {
    live.statementFor(db, text, []).succeeded([])
  }
  /** Plans a statement of the app's, called with `params` through a method that returns rows as `rows`. */
  const plan = (statement: SyncStatement, params: unknown[], rows: RowsKind) => {
    ready()
    const values = valuesOf(statement.text, params)
    const rewrite = values !== undefined && rows !== 'other'
    const planned = live.statementFor(db, statement.text, values ?? [], { rewrite })
    const hidden = Array.from({ length: planned.hiddenColumns }, (_, i) => `${HIDDEN_PREFIX}${i}`)
    return {
      planned,
      /** Sends what was planned, unless it's the app's statement, through our statement's method `name`. */
      sendOurs: (name: string) => statement.prepare(planned.text)[name]!(...(planned.values ?? values!)),
      /** Takes the hidden columns off `list`'s rows, and the rows too when the app's statement returns none. */
      takeHidden: (list: unknown[]) => {
        const taken = takeHiddenValues(list, hidden, rows === 'arrays')
        if (!planned.returnsRows) list.length = 0
        return taken
      },
    }
  }
  /** The app's statement method `name`, which returns rows as `rowsKind()` says when called. */
  const method =
    (statement: SyncStatement, native: (...params: unknown[]) => unknown, name: string, rowsKind: () => RowsKind) =>
    (...params: unknown[]) => {
      const { planned, sendOurs, takeHidden } = plan(statement, params, rowsKind())
      if (planned.text === statement.text) return run(planned, () => native(...params))
      const listOf = (result: unknown) =>
        Array.isArray(result) ? result : result === undefined || result === null ? [] : [result]
      return run(
        planned,
        () => sendOurs(name),
        (result) => takeHidden(listOf(result)),
      )
    }
  ready()

  return {
    method,
    /** The app's statement method `iterate`, which runs its statement as its rows are read: a read outside `.live()`
     *  gets the driver's own iterator. Any other statement is done once its iterator ran out, was closed or failed
     *  (a write's changes stay its own until then); its rows come without their hidden columns. */
    iterate(statement: SyncStatement, native: (...params: unknown[]) => Iterator<unknown>, rowsKind: () => RowsKind) {
      return (...params: unknown[]) => {
        const { planned, sendOurs, takeHidden } = plan(statement, params, rowsKind())
        if (planned.unwatched) return run(planned, () => native(...params))
        const rows = sent(planned, () =>
          planned.text === statement.text ? native(...params) : (sendOurs('iterate') as Iterator<unknown>),
        )
        const hiddenRows: (string | null)[][] = []
        return whenDone(rows, {
          row: (row) => hiddenRows.push(...takeHidden([row])),
          succeeded: () => settle(planned.succeeded(hiddenRows), 'commit'),
          failed: () => settle(planned.failed(), 'rollback'),
        })
      }
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

/** `iterator`, telling `done` of each row it gives, and once, of how it ended: it ran out, was closed or failed. */
function whenDone<T>(
  iterator: Iterator<T>,
  done: { row: (row: T) => void; succeeded: () => void; failed: () => void },
): Iterator<T> {
  let running = true
  const end = (then: () => void) => {
    if (!running) return
    running = false
    then()
  }
  const { next, return: close } = iterator
  iterator.next = (...args: [] | [unknown]) => {
    let result: IteratorResult<T>
    try {
      result = next.apply(iterator, args)
    } catch (err) {
      end(done.failed)
      throw err
    }
    if (result.done) end(done.succeeded)
    else done.row(result.value)
    return result
  }
  if (close)
    iterator.return = (value?: unknown) => {
      const result = close.call(iterator, value)
      end(done.succeeded)
      return result
    }
  return iterator
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
