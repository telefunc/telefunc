export { captureD1, isD1 }
export type { D1 }

import { assertUsage } from '../assert.js'
import type { FollowUp, LiveDatabase, Statement } from '../liveDatabase.js'
import { takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

// The parts of a D1 binding used here, so that `@cloudflare/workers-types` stays the app's dependency.
type D1Result = { results: unknown[]; [key: string]: unknown }
type D1Statement = {
  bind(...values: unknown[]): D1Statement
  first(column?: string): Promise<unknown>
  all(): Promise<D1Result>
  run(): Promise<D1Result>
  raw(options?: { columnNames?: boolean }): Promise<unknown[][]>
}
type D1Session = {
  prepare(text: string): D1Statement
  batch(statements: D1Statement[]): Promise<D1Result[]>
}
type D1 = D1Session & {
  exec(text: string): Promise<unknown>
  withSession?(...args: unknown[]): D1Session
}

const CAPTURED = Symbol.for('@telefunc/live/captured')

function isD1(client: object): client is D1 {
  return 'prepare' in client && 'batch' in client && 'exec' in client && !('transaction' in client)
}

/** Captures every statement of a D1 database. D1 runs each statement, and each `batch()`, as a transaction of its
 *  own, whichever request sends it: the database is the connection. */
function captureD1(db: D1, live: LiveDatabase): void {
  if (CAPTURED in db) return
  Object.defineProperty(db, CAPTURED, { value: true, configurable: true })
  const exec = db.exec.bind(db)
  const ready = captureSession(db, db, live)
  db.exec = async (text) => {
    await ready()
    // Several statements, possibly: run as written, and count every write as a change to its whole table.
    const statement = live.statementFor(db, text, [], { rewrite: false })
    let result: unknown
    try {
      result = await exec(text)
    } catch (err) {
      statement.failed()
      throw err
    }
    statement.succeeded([])
    return result
  }
  const withSession = db.withSession?.bind(db)
  if (withSession)
    db.withSession = (...args) => {
      const session = withSession(...args)
      captureSession(session, db, live)
      return session
    }
}

/** What a statement of ours was prepared with: the app's text and values. */
type Prepared = { text: string; values: unknown[] }

/** Captures `session`'s `prepare()` and `batch()`; returns how to load what capturing needs. */
function captureSession(session: D1Session, db: D1, live: LiveDatabase): () => Promise<void> {
  const prepare = session.prepare.bind(session)
  const batch = session.batch.bind(session)
  const prepared = new WeakMap<D1Statement, Prepared>()
  const ready = async () => {
    if (live.isReady()) return
    await live.ready(async (text) => (await prepare(text).raw())[0]![0] as string)
  }
  const runFollowUps = async (followUps: FollowUp[]) => {
    for (const { text, values, done } of followUps) {
      const rows = await prepare(text)
        .bind(...values)
        .raw()
        .catch(() => undefined)
      done(rows as (string | null)[][] | undefined)
    }
  }
  const ours = ({ text, values }: FollowUp) => prepare(text).bind(...values)
  /** The statement `planned` as sent: our text, with the values it takes. */
  const sent = (planned: Statement, values: unknown[]) => prepare(planned.text).bind(...(planned.values ?? values))
  /** Runs `planned` through `raw()`: its column names, then its rows as arrays. A query of ours it reads first goes in
   *  one `batch()` with it, whose rows come as objects: the statement is then sent with its columns aliased by
   *  position, which rebuild the arrays, and comes back without names (`batched`). */
  const rawRows = async (planned: Statement, values: unknown[]): Promise<{ rows: unknown[][]; batched: boolean }> => {
    const { readsFirst, positional } = planned
    if (!readsFirst || !positional)
      return { rows: await sent(planned, values).raw({ columnNames: true }), batched: false }
    const [first, result] = await batch([ours(readsFirst), prepare(positional.text).bind(...positional.values)])
    readsFirst.done(rowsOf(first!))
    const keys = [...positional.keys, ...hiddenNames(planned)]
    const rows = result!.results.map((row) => keys.map((key) => (row as Record<string, unknown>)[key]))
    return { rows: [hiddenNames(planned), ...rows], batched: true }
  }
  const send = async <T>(planned: Statement, request: () => Promise<T>): Promise<T> => {
    try {
      return await request()
    } catch (err) {
      planned.failed()
      throw err
    }
  }
  /** Runs `text` as `live` plans it, through `all()`; the caller's rows are objects. A query of ours it reads first
   *  goes in one `batch()` with it: a transaction. */
  const all = async (text: string, values: unknown[]): Promise<D1Result> => {
    await ready()
    const planned = live.statementFor(db, text, values)
    const result = await send(planned, async () => {
      if (!planned.readsFirst) return await sent(planned, values).all()
      const [first, result] = await batch([ours(planned.readsFirst), sent(planned, values)])
      planned.readsFirst.done(rowsOf(first!))
      return result!
    })
    const hidden = hiddenNames(planned)
    const hiddenRows = takeHiddenValues(result.results, hidden, false)
    await runFollowUps(planned.succeeded(hiddenRows))
    return planned.returnsRows ? result : { ...result, results: [] }
  }
  const statementOf = (text: string, values: unknown[]): D1Statement => {
    const statement: D1Statement = {
      bind: (...bound) => statementOf(text, bound),
      all: () => all(text, values),
      run: () => all(text, values),
      first: async (column) => {
        const row = ((await all(text, values)).results[0] ?? null) as Record<string, unknown> | null
        if (row === null || column === undefined) return row
        // D1's own error.
        if (!(column in row)) throw new Error(`D1_COLUMN_NOTFOUND: Column not found (${column})`)
        return row[column]
      },
      raw: async (options) => {
        await ready()
        const planned = live.statementFor(db, text, values)
        const { rows: answer, batched } = await send(planned, () => rawRows(planned, values))
        const [returned = [], ...rows] = answer
        const hiddenRows = takeHiddenValues(rows, hiddenNames(planned), true)
        await runFollowUps(planned.succeeded(hiddenRows))
        const kept = planned.returnsRows ? rows : []
        if (!options?.columnNames) return kept
        // Rows that came as objects came without names: SQLite's, from a query of ours reading no rows.
        const named = batched ? planned.positional!.names : undefined
        if (!named) return [returned.slice(0, returned.length - planned.hiddenColumns), ...kept]
        const [names = []] = await prepare(named.text)
          .bind(...named.values)
          .raw({ columnNames: true })
        return [names, ...kept]
      },
    }
    prepared.set(statement, { text, values })
    return statement
  }
  session.prepare = (text) => statementOf(text, [])

  // A batch is one transaction: told whole once it committed, so that no other request's statement lands inside it.
  session.batch = async (statements) => {
    await ready()
    const planned = statements.map((statement) => {
      const app = prepared.get(statement)
      assertUsage(app, 'Prepare the statements of a D1 batch() with the database live() wraps.')
      return { app, statement: live.statementFor(db, app.text, app.values) }
    })
    // Each statement's query of ours right before it.
    const sentStatements = planned.flatMap(({ app, statement }) => {
      const own = sent(statement, app.values)
      return statement.readsFirst ? [ours(statement.readsFirst), own] : [own]
    })
    let results: D1Result[]
    try {
      results = await batch(sentStatements)
    } catch (err) {
      // It rolled back whole.
      live.statementFor(db, 'BEGIN', []).succeeded([])
      for (const { statement } of planned) statement.failed()
      live.statementFor(db, 'ROLLBACK', []).succeeded([])
      throw err
    }
    const followUps: FollowUp[] = []
    live.statementFor(db, 'BEGIN', []).succeeded([])
    const answers = planned.map(({ statement }) => {
      if (statement.readsFirst) statement.readsFirst.done(rowsOf(results.shift()!))
      const result = results.shift()!
      followUps.push(...statement.succeeded(takeHiddenValues(result.results, hiddenNames(statement), false)))
      return statement.returnsRows ? result : { ...result, results: [] }
    })
    live.statementFor(db, 'COMMIT', []).succeeded([])
    await runFollowUps(followUps)
    return answers
  }
  return ready
}

/** A result's rows as arrays of their values (a query of ours names each of its columns once). */
function rowsOf(result: D1Result): (string | null)[][] {
  return result.results.map((row) => Object.values(row as object))
}

function hiddenNames(statement: Statement): string[] {
  return Array.from({ length: statement.hiddenColumns }, (_, i) => `${HIDDEN_PREFIX}${i}`)
}
