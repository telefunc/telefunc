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
  /** The statement `planned` as sent: our text, with the values it takes. */
  const sent = (planned: Statement, values: unknown[]) => prepare(planned.text).bind(...(planned.values ?? values))
  const send = async <T>(planned: Statement, request: () => Promise<T>): Promise<T> => {
    try {
      return await request()
    } catch (err) {
      planned.failed()
      throw err
    }
  }
  /** Runs `text` as `live` plans it, through `all()`; the caller's rows are objects. */
  const all = async (text: string, values: unknown[]): Promise<D1Result> => {
    await ready()
    const planned = live.statementFor(db, text, values)
    const result = await send(planned, () => sent(planned, values).all())
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
        const [names = [], ...rows] = await send(planned, () => sent(planned, values).raw({ columnNames: true }))
        const hiddenRows = takeHiddenValues(rows, hiddenNames(planned), true)
        names.splice(names.length - planned.hiddenColumns, planned.hiddenColumns)
        await runFollowUps(planned.succeeded(hiddenRows))
        const kept = planned.returnsRows ? rows : []
        return options?.columnNames ? [names, ...kept] : kept
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
    let results: D1Result[]
    try {
      results = await batch(planned.map(({ app, statement }) => sent(statement, app.values)))
    } catch (err) {
      for (const { statement } of planned) statement.failed()
      throw err
    }
    const followUps: FollowUp[] = []
    live.statementFor(db, 'BEGIN', []).succeeded([])
    const answers = results.map((result, i) => {
      const { statement } = planned[i]!
      followUps.push(...statement.succeeded(takeHiddenValues(result.results, hiddenNames(statement), false)))
      return statement.returnsRows ? result : { ...result, results: [] }
    })
    live.statementFor(db, 'COMMIT', []).succeeded([])
    await runFollowUps(followUps)
    return answers
  }
  return ready
}

function hiddenNames(statement: Statement): string[] {
  return Array.from({ length: statement.hiddenColumns }, (_, i) => `${HIDDEN_PREFIX}${i}`)
}
