export { capturePglite }
export type { Pglite }

import type { FollowUp, LiveDatabase, Statement } from '../liveDatabase.js'
import { copyRows } from '../sharedReads.js'
import { runInOrder, sendWithBeforeCommit } from './beforeCommit.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'

// The parts of PGlite used here, so that `@electric-sql/pglite` stays the app's dependency.
type Results = { rows: unknown[]; fields: { name: string }[]; [key: string]: unknown }
type QueryOptions = { rowMode?: 'array' | 'object'; [key: string]: unknown }
type Executor = {
  query(text: string, params?: unknown[], options?: QueryOptions): Promise<Results>
  exec(text: string, options?: unknown): Promise<Results[]>
}
type Transaction = Executor & { rollback(): Promise<void> }
type TransactionOf = <T>(callback: (tx: Transaction) => Promise<T>) => Promise<T>
type Pglite = Executor & {
  transaction: TransactionOf
  waitReady: Promise<void>
  /** Its database runs: from then on, until `waitReady` resolves, it runs statements of its own. */
  ready: boolean
}

const CAPTURED = Symbol.for('@telefunc/live/captured')

/** Captures every statement of a PGlite database. It has one session, so it is the connection. */
function capturePglite(pglite: Pglite, live: LiveDatabase): void {
  if (CAPTURED in pglite) return
  Object.defineProperty(pglite, CAPTURED, { value: true, configurable: true })
  const transaction: TransactionOf = pglite.transaction.bind(pglite)
  const ready = captureExecutor(pglite, pglite, live, startingStatements(pglite), transaction)
  // PGlite sends its own `BEGIN` and `COMMIT`, past `query()`.
  const control = (text: string) => live.statementFor(pglite, text, []).succeeded([])
  pglite.transaction = async <T>(callback: (tx: Transaction) => Promise<T>): Promise<T> => {
    await ready()
    let commit: Statement | undefined
    let result: T
    try {
      result = await transaction(async (tx) => {
        const query = tx.query.bind(tx)
        let rolledBack = false
        control('BEGIN')
        captureExecutor(tx, pglite, live, () => false, transaction)
        const rollback = tx.rollback.bind(tx)
        tx.rollback = async () => {
          await rollback()
          rolledBack = true
          control('ROLLBACK')
        }
        const result = await callback(tx)
        if (rolledBack) return result
        // PGlite commits once this returns.
        commit = live.statementFor(pglite, 'COMMIT', [])
        await runInOrder(commit.beforeCommit?.queries ?? [], oursThrough(query))
        return result
      })
    } catch (err) {
      if (commit) commit.failed()
      else control('ROLLBACK')
      throw err
    }
    commit?.succeeded([])
    return result
  }
}

const ARRAYS: QueryOptions = { rowMode: 'array' }

/** Whether a statement is one PGlite runs while it starts, through `query()` and `exec()` like the app's. */
function startingStatements(pglite: Pglite): () => boolean {
  // Decided now: a database that already started runs none, though `waitReady` tells so only a microtask later.
  if (pglite.ready) return () => false
  let starting = true
  pglite.waitReady.then(
    () => {
      starting = false
    },
    () => {},
  )
  return () => starting && pglite.ready
}

/** Returns how to load what capturing needs, through `executor`'s own `query()`. `isStarting`: the statement is one
 *  PGlite runs while it starts, sent as it is (ours wait for it to have started). `transaction`: PGlite's own, which
 *  holds the database while a statement and ours run in a transaction of ours. */
function captureExecutor(
  executor: Executor,
  connection: Pglite,
  live: LiveDatabase,
  isStarting: () => boolean,
  transaction: TransactionOf,
): () => Promise<void> {
  const query = executor.query.bind(executor)
  const exec = executor.exec.bind(executor)
  const ready = async () => {
    if (live.isReady()) return
    // A database just created starts first; once its database runs, its `query()` doesn't wait for the rest.
    if (!connection.ready) await connection.waitReady
    await live.ready(async (text) => firstValue(await query(text)))
  }
  /** Queued on the database now: it runs before anything the caller sends next. */
  const runFollowUp = ({ text, values, done }: FollowUp) => {
    const answered = query(text, values, ARRAYS)
    answered.then(
      (result) => done(result.rows as (string | null)[][]),
      () => done(undefined),
    )
  }
  executor.query = async (text, params = [], options) => {
    if (isStarting()) return query(text, params, options)
    await ready()
    // Other options (parsers, serializers) make rows only this caller can be given.
    const rowForm =
      Object.keys(options ?? {}).every((key) => key === 'rowMode') && `pglite:${options?.rowMode ?? 'object'}`
    const plan = () => live.statementFor(connection, text, params, { sharing: rowForm ? { rowForm } : undefined })
    let statement = plan()
    while (statement.joined) {
      const joined = await statement.joined
      if (joined) return joined.result as Results
      statement = plan()
    }
    const sent = statement
    const result = await run(sent, () => withBeforeCommit(sent, (send) => send(sent.text, params, options)))
    const hidden = takeHiddenFields(result.fields, sent)
    sent.succeeded(takeHiddenValues(result.rows, hidden, options?.rowMode === 'array')).forEach(runFollowUp)
    if (!sent.returnsRows) result.rows.length = 0
    sent.answered(result, (r) => ({ ...r, rows: copyRows(r.rows) }))
    return result
  }
  executor.exec = async (text, options) => {
    if (isStarting()) return exec(text, options)
    await ready()
    // Several statements, possibly: run as written, and count every write as a change to its whole table.
    const statement = live.statementFor(connection, text, [], { rewrite: false })
    const results = await run(statement, () => exec(text, options))
    statement.succeeded([])
    return results
  }
  /** Sends `statement` through `send` with the queries of ours its commit needs (`Statement.beforeCommit`). */
  function withBeforeCommit(
    statement: Statement,
    send: (query: Executor['query']) => Promise<Results>,
  ): Promise<Results> {
    if (!statement.beforeCommit?.ownTransaction)
      return sendWithBeforeCommit(statement, () => send(query), oursThrough(query))
    const { queries } = statement.beforeCommit
    return transaction(async (tx) => {
      const txQuery = tx.query.bind(tx)
      const result = await send(txQuery)
      await runInOrder(queries, oursThrough(txQuery))
      return result
    })
  }

  return ready
}

/** Sends a query of ours through `query`: its rows, as arrays of text. */
function oursThrough(query: Executor['query']): (text: string) => Promise<(string | null)[][]> {
  return async (text) => (await query(text, [], ARRAYS)).rows as (string | null)[][]
}

async function run<T>(statement: Statement, send: () => Promise<T>): Promise<T> {
  try {
    return await send()
  } catch (err) {
    statement.failed()
    throw err
  }
}

function firstValue(result: Results): string {
  return (result.rows[0] as { catalog: string }).catalog
}
