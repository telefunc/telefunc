export { capturePglite }
export type { Pglite }

import type { LiveDatabase, Statement } from '../liveDatabase.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'

// The parts of PGlite used here, so that `@electric-sql/pglite` stays the app's dependency.
type Results = { rows: unknown[]; fields: { name: string }[]; [key: string]: unknown }
type QueryOptions = { rowMode?: 'array' | 'object'; [key: string]: unknown }
type Executor = {
  query(text: string, params?: unknown[], options?: QueryOptions): Promise<Results>
  exec(text: string, options?: unknown): Promise<Results[]>
}
type Transaction = Executor & { rollback(): Promise<void> }
type Pglite = Executor & {
  transaction<T>(callback: (tx: Transaction) => Promise<T>): Promise<T>
  waitReady: Promise<void>
}

const CAPTURED = Symbol.for('@telefunc/live/captured')

/** Captures every statement of a PGlite database. It has one session, so it is the connection. */
function capturePglite(pglite: Pglite, live: LiveDatabase): void {
  if (CAPTURED in pglite) return
  Object.defineProperty(pglite, CAPTURED, { value: true, configurable: true })
  const ready = captureExecutor(pglite, pglite, live)
  const transaction = pglite.transaction.bind(pglite)
  // PGlite sends its own `BEGIN` and `COMMIT`, past `query()`.
  const control = (text: string) => live.statementFor(pglite, text, []).succeeded([])
  pglite.transaction = async <T>(callback: (tx: Transaction) => Promise<T>): Promise<T> => {
    await ready()
    let result: T
    try {
      result = await transaction(async (tx) => {
        control('BEGIN')
        captureExecutor(tx, pglite, live)
        const rollback = tx.rollback.bind(tx)
        tx.rollback = async () => {
          await rollback()
          control('ROLLBACK')
        }
        return callback(tx)
      })
    } catch (err) {
      control('ROLLBACK')
      throw err
    }
    control('COMMIT')
    return result
  }
}

/** Returns how to load what capturing needs, through `executor`'s own `query()`. */
function captureExecutor(executor: Executor, connection: Pglite, live: LiveDatabase): () => Promise<void> {
  const query = executor.query.bind(executor)
  const exec = executor.exec.bind(executor)
  const ready = async () => {
    if (!live.isReady()) await live.ready(async (text) => firstValue(await query(text)))
  }
  executor.query = async (text, params = [], options) => {
    await ready()
    const statement = live.statementFor(connection, text, params)
    const result = await run(statement, () => query(statement.text, params, options))
    const hidden = takeHiddenFields(result.fields, statement)
    statement.succeeded(takeHiddenValues(result.rows, hidden, options?.rowMode === 'array'))
    if (!statement.returnsRows) result.rows.length = 0
    return result
  }
  executor.exec = async (text, options) => {
    await ready()
    // Several statements, possibly: run as written, and count every write as a change to its whole table.
    const statement = live.statementFor(connection, text, [], false)
    const results = await run(statement, () => exec(text, options))
    statement.succeeded([])
    return results
  }
  return ready
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
