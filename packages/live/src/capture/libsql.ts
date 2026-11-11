export { captureLibsql, isLibsql }
export type { Libsql }

import { assertUsage } from '../assert.js'
import type { FollowUp, LiveDatabase, Statement } from '../liveDatabase.js'
import { paramValues } from '../sqlite/dialect.js'

// The parts of `@libsql/client` used here, so that it stays the app's dependency.
type Args = unknown[] | Record<string, unknown>
type InStatement = string | { sql: string; args?: Args }
type BatchStatement = InStatement | [string, Args?]
type Row = Record<string, unknown> & ArrayLike<unknown>
type ResultSet = {
  columns: string[]
  columnTypes: string[]
  rows: Row[]
  rowsAffected: number
  lastInsertRowid: bigint | undefined
}
type Transaction = {
  execute(statement: InStatement, args?: Args): Promise<ResultSet>
  batch(statements: BatchStatement[]): Promise<ResultSet[]>
  executeMultiple(text: string): Promise<void>
  commit(): Promise<void>
  rollback(): Promise<void>
  close(): void
  readonly closed: boolean
}
type Libsql = {
  execute(statement: InStatement, args?: Args): Promise<ResultSet>
  batch(statements: BatchStatement[], mode?: string): Promise<ResultSet[]>
  migrate(statements: InStatement[]): Promise<ResultSet[]>
  transaction(mode?: string): Promise<Transaction>
  executeMultiple(text: string): Promise<void>
  readonly protocol: string
}

const CAPTURED = Symbol.for('@telefunc/live/captured')
/** What a write that returns no rows of its own answers: SQLite's, not the `RETURNING` of ours it was sent with. As
 *  text, so that no `intMode` turns them into what the caller's result doesn't hold. */
const WRITE_INFO = 'SELECT CAST(changes() AS TEXT), CAST(last_insert_rowid() AS TEXT)'

function isLibsql(client: object): client is Libsql {
  return 'execute' in client && 'executeMultiple' in client && 'migrate' in client && 'protocol' in client
}

/** A statement of the app's, as planned. */
type Planned = { app: { sql: string; args: Args }; statement: Statement }

/**
 * Captures a local libSQL client (`@libsql/client` on a `file:` URL). It runs each statement on any connection of its
 * pool, so a connection's own change log can't follow them: a write gives its rows through a `RETURNING` of ours, as
 * on D1, and a query of ours reads its old rows first, in one transaction with it. The client is the connection of
 * statements sent alone and of a `batch()`, which is one transaction; each `transaction()` is one of its own.
 */
function captureLibsql(client: Libsql, live: LiveDatabase): void {
  if (CAPTURED in client) return
  assertUsage(
    client.protocol === 'file',
    `live…() follows a local libSQL database (a file: URL) only, not one at a ${client.protocol}: URL.`,
  )
  Object.defineProperty(client, CAPTURED, { value: true, configurable: true })
  const execute = client.execute.bind(client)
  const batch = client.batch.bind(client)
  const migrate = client.migrate.bind(client)
  const transaction = client.transaction.bind(client)
  const executeMultiple = client.executeMultiple.bind(client)
  const ready = readyThrough(live, execute)
  const plan = planOn(live, client)

  client.execute = async (statement, args) => {
    const app = inStatement(statement, args)
    await ready()
    const planned = plan(app)
    // A write whose old rows, or whose answer, take a query of ours runs with them in a transaction of ours.
    if (!planned.readsFirst && planned.returnsRows) return await runOn(planned, app, execute, execute)
    let result: Answer
    let before: Rows | undefined
    try {
      const tx = await transaction('write')
      try {
        before = planned.readsFirst && (await readRows(tx.execute.bind(tx), planned.readsFirst))
        result = await sent(planned, app, tx.execute.bind(tx))
        await tx.commit()
      } finally {
        tx.close()
      }
    } catch (err) {
      planned.failed()
      throw err
    }
    planned.readsFirst?.done(before)
    await runFollowUps(planned.succeeded(result.hiddenRows), execute)
    return result.answer
  }

  // A batch is one transaction: told whole once it committed, so that no other statement of the client lands inside it.
  client.batch = async (statements, mode) => {
    const apps = statements.map((statement) => inStatement(statement))
    await ready()
    const planned = apps.map((app) => ({ app, statement: plan(app) }))
    const { sentStatements, appIndex } = interleaved(planned)
    let results: ResultSet[]
    try {
      results = await batch(sentStatements, mode)
    } catch (err) {
      for (const { statement } of planned) statement.failed()
      throw withAppIndex(err, appIndex)
    }
    const followUps: FollowUp[] = []
    live.statementFor(client, 'BEGIN', []).succeeded([])
    const answers = planned.map(({ statement }) => {
      if (statement.readsFirst) statement.readsFirst.done(rowsOf(results.shift()!))
      const { answer, hiddenRows } = answerOf(
        results.shift()!,
        statement,
        statement.returnsRows ? undefined : results.shift(),
      )
      followUps.push(...statement.succeeded(hiddenRows))
      return answer
    })
    live.statementFor(client, 'COMMIT', []).succeeded([])
    await runFollowUps(followUps, execute)
    return answers
  }

  // Schema changes, as written: their writes count their whole tables.
  client.migrate = async (statements) => {
    await ready()
    const planned = statements.map((statement) => {
      const { sql } = inStatement(statement)
      return live.statementFor(client, sql, [], { rewrite: false })
    })
    let results: ResultSet[]
    try {
      results = await migrate(statements)
    } catch (err) {
      for (const statement of planned) statement.failed()
      throw err
    }
    live.statementFor(client, 'BEGIN', []).succeeded([])
    for (const statement of planned) statement.succeeded([])
    live.statementFor(client, 'COMMIT', []).succeeded([])
    return results
  }

  client.executeMultiple = async (text) => {
    await ready()
    const statements = live.statementsOf(text)
    if (!statements)
      return await sendAsWritten(live.statementFor(client, text, [], { rewrite: false }), () => executeMultiple(text))
    for (const statement of statements) await client.execute(statement)
  }

  client.transaction = async (mode) => {
    await ready()
    const tx = await transaction(mode)
    captureTransaction(tx, live)
    return tx
  }
}

/** Captures one `transaction()`, from the `BEGIN` it ran on its connection. */
function captureTransaction(tx: Transaction, live: LiveDatabase): void {
  const execute = tx.execute.bind(tx)
  const batch = tx.batch.bind(tx)
  const executeMultiple = tx.executeMultiple.bind(tx)
  const commit = tx.commit.bind(tx)
  const rollback = tx.rollback.bind(tx)
  const close = tx.close.bind(tx)
  // A statement of the transaction may have changed the schema, which `live` then loads again.
  const ready = readyThrough(live, execute)
  const plan = planOn(live, tx)
  live.statementFor(tx, 'BEGIN', []).succeeded([])

  /** Runs `app` on the transaction's connection; `send` sends the app's statement itself. */
  const run = async (app: Planned['app'], send: (statement: InStatement) => Promise<ResultSet>) => {
    await ready()
    return await runOn(plan(app), app, send, execute)
  }

  tx.execute = (statement, args) => run(inStatement(statement, args), execute)
  // One statement at a time, each through the transaction's own `batch()`, which tells its error by its index there.
  tx.batch = async (statements) => {
    const results: ResultSet[] = []
    for (const [index, statement] of statements.entries()) {
      const sendOne = async (one: InStatement) => {
        try {
          return (await batch([one]))[0]!
        } catch (err) {
          throw withAppIndex(err, [index])
        }
      }
      results.push(await run(inStatement(statement), sendOne))
    }
    return results
  }
  tx.executeMultiple = async (text) => {
    await ready()
    const statements = live.statementsOf(text)
    if (!statements)
      return await sendAsWritten(live.statementFor(tx, text, [], { rewrite: false }), () => executeMultiple(text))
    for (const statement of statements) await tx.execute(statement)
  }
  tx.commit = async () => {
    const statement = live.statementFor(tx, 'COMMIT', [])
    await sendAsWritten(statement, commit)
  }
  tx.rollback = async () => {
    try {
      await rollback()
    } finally {
      live.statementFor(tx, 'ROLLBACK', []).succeeded([])
    }
  }
  tx.close = () => {
    try {
      close()
    } finally {
      live.statementFor(tx, 'ROLLBACK', []).succeeded([])
    }
  }
}

type Rows = (string | null)[][]

/** How to load what capturing needs, through `execute`. */
function readyThrough(live: LiveDatabase, execute: (statement: InStatement) => Promise<ResultSet>) {
  return async () => {
    if (live.isReady()) return
    await live.ready(async (text) => (await execute(text)).rows[0]![0] as string)
  }
}

/** Plans the app's statements on `connection`; named parameters (an object) by the number SQLite gives each. */
function planOn(live: LiveDatabase, connection: object) {
  return ({ sql, args }: Planned['app']) => {
    const values = Array.isArray(args) ? args : paramValues(sql, [], unprefixed(args))
    return values
      ? live.statementFor(connection, sql, values)
      : live.statementFor(connection, sql, [], { rewrite: false })
  }
}

/** Named parameters as libSQL binds them: by the name without its prefix, the last of a name winning. */
function unprefixed(named: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(named).map(([name, value]) => [name.replace(/^[:@$]/, ''), value]))
}

/** Runs `planned` on one connection, with the queries of ours it needs on either side: `send` sends the app's
 *  statement, `execute` ours. */
async function runOn(
  planned: Statement,
  app: Planned['app'],
  send: (statement: InStatement) => Promise<ResultSet>,
  execute: (statement: InStatement) => Promise<ResultSet>,
): Promise<ResultSet> {
  if (planned.readsFirst) planned.readsFirst.done(await readRows(execute, planned.readsFirst))
  let result: Answer
  try {
    result = await sent(planned, app, send, execute)
  } catch (err) {
    planned.failed()
    throw err
  }
  await runFollowUps(planned.succeeded(result.hiddenRows), execute)
  return result.answer
}

/** Sends `planned` through `send`, then, for a write that returns no rows of its own, asks SQLite what it answers. */
async function sent(
  planned: Statement,
  app: Planned['app'],
  send: (statement: InStatement) => Promise<ResultSet>,
  execute: (statement: InStatement) => Promise<ResultSet> = send,
): Promise<Answer> {
  const result = await send({ sql: planned.text, args: planned.values ?? app.args })
  return answerOf(result, planned, planned.returnsRows ? undefined : await execute(WRITE_INFO))
}

async function sendAsWritten(statement: Statement, send: () => Promise<void>): Promise<void> {
  try {
    await send()
  } catch (err) {
    statement.failed()
    throw err
  }
  statement.succeeded([])
}

/** The app's statements with the queries of ours each needs on either side, and the app's index of each sent one. */
function interleaved(planned: Planned[]): { sentStatements: InStatement[]; appIndex: number[] } {
  const sentStatements: InStatement[] = []
  const appIndex: number[] = []
  for (const [index, { app, statement }] of planned.entries()) {
    const own: InStatement[] = [{ sql: statement.text, args: statement.values ?? app.args }]
    if (statement.readsFirst) own.unshift({ sql: statement.readsFirst.text, args: statement.readsFirst.values })
    if (!statement.returnsRows) own.push(WRITE_INFO)
    sentStatements.push(...own)
    appIndex.push(...own.map(() => index))
  }
  return { sentStatements, appIndex }
}

/** A `LibsqlBatchError` tells the index of the statement that failed: the app's, not among the queries of ours. */
function withAppIndex(err: unknown, appIndex: number[]): unknown {
  const failed = err as { statementIndex?: unknown }
  if (typeof failed?.statementIndex === 'number') failed.statementIndex = appIndex[failed.statementIndex]
  return err
}

/** What the app gets, and the values of our hidden columns in each row. */
type Answer = { answer: ResultSet; hiddenRows: Rows }

/** `result` as the app's statement answers it: without our hidden columns, and for a write that returns no rows of
 *  its own, the rows it changed and its last inserted row id as `info` (`WRITE_INFO`'s answer) tells them. */
function answerOf(result: ResultSet, planned: Statement, info?: ResultSet): Answer {
  const hidden = planned.hiddenColumns
  if (hidden === 0 && !info) return { answer: result, hiddenRows: [] }
  const kept = result.columns.length - hidden
  const hiddenRows = result.rows.map((row) => Array.prototype.slice.call(row, kept) as (string | null)[])
  const ResultSetOf = result.constructor as new (...fields: unknown[]) => ResultSet
  const answer = info
    ? new ResultSetOf([], [], [], Number(info.rows[0]![0]), BigInt(info.rows[0]![1] as string))
    : new ResultSetOf(
        result.columns.slice(0, kept),
        result.columnTypes.slice(0, kept),
        result.rows.map((row) => rowOf(row, result.columns, kept)),
        result.rowsAffected,
        result.lastInsertRowid,
      )
  return { answer, hiddenRows }
}

/** `row`'s first `length` values, as libSQL makes a row: by index, and by column name for the first of each name. */
function rowOf(row: Row, columns: string[], length: number): Row {
  const kept = {} as Row
  Object.defineProperty(kept, 'length', { value: length })
  for (let i = 0; i < length; i++) {
    Object.defineProperty(kept, i, { value: row[i] })
    if (!Object.hasOwn(kept, columns[i]!))
      Object.defineProperty(kept, columns[i]!, { value: row[i], enumerable: true, configurable: true, writable: true })
  }
  return kept
}

/** Runs a query of ours: its rows, or `undefined` when it failed. */
async function readRows(
  execute: (statement: InStatement) => Promise<ResultSet>,
  { text, values }: FollowUp,
): Promise<Rows | undefined> {
  try {
    return rowsOf(await execute({ sql: text, args: values }))
  } catch {
    return undefined
  }
}

async function runFollowUps(
  followUps: FollowUp[],
  execute: (statement: InStatement) => Promise<ResultSet>,
): Promise<void> {
  for (const followUp of followUps) followUp.done(await readRows(execute, followUp))
}

function rowsOf(result: ResultSet): Rows {
  return result.rows.map((row) => Array.from(row) as (string | null)[])
}

/** A statement as libSQL takes it: text alone, with `args`, or a `[text, args]` pair in a batch. */
function inStatement(statement: BatchStatement, args?: Args): { sql: string; args: Args } {
  if (typeof statement === 'string') return { sql: statement, args: args ?? [] }
  if (Array.isArray(statement)) return { sql: statement[0], args: statement[1] ?? [] }
  return { sql: statement.sql, args: statement.args ?? [] }
}
