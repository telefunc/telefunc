export { capturePostgresJs, isPostgresJs }
export type { PostgresJs }

import type { FollowUp, LiveDatabase, Statement } from '../liveDatabase.js'
import { copyRows } from '../sharedReads.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

// The parts of postgres.js used here, so that `postgres` stays the app's dependency.
type PostgresJs = {
  unsafe(text: string, args?: unknown[]): Query
  begin(...args: unknown[]): Promise<unknown>
  reserve(): Promise<PostgresJs & { release(): void }>
  options: { max: number }
}
/** A scope `begin()` and `savepoint()` give their callback: statements on the transaction's connection. */
type Scope = PostgresJs & { savepoint(...args: unknown[]): Promise<unknown> }
type Result = unknown[] & { columns?: { name: string }[] | null }
/** postgres.js runs a query when it is first awaited: `handle()` sends it through `handler`, its connection's queue. */
type Query = Promise<Result> & {
  handler: (query: Query) => void
  strings: readonly string[]
  args: unknown[]
  tagged: boolean
  /** The text sent and its parameters, once built. */
  string?: string
  parameters?: unknown[]
  executed: boolean
  isRaw?: boolean | 'values'
  cursorFn?: unknown
  forEachFn?: unknown
  onlyDescribe?: boolean
  streaming?: boolean
  resolve(result: Result): void
  reject(err: Error): void
  handle(): Promise<void>
  values(): Query
}

/** How the queries of one handler are captured: the database, and the connection they run on (`undefined`: any
 *  connection of the pool, a new one for each statement). */
type Target = { live: LiveDatabase; sql: PostgresJs; connection: object | undefined }

const CAPTURED = Symbol.for('@telefunc/live/captured')
const TARGETS = Symbol.for('@telefunc/live/postgresJsTargets')
/** A query of ours: it runs as written. */
const OURS = Symbol.for('@telefunc/live/ours')

function isPostgresJs(database: object): database is PostgresJs {
  return typeof database === 'function' && 'unsafe' in database && 'begin' in database
}

/** Captures every statement of a postgres.js `sql`: its own, and those of its transactions and reserved connections. */
function capturePostgresJs(sql: PostgresJs, live: LiveDatabase): void {
  if (CAPTURED in sql) return
  Object.defineProperty(sql, CAPTURED, { value: true, configurable: true })
  const targets = captureQueries(Object.getPrototypeOf(sql.unsafe('')))
  // With one connection, a `BEGIN` sent through `sql` holds for the statements after it.
  targets.set(handlerOf(sql), { live, sql, connection: sql.options.max === 1 ? sql : undefined })

  const begin = sql.begin.bind(sql)
  sql.begin = (...args) => {
    const callback = args.pop() as (scope: Scope) => unknown
    return begin(...args, (scope: Scope) => {
      const transaction = {}
      // postgres.js sent its `BEGIN` through `sql`, before the transaction had a connection.
      control(live, transaction, 'BEGIN')
      captureScope(scope, transaction, live, targets)
      return callback(scope)
    })
  }
  const reserve = sql.reserve.bind(sql)
  sql.reserve = async () => {
    const reserved = await reserve()
    targets.set(handlerOf(reserved), { live, sql: reserved, connection: reserved })
    return reserved
  }
}

function captureScope(scope: Scope, transaction: object, live: LiveDatabase, targets: WeakMap<object, Target>): void {
  targets.set(handlerOf(scope), { live, sql: scope, connection: transaction })
  const savepoint = scope.savepoint.bind(scope)
  let savepoints = 0
  scope.savepoint = (...args) => {
    const callback = args.pop() as (inner: Scope) => unknown
    // postgres.js names its savepoint and sends it before the callback: ours stands for it.
    const name = `"${HIDDEN_PREFIX}${savepoints++}"`
    control(live, transaction, `SAVEPOINT ${name}`)
    const done = savepoint(...args, (inner: Scope) => {
      captureScope(inner, transaction, live, targets)
      return callback(inner)
    })
    done.catch(() => control(live, transaction, `ROLLBACK TO SAVEPOINT ${name}`))
    return done
  }
}

/** Patches the `Query` class of this copy of postgres.js once; returns the handlers whose queries are captured. */
function captureQueries(query: Query): WeakMap<object, Target> {
  const known = (query as unknown as Record<symbol, WeakMap<object, Target>>)[TARGETS]
  if (known) return known
  const targets = new WeakMap<object, Target>()
  const handle = query.handle
  query.handle = function (this: Query) {
    const target = targets.get(this.handler)
    if (!target || this.executed || OURS in this) return handle.call(this)
    this.executed = true
    return run(this, target)
  }
  Object.defineProperty(query, TARGETS, { value: targets })
  return targets
}

async function run(query: Query, { live, sql, connection = {} }: Target): Promise<void> {
  // As postgres.js does: `.values()` and the like are called before it runs.
  await 1
  try {
    if (!live.isReady()) await live.ready(async (text) => ((await ours(sql, text))[0] as string[])[0]!)
  } catch (err) {
    return query.reject(err as Error)
  }
  const known = textOf(query)
  let statement = known && rewritten(query, known, live, connection)
  while (statement && statement.joined) {
    const joined = await statement.joined
    if (joined) return query.resolve(joined.result as Result)
    statement = rewritten(query, known!, live, connection)
  }
  // A template with helpers: its text is known once built.
  const opaque = known ? undefined : live.statementWithTextLater(connection)
  const sent = () => statement || opaque!.sent(query.string ?? '', query.parameters ?? [])
  const { resolve, reject } = query
  query.resolve = (result) => {
    const done = sent()
    const followUps = done.succeeded(takeHidden(query, result, done))
    done.answered(result, copyResult)
    resolve(result)
    for (const followUp of followUps) runFollowUp(sql, followUp)
  }
  query.reject = (err) => {
    sent().failed()
    reject(err)
  }
  query.handler(query)
}

function rewritten(query: Query, known: { text: string; args: unknown[] }, live: LiveDatabase, connection: object) {
  // Rows reach the caller other than as the result: can't strip what the statement would add.
  const asWritten = Boolean(query.cursorFn || query.forEachFn || query.onlyDescribe || query.streaming)
  const sharing = asWritten ? undefined : { rowForm: `postgres.js:${query.isRaw || 'object'}` }
  const statement = live.statementFor(connection, known.text, known.args, { rewrite: !asWritten, sharing })
  if (statement.text !== known.text) {
    query.strings = [statement.text]
    query.args = known.args
    query.tagged = false
  }
  return statement
}

/** The text of a query and its parameters, unless a template's helpers build part of it. */
function textOf(query: Query): { text: string; args: unknown[] } | undefined {
  if (!query.tagged) return { text: query.strings[0]!, args: query.args }
  if (query.args.some(isFragment)) return undefined
  return { text: query.strings.reduce((text, part, i) => `${text}$${i}${part}`), args: query.args }
}

/** `sql(identifier)`, `sql(rows)`, a nested `sql`…``: part of the text rather than a value. */
function isFragment(value: unknown): boolean {
  const first = Array.isArray(value) ? value[0] : value
  return typeof first === 'object' && first !== null && 'then' in first && !('type' in first)
}

function takeHidden(query: Query, result: Result, statement: Statement): (string | null)[][] {
  if (statement.hiddenColumns === 0 || !Array.isArray(result)) return []
  // The columns array is the prepared statement's own: the next run parses rows by it.
  const columns = [...(result.columns ?? [])]
  const hidden = takeHiddenFields(columns, statement)
  result.columns = columns
  const values = takeHiddenValues(result, hidden, Boolean(query.isRaw))
  if (!statement.returnsRows) result.length = 0
  // `.raw()` rows hold buffers.
  return values.map((row) => row.map((v) => (v === null || typeof v === 'string' ? v : String(v))))
}

/** A copy of a result for a read that joined this one: postgres.js's own array, with its `count`, `columns`, …. */
function copyResult(result: Result): Result {
  const copy = new (result.constructor as new () => Result)()
  for (const row of copyRows(result)) copy.push(row)
  for (const key of Object.getOwnPropertyNames(result)) {
    if (key !== 'length' && !/^\d+$/.test(key))
      (copy as unknown as Record<string, unknown>)[key] = (result as unknown as Record<string, unknown>)[key]
  }
  return copy
}

function runFollowUp(sql: PostgresJs, { text, values, done }: FollowUp): void {
  ours(sql, text, values).then(
    (rows) => done(rows as (string | null)[][]),
    () => done(undefined),
  )
}

function ours(sql: PostgresJs, text: string, values: unknown[] = []): Query {
  const query = sql.unsafe(text, values).values()
  Object.defineProperty(query, OURS, { value: true })
  return query
}

/** A transaction statement postgres.js sends where it isn't captured. */
function control(live: LiveDatabase, connection: object, text: string): void {
  live.statementFor(connection, text, []).succeeded([])
}

function handlerOf(sql: PostgresJs): object {
  return sql.unsafe('').handler
}
