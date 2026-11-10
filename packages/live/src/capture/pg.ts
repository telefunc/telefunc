export { capturePg }
export type { PgPool, PgClient }

import { AsyncResource } from 'node:async_hooks'
import { MAX_RESULT_ROWS, type FollowUp, type LiveDatabase, type Reservation, type Statement } from '../liveDatabase.js'
import { sendWithBeforeCommit } from './beforeCommit.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

// The parts of `pg` (node-postgres) used here, so that `pg` stays the app's dependency.
type PgQueryConfig = {
  text: string
  values?: unknown[]
  name?: string
  rowMode?: 'array'
  [RESERVATION]?: Reservation
  [key: string]: unknown
}
type PgResult = { rows: unknown[]; fields: { name: string }[]; [key: string]: unknown }
/** `pg`'s `Result` class: it parses a row's text values with the type parsers it was given. */
type PgResultClass = new (
  rowMode: 'array' | undefined,
  types: unknown,
) => PgResult & { addFields(fields: unknown[]): void; parseRow(row: unknown[]): unknown; addRow(row: unknown): void }
/** A read's result for the identical reads that joined it: its values as text, and the type parsers of the client it
 *  ran on, for the callers that don't bring their own. */
type SharedPgResult = { result: PgResult; types: unknown }
type PgCallback = (err: Error | null, result?: PgResult | PgResult[]) => void
type PgClient = { query: (...args: any[]) => any }
type PgPool = {
  on(event: 'connect', listener: (client: PgClient) => void): unknown
  connect: (callback?: (...args: any[]) => void) => any
  query: (...args: any[]) => any
  totalCount: number
}
/** A `pg-cursor` (Prisma reads and writes through one), or another object `pg` submits. */
type Submittable = {
  submit: (connection: unknown) => void
  text: string
  values?: unknown[]
  name?: string
  read?: (count: number, callback: (err: Error | null, rows: unknown[], result: unknown) => void) => void
  close?: (callback?: (err?: Error) => void) => unknown
  _result?: { fields: { name: string }[] }
  handleError?: (err: Error, connection?: unknown) => void
  handleReadyForQuery?: (connection: unknown) => void
}

const CAPTURED = Symbol.for('@telefunc/live/captured')
/** On a query config `pool.query()` passes to the client it gets: the read it reserved. */
const RESERVATION = Symbol('reservation')
/** Type parsers that keep each value's text: a shared read's callers each parse it their own way. */
const AS_TEXT = { getTypeParser: () => (text: string) => text }

/** Captures every statement of a `pg.Pool` (each of its clients) or a `pg.Client`. */
function capturePg(database: PgPool | PgClient, live: LiveDatabase): void {
  if (!('totalCount' in database)) return captureClient(database, live)
  database.on('connect', (client) => captureClient(client, live))
  keepCallerContext(database)
  shareReadsBeforeConnecting(database, live)
}

/** A live read sent with `pool.query()` joins an identical one before waiting for a connection: N clients refetching
 *  one query wait for one connection, not N. */
function shareReadsBeforeConnecting(pool: PgPool, live: LiveDatabase): void {
  const query = pool.query.bind(pool)
  pool.query = (...args: unknown[]) => {
    const [config, values, callback] = args
    const queryConfig = typeof config === 'string' ? { text: config } : (config as PgQueryConfig)
    const shared =
      !isSubmittable(config) &&
      isShareable(queryConfig) &&
      live.reserveRead(queryConfig.text, Array.isArray(values) ? values : (queryConfig.values ?? []), ROW_FORM)
    if (!shared) return query(...args)
    const done = typeof values === 'function' ? (values as PgCallback) : (callback as PgCallback | undefined)
    const own = { ...queryConfig, ...(Array.isArray(values) ? { values } : {}) }
    if ('joined' in shared)
      return answer(
        joinedResult(shared.joined, own, () => query(own)),
        done,
      )
    const result = query({ ...own, [RESERVATION]: shared.reservation }) as Promise<PgResult>
    // Failed before the client ran it (no connection): the reads that joined run their own.
    result.catch(() => shared.reservation.failed())
    return answer(result, done)
  }
}

/** The leader's result for a read that joined it, or the read's own when the leader failed. */
async function joinedResult(
  joined: Promise<{ result: unknown } | undefined>,
  config: PgQueryConfig,
  own: () => Promise<PgResult>,
): Promise<PgResult> {
  const leader = await joined
  return leader ? parsedFor(leader.result as SharedPgResult, config) : await own()
}

function answer(result: Promise<PgResult | PgResult[]>, callback: PgCallback | undefined) {
  if (!callback) return result
  result.then(
    (r) => callback(null, r),
    (err) => callback(err),
  )
  return undefined
}

/** All `pg` reads share: each caller parses the text values itself (`parsedFor()`). */
const ROW_FORM = 'pg'

/** Whether a query asks for nothing but its rows, in a form `parsedFor()` reproduces: not binary values, say. */
function isShareable(config: PgQueryConfig): boolean {
  const known = ['text', 'values', 'name', 'rowMode', 'types']
  return Object.keys(config).every((key) => known.includes(key))
}

/** A shared read's result as `config` asks for it: parsed by `pg`'s own `Result`, as the caller's query would have. */
function parsedFor({ result, types }: SharedPgResult, config: PgQueryConfig): PgResult {
  const Result = result.constructor as PgResultClass
  const own = new Result(config.rowMode, config.types ?? types)
  own.addFields(result.fields)
  for (const row of result.rows as unknown[][]) own.addRow(own.parseRow(row))
  return Object.assign(own, { command: result.command, rowCount: result.rowCount, oid: result.oid })
}

/** A waiting `connect(callback)` (each `pool.query()` makes one) is called back by whoever frees a client: without
 *  this, its statements would run in that caller's telefunction call, and its reads would be watched for the wrong
 *  client. */
function keepCallerContext(pool: PgPool): void {
  const connect = pool.connect.bind(pool)
  pool.connect = (callback) => (callback ? connect(AsyncResource.bind(callback)) : connect())
}

function captureClient(client: PgClient, live: LiveDatabase): void {
  if (CAPTURED in client) return
  Object.defineProperty(client, CAPTURED, { value: true, configurable: true })
  const query = client.query.bind(client)
  const runOurs = async (text: string) =>
    ((await query(text)) as PgResult).rows.map((row: any) => row.catalog)[0] as string
  /** A query of ours: its rows, as arrays of text. */
  const sendOurs = async (text: string) =>
    ((await query({ text, rowMode: 'array' })) as PgResult).rows as (string | null)[][]
  client.query = (config: unknown, values?: unknown, callback?: PgCallback) => {
    if (isSubmittable(config)) return submit(config)
    if (typeof values === 'function') {
      callback = values as PgCallback
      values = undefined
    }
    const { [RESERVATION]: reservation, ...queryConfig }: PgQueryConfig =
      typeof config === 'string' ? { text: config } : { ...(config as PgQueryConfig) }
    if (Array.isArray(values)) queryConfig.values = values
    return answer(run(queryConfig, reservation), callback)
  }

  async function run(queryConfig: PgQueryConfig, reservation: Reservation | undefined): Promise<PgResult | PgResult[]> {
    if (!live.isReady()) await live.ready(runOurs)
    const plan = () =>
      live.statementFor(client, queryConfig.text, queryConfig.values ?? [], {
        sharing: isShareable(queryConfig) ? { rowForm: ROW_FORM, reservation } : undefined,
      })
    let statement = plan()
    while (statement.joined) {
      const joined = await statement.joined
      if (joined) return parsedFor(joined.result as SharedPgResult, queryConfig)
      // The read it joined failed: it runs on its own.
      statement = plan()
    }
    const config = statement.leads ? { ...queryConfig, rowMode: 'array' as const, types: AS_TEXT } : queryConfig
    let result: PgResult | PgResult[]
    try {
      result = await sendWithBeforeCommit(statement, () => query(sent(config, statement)), sendOurs)
    } catch (err) {
      statement.failed()
      throw err
    }
    // Several statements in one text: nothing was rewritten.
    if (Array.isArray(result)) {
      statement.succeeded([])
      return result
    }
    const hidden = takeHiddenFields(result.fields, statement)
    statement.succeeded(takeHiddenValues(result.rows, hidden, config.rowMode === 'array')).forEach(runFollowUp)
    if (!statement.returnsRows) result.rows.length = 0
    if (!statement.leads) return result
    // Text values, parsed by each caller: nothing they parse is shared.
    const shared: SharedPgResult = { result, types: client }
    statement.answered(shared, () => shared)
    return parsedFor(shared, queryConfig)
  }

  /** Queued on the client now: it runs before anything the caller sends next. */
  function runFollowUp({ text, values, done }: FollowUp): void {
    const answered = query({ text, values, rowMode: 'array' }) as Promise<PgResult>
    answered.then(
      (result) => done(result.rows as (string | null)[][]),
      () => done(undefined),
    )
  }

  function submit(submittable: Submittable): Submittable {
    // Wrapped now: the caller may read before the statement is planned, and rows only come after.
    const begin = isCursor(submittable) ? captureCursor(submittable) : undefined
    const send = () => {
      const values = submittable.values ?? []
      if (!begin) {
        // Rows reach the caller as events (an event-based `pg.Query`): run as written, nothing to strip.
        settleWhenDone(submittable, live.statementFor(client, submittable.text, values, { rewrite: false }))
        return query(submittable)
      }
      const statement = live.statementFor(client, submittable.text, values)
      if (statement.text !== submittable.text) {
        submittable.text = statement.text
        if (submittable.name) submittable.name += HIDDEN_PREFIX
      }
      begin(statement)
      return query(submittable)
    }
    if (live.isReady()) return send()
    // The schema isn't known yet: a `pg-cursor` queues reads until it is submitted.
    void live.ready(runOurs).then(send, (err) => submittable.handleError?.(err))
    return submittable
  }

  /** Tells `statement` how a submittable that isn't a cursor ended: the client calls `handleReadyForQuery()` once it
   *  did, after `handleError()` when it failed. */
  function settleWhenDone(submittable: Submittable, statement: Statement): void {
    const handleError = submittable.handleError?.bind(submittable)
    const handleReadyForQuery = submittable.handleReadyForQuery?.bind(submittable)
    let failed = false
    submittable.handleError = (err, connection) => {
      failed = true
      statement.failed()
      handleError?.(err, connection)
    }
    submittable.handleReadyForQuery = (connection) => {
      if (!failed) statement.succeeded([]).forEach(runFollowUp)
      handleReadyForQuery?.(connection)
    }
  }

  /** Strips the hidden columns from the rows of a cursor as they are read; returns how to give it its statement. */
  function captureCursor(cursor: Cursor): (statement: Statement) => void {
    const read = cursor.read.bind(cursor)
    const close = cursor.close.bind(cursor)
    let statement: Statement | undefined
    const hiddenRows: (string | null)[][] = []
    let hidden: string[] | undefined
    let settled = false
    const settle = (err?: Error | null) => {
      if (settled || !statement) return
      settled = true
      if (err) return statement.failed()
      statement.succeeded(hiddenRows).forEach(runFollowUp)
    }
    cursor.read = (count, callback) =>
      read(count, (err, rows, result) => {
        if (err) {
          settle(err)
          return callback(err, rows, result)
        }
        hidden ??= takeHiddenFields(cursor._result?.fields ?? [], statement!)
        const values = takeHiddenValues(rows, hidden, Array.isArray(rows[0]))
        // Past the limit they aren't kept: a cursor reads rows a batch at a time to keep memory low.
        if (hiddenRows.length <= MAX_RESULT_ROWS) hiddenRows.push(...values)
        if (rows.length < count || rows.length === 0) settle()
        callback(err, statement!.returnsRows ? rows : [], result)
      })
    let closed = false
    cursor.close = (callback) => {
      closed = true
      settle()
      return close(callback)
    }
    return (s) => {
      statement = s
      // Closed before it was sent: it returns nothing.
      if (closed) settle()
    }
  }
}

function sent(config: PgQueryConfig, statement: Statement): PgQueryConfig {
  if (statement.text === config.text) return config
  // A prepared statement's name is bound to its text on the connection.
  return { ...config, text: statement.text, ...(config.name ? { name: config.name + HIDDEN_PREFIX } : {}) }
}

type Cursor = Submittable & Required<Pick<Submittable, 'read' | 'close'>>

function isCursor(submittable: Submittable): submittable is Cursor {
  return typeof submittable.read === 'function' && typeof submittable.close === 'function'
}

function isSubmittable(value: unknown): value is Submittable {
  return typeof value === 'object' && value !== null && typeof (value as Submittable).submit === 'function'
}
