export { capturePg }
export type { PgPool, PgClient }

import { AsyncResource } from 'node:async_hooks'
import { MAX_RESULT_ROWS, type FollowUp, type LiveDatabase, type Statement } from '../liveDatabase.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

// The parts of `pg` (node-postgres) used here, so that `pg` stays the app's dependency.
type PgQueryConfig = { text: string; values?: unknown[]; name?: string; rowMode?: 'array'; [key: string]: unknown }
type PgResult = { rows: unknown[]; fields: { name: string }[]; [key: string]: unknown }
type PgCallback = (err: Error | null, result?: PgResult | PgResult[]) => void
type PgClient = { query: (...args: any[]) => any }
type PgPool = {
  on(event: 'connect', listener: (client: PgClient) => void): unknown
  connect: (callback?: (...args: any[]) => void) => any
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
  handleError?: (err: Error) => void
}

const CAPTURED = Symbol.for('@telefunc/live/captured')

/** Captures every statement of a `pg.Pool` (each of its clients) or a `pg.Client`. */
function capturePg(database: PgPool | PgClient, live: LiveDatabase): void {
  if (!('totalCount' in database)) return captureClient(database, live)
  database.on('connect', (client) => captureClient(client, live))
  keepCallerContext(database)
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
  client.query = (config: unknown, values?: unknown, callback?: PgCallback) => {
    if (isSubmittable(config)) return submit(config)
    if (typeof values === 'function') {
      callback = values as PgCallback
      values = undefined
    }
    const queryConfig: PgQueryConfig = typeof config === 'string' ? { text: config } : { ...(config as PgQueryConfig) }
    if (Array.isArray(values)) queryConfig.values = values
    const result = run(queryConfig)
    if (!callback) return result
    result.then(
      (r) => callback!(null, r),
      (err) => callback!(err),
    )
    return undefined
  }

  async function run(queryConfig: PgQueryConfig): Promise<PgResult | PgResult[]> {
    if (!live.isReady()) await live.ready(runOurs)
    const statement = live.statementFor(client, queryConfig.text, queryConfig.values ?? [])
    let result: PgResult | PgResult[]
    try {
      result = await query(sent(queryConfig, statement))
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
    const followUp = statement.succeeded(takeHiddenValues(result.rows, hidden, queryConfig.rowMode === 'array'))
    if (followUp) runFollowUp(followUp)
    if (!statement.returnsRows) result.rows.length = 0
    return result
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
      const statement = live.statementFor(client, submittable.text, submittable.values ?? [])
      if (statement.text !== submittable.text) {
        submittable.text = statement.text
        if (submittable.name) submittable.name += HIDDEN_PREFIX
      }
      if (begin) begin(statement)
      // Rows reach the caller as events here: can't strip what the statement added.
      else if (statement.hiddenColumns > 0)
        throw new Error('Unreachable: a rewritten statement on a non-cursor submittable')
      else statement.succeeded([])
      return query(submittable)
    }
    if (live.isReady()) return send()
    // The schema isn't known yet: a `pg-cursor` queues reads until it is submitted.
    void live.ready(runOurs).then(send, (err) => submittable.handleError?.(err))
    return submittable
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
      const followUp = statement.succeeded(hiddenRows)
      if (followUp) runFollowUp(followUp)
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
