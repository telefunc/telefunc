export { capturePg }
export type { PgPool, PgClient }

import type { LiveDatabase, Statement } from '../liveDatabase.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'

// The parts of `pg` (node-postgres) used here, so that `pg` stays the app's dependency.
type PgQueryConfig = { text: string; values?: unknown[]; name?: string; rowMode?: 'array'; [key: string]: unknown }
type PgResult = { rows: unknown[]; fields: { name: string }[]; [key: string]: unknown }
type PgCallback = (err: Error | null, result?: PgResult | PgResult[]) => void
type PgClient = { query: (...args: any[]) => any }
type PgPool = { on(event: 'connect', listener: (client: PgClient) => void): unknown; totalCount: number }
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
  if ('totalCount' in database) database.on('connect', (client) => captureClient(client, live))
  else captureClient(database, live)
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
    statement.succeeded(takeHiddenValues(result.rows, hidden, queryConfig.rowMode === 'array'))
    if (!statement.returnsRows) result.rows.length = 0
    return result
  }

  function submit(submittable: Submittable): Submittable {
    if (live.isReady()) return query(captureSubmittable(submittable))
    // The schema isn't known yet: a `pg-cursor` queues reads until it is submitted.
    void live.ready(runOurs).then(
      () => query(captureSubmittable(submittable)),
      (err) => submittable.handleError?.(err),
    )
    return submittable
  }

  function captureSubmittable(submittable: Submittable): Submittable {
    const statement = live.statementFor(client, submittable.text, submittable.values ?? [])
    if (statement.text !== submittable.text) {
      submittable.text = statement.text
      if (submittable.name) submittable.name += HIDDEN_PREFIX
    }
    const read = submittable.read?.bind(submittable)
    const close = submittable.close?.bind(submittable)
    if (!read || !close) {
      // Rows reach the caller as events here: can't strip what the statement added.
      if (statement.hiddenColumns > 0) throw new Error('Unreachable: a rewritten statement on a non-cursor submittable')
      statement.succeeded([])
      return submittable
    }
    const hiddenRows: (string | null)[][] = []
    let hidden: string[] | undefined
    let settled = false
    const settle = (err?: Error | null) => {
      if (settled) return
      settled = true
      if (err) statement.failed()
      else statement.succeeded(hiddenRows)
    }
    submittable.read = (count, callback) =>
      read(count, (err, rows, result) => {
        if (err) {
          settle(err)
          return callback(err, rows, result)
        }
        hidden ??= takeHiddenFields(submittable._result?.fields ?? [], statement)
        hiddenRows.push(...takeHiddenValues(rows, hidden, Array.isArray(rows[0])))
        if (rows.length < count || rows.length === 0) settle()
        callback(err, statement.returnsRows ? rows : [], result)
      })
    submittable.close = (callback) => {
      settle()
      return close(callback)
    }
    return submittable
  }
}

function sent(config: PgQueryConfig, statement: Statement): PgQueryConfig {
  if (statement.text === config.text) return config
  // A prepared statement's name is bound to its text on the connection.
  return { ...config, text: statement.text, ...(config.name ? { name: config.name + HIDDEN_PREFIX } : {}) }
}

function isSubmittable(value: unknown): value is Submittable {
  return typeof value === 'object' && value !== null && typeof (value as Submittable).submit === 'function'
}
