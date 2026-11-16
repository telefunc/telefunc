export { capturePg, pgConnectionTarget }
export type { PgPool, PgClient }

import { AsyncResource } from 'node:async_hooks'
import { MAX_RESULT_ROWS, type FollowUp, type LiveDatabase, type Reservation, type Statement } from '../liveDatabase.js'
import { processWide } from '../processWide.js'
import { inOneTransaction, sendWithBeforeCommit } from './beforeCommit.js'
import { takeHiddenFields, takeHiddenValues } from './hiddenColumns.js'
import { HIDDEN_PREFIX } from './statement.js'
import { Turns } from './turns.js'

// The parts of `pg` (node-postgres) used here, so that `pg` stays the app's dependency.
type PgQueryConfig = {
  text: string
  values?: unknown[]
  name?: string
  rowMode?: 'array'
  [RESERVATION]?: Reservation
  [key: string]: unknown
}
type PgResult = { rows: unknown[]; fields: { name: string }[]; command?: string; [key: string]: unknown }
/** `pg`'s `Result` class: it parses a row's text values with the type parsers it was given. */
type PgResultClass = new (
  rowMode: 'array' | undefined,
  types: unknown,
) => PgResult & { addFields(fields: unknown[]): void; parseRow(row: unknown[]): unknown; addRow(row: unknown): void }
/** A read's result for the identical reads that joined it: its values as text, and the type parsers of the client it
 *  ran on, for the callers that don't bring their own. */
type SharedPgResult = { result: PgResult; types: unknown }
type PgCallback = (err: Error | null, result?: PgResult | PgResult[]) => void
type PgClient = {
  query: (...args: any[]) => any
  /** Where it connects, as `pg` resolved its configuration: the connection string, else its options, `PG*` variables,
   *  defaults. */
  connectionParameters?: { host: string; port: number }
  /** What it starts its session with: user, database, `options` (`-c role=…`), application name, timeouts. */
  getStartupConf?: () => Record<string, string>
}
/** A client checked out of a `pg.Pool`, until it calls `release()`. */
type CheckedOut = PgClient & { release: (err?: Error | boolean) => void }
type PgPool = {
  on(event: 'connect', listener: (client: PgClient) => void): unknown
  connect: (callback?: (...args: any[]) => void) => any
  query: (...args: any[]) => any
  totalCount: number
  /** The class of the clients it opens, given `options`. */
  Client?: new (
    options: unknown,
  ) => PgClient
  /** What it opens its clients with: `types`, the type parsers they parse values with. */
  options?: { types?: object }
}
/** A `pg-cursor` (Prisma reads and writes through one), or another object `pg` submits. */
type Submittable = {
  submit: (connection: unknown) => void
  text: string
  values?: unknown[]
  name?: string
  /** Without `callback`, returns a promise of the rows. */
  read?: (count: number, callback?: (err: Error | null, rows: unknown[], result: unknown) => void) => unknown
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
/** The follow-up queries of ours a client still runs, one after the other; settles once they all ended. */
const followingUp = new WeakMap<PgClient, Promise<void>>()
/** What decides how clients parse values, numbered (`rowFormOf()`). */
const parsings = processWide('pgParsings', () => ({ numbers: new WeakMap<object, number>(), count: 0 }))

/** Captures every statement of a `pg.Pool` (each of its clients) or a `pg.Client`. */
function capturePg(database: PgPool | PgClient, live: LiveDatabase): void {
  // A client of no pool doesn't tell its `types`: its reads share only with its own.
  if (!('totalCount' in database)) return captureClient(database, live, rowFormOf(database))
  const rowForm = rowFormOf(database.options?.types)
  database.on('connect', (client) => captureClient(client, live, rowForm))
  checkOutForCaller(database)
  shareReadsBeforeConnecting(database, live, rowForm)
}

/** What `database` connects to, and how its sessions start. A pool tells through a client it would open, not
 *  connected. */
function pgConnectionTarget(database: PgPool | PgClient): string | undefined {
  const client = 'totalCount' in database ? database.Client && new database.Client(database.options) : database
  const parameters = client?.connectionParameters
  const startup = client?.getStartupConf?.()
  return parameters && startup && `${parameters.host}:${parameters.port} ${JSON.stringify(startup)}`
}

/** A live read sent with `pool.query()` joins an identical one before waiting for a connection: N clients refetching
 *  one query wait for one connection, not N. */
function shareReadsBeforeConnecting(pool: PgPool, live: LiveDatabase, rowForm: string): void {
  const query = pool.query.bind(pool)
  pool.query = (...args: unknown[]) => {
    const [config, values, callback] = args
    const queryConfig = typeof config === 'string' ? { text: config } : (config as PgQueryConfig)
    const shared =
      !isSubmittable(config) &&
      isShareable(queryConfig) &&
      live.reserveRead(queryConfig.text, Array.isArray(values) ? values : (queryConfig.values ?? []), rowForm)
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

/** How the reads of clients parsing values with `parsing` return rows (`undefined`: `pg`'s own type parsers). A read
 *  shares the text values of its result only with reads of clients that parse them alike (`parsedFor()`). */
function rowFormOf(parsing: object | undefined): string {
  if (!parsing) return 'pg'
  let number = parsings.numbers.get(parsing)
  if (number === undefined) parsings.numbers.set(parsing, (number = parsings.count++))
  return `pg:${number}`
}

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

/**
 * A waiting `connect(callback)` (each `pool.query()` makes one) is called back in the caller's context, not that of
 * whoever frees a client: else its statements would run in that caller's telefunction call, and its reads would be
 * watched for the wrong client. And the client goes back to the pool only once our follow-up queries ended, so that
 * no one else's query is sent while one of ours runs.
 */
function checkOutForCaller(pool: PgPool): void {
  const connect = pool.connect.bind(pool)
  pool.connect = (callback) => {
    if (!callback) return connect().then(releasedAfterFollowUps)
    return connect(
      AsyncResource.bind((err: Error | undefined, client: CheckedOut | undefined, release: unknown) => {
        if (!client) return callback(err, client, release)
        releasedAfterFollowUps(client)
        callback(err, client, client.release)
      }),
    )
  }
}

/** Makes the `release()` the pool gave `client` for this checkout wait for our follow-up queries. */
function releasedAfterFollowUps(client: CheckedOut): CheckedOut {
  const release = client.release
  client.release = (err) => {
    const pending = followingUp.get(client)
    if (!pending) return release(err)
    void pending.then(() => release(err))
  }
  return client
}

function captureClient(client: PgClient, live: LiveDatabase, rowForm: string): void {
  if (CAPTURED in client) return
  Object.defineProperty(client, CAPTURED, { value: true, configurable: true })
  const query = client.query.bind(client)
  const turns = new Turns()
  const runOurs = async (text: string) =>
    ((await query(text)) as PgResult).rows.map((row: any) => row.catalog)[0] as string
  /** A query of ours: its rows, as arrays of text. */
  const sendOurs = async (text: string) =>
    ((await query({ text, rowMode: 'array' })) as PgResult).rows as (string | null)[][]
  /** Once our follow-up queries ended and the schema is known: what the caller sends next comes after both. */
  const free = async () => {
    await followingUp.get(client)
    if (!live.isReady()) await live.ready(runOurs)
  }
  client.query = (config: unknown, values?: unknown, callback?: PgCallback) => {
    if (isSubmittable(config)) return submit(config)
    if (typeof values === 'function') {
      callback = values as PgCallback
      values = undefined
    }
    const { [RESERVATION]: reservation, ...queryConfig }: PgQueryConfig =
      typeof config === 'string' ? { text: config } : { ...(config as PgQueryConfig) }
    if (Array.isArray(values)) queryConfig.values = values
    return answer(
      turns.take(client, (planned) => run(queryConfig, reservation, planned)),
      callback,
    )
  }

  async function run(
    queryConfig: PgQueryConfig,
    reservation: Reservation | undefined,
    planned: (statement: Statement) => void,
  ): Promise<PgResult | PgResult[]> {
    await free()
    // Its steps run on this client, in this turn: nothing the caller sends next comes between them.
    const steps = !queryConfig.values?.length && live.oneTransactionSteps(queryConfig.text)
    if (steps)
      return inOneTransaction(
        live,
        client,
        steps,
        (text) => run({ ...queryConfig, text }, undefined, () => {}) as Promise<PgResult>,
      )
    const plan = () => {
      const statement = live.statementFor(client, queryConfig.text, queryConfig.values ?? [], {
        sharing: isShareable(queryConfig) ? { rowForm, reservation } : undefined,
      })
      planned(statement)
      return statement
    }
    let statement = plan()
    while (statement.joined) {
      const joined = await statement.joined
      if (joined) return parsedFor(joined.result as SharedPgResult, queryConfig)
      // The read it joined failed: it runs on its own.
      statement = plan()
    }
    if (statement.asks) {
      await sendFollowUp(statement.asks)
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
    runFollowUps(statement.succeeded(takeHiddenValues(result.rows, hidden, config.rowMode === 'array'), result.command))
    if (!statement.returnsRows) result.rows.length = 0
    if (!statement.leads) return result
    // Text values, parsed by each caller: nothing they parse is shared.
    const shared: SharedPgResult = { result, types: client }
    statement.answered(shared, () => shared)
    return parsedFor(shared, queryConfig)
  }

  /** Sent one after the other, after those still running, and before anything the caller sends next (`free()`): the
   *  caller's result doesn't wait for them. */
  function runFollowUps(followUps: FollowUp[]): void {
    if (followUps.length === 0) return
    const running = sendInOrder(followingUp.get(client), followUps)
    followingUp.set(client, running)
    void running.then(() => {
      if (followingUp.get(client) === running) followingUp.delete(client)
    })
  }

  async function sendInOrder(previous: Promise<void> | undefined, followUps: FollowUp[]): Promise<void> {
    await previous
    for (const followUp of followUps) await sendFollowUp(followUp)
  }

  /** Runs a query of ours: `done` gets its rows, or `undefined` when it failed. */
  async function sendFollowUp({ text, values, done }: FollowUp): Promise<void> {
    try {
      done(((await query({ text, values, rowMode: 'array' })) as PgResult).rows as (string | null)[][])
    } catch {
      done(undefined)
    }
  }

  function submit(submittable: Submittable): Submittable {
    // Wrapped now: the caller may read before the statement is planned, and rows only come after.
    const cursor = isCursor(submittable) ? captureCursor(submittable) : undefined
    const send = async () => {
      const values = submittable.values ?? []
      if (!cursor) {
        // Rows reach the caller as events (an event-based `pg.Query`): run as written, nothing to strip.
        settleWhenDone(submittable, live.statementFor(client, submittable.text, values, { rewrite: false }))
        return query(submittable)
      }
      let statement = live.statementFor(client, submittable.text, values)
      if (statement.asks) {
        await sendFollowUp(statement.asks)
        statement = live.statementFor(client, submittable.text, values)
      }
      if (statement.text !== submittable.text) {
        submittable.text = statement.text
        if (submittable.name) submittable.name += HIDDEN_PREFIX
      }
      cursor.begin(statement)
      return query(submittable)
    }
    void turns.take(client, () => sendInTurn(submittable, send, cursor?.settled))
    return submittable
  }

  /** Sends `submittable` with `send`; its turn ends once its statement `settled`, else once the client is done with it. */
  async function sendInTurn(
    submittable: Submittable,
    send: () => Promise<unknown>,
    settled = doneWith(submittable),
  ): Promise<void> {
    try {
      // A `pg-cursor` queues reads until it is submitted.
      await free()
      await send()
    } catch (err) {
      return submittable.handleError?.(err as Error)
    }
    await settled
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
      if (!failed) runFollowUps(statement.succeeded([]))
      handleReadyForQuery?.(connection)
    }
  }

  /** Strips the hidden columns from the rows of a cursor as they are read; returns how to give it its statement, and
   *  when the statement settled: once its last rows were read (`pg` is ready for the next query before they are), it
   *  was closed, or it failed. */
  function captureCursor(cursor: Cursor): { begin: (statement: Statement) => void; settled: Promise<void> } {
    const read = cursor.read.bind(cursor)
    const close = cursor.close.bind(cursor)
    const handleError = cursor.handleError?.bind(cursor)
    let statement: Statement | undefined
    const hiddenRows: (string | null)[][] = []
    let hidden: string[] | undefined
    let ended: (() => void) | undefined
    const settled = new Promise<void>((resolve) => (ended = resolve))
    const settle = (err?: Error | null) => {
      if (!ended || !statement) return
      ended()
      ended = undefined
      if (err) return statement.failed()
      runFollowUps(statement.succeeded(hiddenRows))
    }
    cursor.read = (count, callback) => {
      if (!callback)
        return new Promise((resolve, reject) => cursor.read(count, (err, rows) => (err ? reject(err) : resolve(rows))))
      return read(count, (err, rows, result) => {
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
    }
    let closed = false
    cursor.close = (callback) => {
      closed = true
      settle()
      return close(callback)
    }
    // Also when no read waits for its rows.
    cursor.handleError = (err, connection) => {
      settle(err)
      handleError?.(err, connection)
    }
    const begin = (s: Statement) => {
      statement = s
      // Closed before it was sent: it returns nothing.
      if (closed) settle()
    }
    return { begin, settled }
  }
}

/** Settles once the client is done with `submittable`: it's ready for the next query, or its connection failed. */
function doneWith(submittable: Submittable): Promise<void> {
  return new Promise((resolve) => {
    const { handleReadyForQuery, handleError } = submittable
    submittable.handleReadyForQuery = (connection) => {
      resolve()
      handleReadyForQuery?.call(submittable, connection)
    }
    submittable.handleError = (err, connection) => {
      resolve()
      handleError?.call(submittable, err, connection)
    }
  })
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
