export { LiveDatabase, MAX_RESULT_ROWS }
export type { Statement, FollowUp, LiveOptions, Sharing, Reservation, Engine }

import type { BroadcastTransport } from 'telefunc'
import {
  accessAnswered,
  ACCESS_QUERY,
  ROLE_QUERY,
  tablesChangedBy,
  type Access,
  type Catalog,
  type Table,
} from './catalog.js'
import { ChangeFeed } from './changeFeed.js'
import { ofRows, READS_EVERYTHING, type ChangedTables, type Plan, type WritePlan } from './capture/statement.js'
import { ConnectionRoles, roleChangeOf } from './capture/roles.js'
import { named, Transactions, UNNAMED_TABLES } from './capture/transactions.js'
import { currentQuery, type LiveQuery } from './liveQuery.js'
import { rowOf, type Row } from './match/condition.js'
import type { Change } from './match/liveRead.js'
import { operandValues, type OperandValue } from './match/operands.js'
import type { ReadShape } from './match/readShape.js'
import { Router } from './match/router.js'
import { keyOfValues, SharedReads, WatchedRead, type Joined } from './sharedReads.js'

type LiveOptions = {
  /** Carries changes to the other servers.
   *  @default the installed Broadcast backend: Redis once installed, Cloudflare's on Workers, else this process */
  transport?: BroadcastTransport
}

/** How a driver runs one statement: send `text`, give back each row's last `hiddenColumns` values (as text) to
 *  `succeeded` and the rest to the caller (no rows at all unless `returnsRows`), or call `failed`. Run the follow-ups
 *  either returns next, in order, on the same connection, before the caller can send anything else. */
type Statement = {
  text: string
  /** The values to send with `text`, when they aren't the caller's. */
  values?: unknown[]
  hiddenColumns: number
  returnsRows: boolean
  /** `command`: the command tag Postgres answered, when the driver tells (a `COMMIT` that rolled back answers
   *  `ROLLBACK`). */
  succeeded(hiddenRows: (string | null)[][], command?: string): FollowUp[]
  failed(): FollowUp[]
  /** A query of ours to send right before `text`, in the same transaction with nothing between: its rows go to `done`
   *  before `succeeded` (a write's rows before it). A driver that doesn't send it makes the write count less precisely. */
  readsFirst?: FollowUp
  /** A write that runs as a role its connection doesn't know yet: run this query of ours on the connection first,
   *  then plan the statement again, which may then capture its rows. A driver that doesn't sends it as is. */
  asks?: FollowUp
  /** Identical reads wait for this one's result: give it to them with `answered()`. */
  leads: boolean
  /** Gives the result to the identical reads that joined this one; `copy` makes each its own. */
  answered<R>(result: R, copy: (result: R) => R): void
  /** An identical read is running: don't send this one, give the caller a copy of that one's result. */
  joined?: Joined
  /** A read outside a transaction that sets no role: what is planned after it on its connection may be planned before
   *  it ran. */
  keepsSession?: boolean
  /** A read outside `.live()`: sent as written, and `succeeded()` may come before its rows (as an iterator gives them). */
  unwatched?: boolean
  /** Queries of ours naming the tables a transaction changed without the catalog naming them, sent on the same
   *  connection right before it commits, with nothing else between: when one fails, the transaction rolls back and the
   *  caller gets its error. `ownTransaction`: the statement runs in a transaction of ours (`BEGIN`, the statement, the
   *  queries, `COMMIT`); otherwise it is the `COMMIT` they precede. A driver that doesn't send them leaves those
   *  changes counting for every table. */
  beforeCommit?: { queries: FollowUp[]; ownTransaction: boolean }
}

/** How a driver that can hand its result to identical reads (`Statement.answered()`) returns rows: reads share only
 *  with reads whose rows come in the same form. */
type Sharing = { rowForm: string; reservation?: Reservation }

/** A read that leads identical ones before its driver got a connection (`reserveRead()`). */
type Reservation = { read: WatchedRead; lead: ReturnType<SharedReads['lead']>; failed(): void }

/** A query whose rows (each value as text, in an array) go to `done`; `undefined` when it failed. */
type FollowUp = { text: string; values: unknown[]; done(rows: (string | null)[][] | undefined): void }

type Rows = (string | null)[][]

/** One kind of database: where its catalog comes from, and how its statements are planned. */
type Engine = {
  /** Runs the queries the catalog needs through `query`, on the connection about to be used. */
  loadCatalog(query: (text: string) => Promise<string>): Promise<Catalog>
  /** The same, on a driver that answers at once. */
  loadCatalogSync?(query: (text: string) => string): Catalog
  /** Each connection logs the rows its statements change (SQLite), once `setup` ran on it for the loaded catalog:
   *  `triggers` answers what a previous `setup` left there, `count` the connection's count of changes, which keys the
   *  log's rows, `read` the rows after a count, whose changes `changesOf` tells (`undefined` when the log may no longer
   *  hold some of them). */
  changeLog?: {
    triggers: string
    setup(catalog: Catalog, ours: string[]): string[]
    count: string
    read: string
    changesOf(rows: Rows, after: number, catalog: Catalog): Change[] | undefined
  }
  plan(text: string, catalog: Catalog): Plan
  /** The statements of text that may hold several, each its own text; `undefined` when the parser can't read it. */
  split?(text: string): string[] | undefined
  /** The database lives in this process alone (PGlite): no other process hears of its changes. */
  inProcess?: boolean
  /** A statement that fails inside a transaction makes it roll back (Postgres). On SQLite, the transaction goes on,
   *  and what the statement changed before failing may stay (`OR FAIL`, the statements before it in a text). */
  failureAbortsTransaction: boolean
}

/** The tables the transaction locked as a write does, or more strongly (`TRUNCATE`): an explicit `LOCK TABLE`, which
 *  writes nothing, lists its table too. */
const LOCKED_TABLES = `select n.nspname || '.' || c.relname from pg_locks l
  join pg_class c on c.oid = l.relation join pg_namespace n on n.oid = c.relnamespace
  where l.locktype = 'relation' and l.pid = pg_backend_pid() and c.relkind in ('r', 'p', 'v', 'm', 'f')
    and l.mode in ('RowExclusiveLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock')`
const MAX_PLANS = 10_000
const MAX_KNOWN_OPERANDS = 10_000
/** A read returning more rows keeps no result keys: any change to a matching row counts. */
const MAX_RESULT_ROWS = 10_000
/** A write changing more rows counts as changing any row of its table: matching each row against the live reads
 *  would cost the writer more than the refetches it spares (a 100,000-row `UPDATE` took twice as long). */
const MAX_CHANGED_ROWS = 10_000

/** Everything one wrapped database shares between its connections: what it knows of the schema, the live reads,
 *  open transactions, and the other servers. */
class LiveDatabase {
  private readonly engine: Engine
  private catalog: Catalog | undefined
  private loadingCatalog: Promise<Catalog> | undefined
  /** Connections whose change log follows the loaded catalog (`Engine.changeLog`): the count its rows were read up to. */
  private loggedUpTo = new WeakMap<object, number>()
  private plans = new Map<string, Plan>()
  private readonly router = new Router()
  private readonly transactions = new Transactions((changes) => this.announce(changes))
  private readonly transport: BroadcastTransport | undefined
  private feed: ChangeFeed | undefined
  private readonly shared = new SharedReads()
  /** A statement changed a session: connections may answer the same read differently from now on. */
  private sessionsDiffer = false
  /** A statement may have changed a session's `search_path`, if only for its transaction: an unqualified name may mean
   *  another table than the catalog's `search_path` resolves it to. */
  private pathsDiffer = false
  /** The role each connection's statements run as, from the catalog's on. */
  private roles: ConnectionRoles | undefined
  /** What each role sees of the tables (`Access`), by role name: learned by a query run as that role. */
  private access = new Map<string, Map<string, Access>>()
  /** The values Postgres gave a read's operands, by its text and parameters: an identical read doesn't ask again. */
  private knownOperands = new Map<string, OperandValue[]>()
  /** Connections whose open transaction changed the schema: rolled back, the catalog loaded since is wrong. */
  private schemaChangedIn = new WeakSet<object>()

  constructor(options: LiveOptions, engine: Engine) {
    this.transport = options.transport
    this.engine = engine
  }

  /** Whether `statementFor()` can run: the schema is loaded. */
  isReady(): boolean {
    return this.catalog !== undefined
  }

  /** Loads what `statementFor()` needs; `query` runs our catalog query on the connection about to be used. */
  async ready(query: (text: string) => Promise<string>): Promise<void> {
    if (this.catalog) return
    this.loadingCatalog ??= this.engine.loadCatalog(query)
    try {
      this.loaded(await this.loadingCatalog)
    } finally {
      // A failure fails the statement waiting for it; the next one tries again.
      this.loadingCatalog = undefined
    }
  }

  /** `ready()` on a driver that answers at once, before each statement on `connection`. Returns the statements that
   *  set up its change log for the loaded catalog, to run on it next: once per connection and catalog. */
  readySync(connection: object, query: (text: string) => string): string[] {
    if (!this.catalog) this.loaded(this.engine.loadCatalogSync!(query))
    const log = this.engine.changeLog
    if (!log || this.loggedUpTo.has(connection)) return []
    this.loggedUpTo.set(connection, Number(query(log.count)))
    return log.setup(this.catalog!, JSON.parse(query(log.triggers)))
  }

  /** The statements of `text`, once `isReady()`, when sent one by one on any connection they do what `text` does: each
   *  reads or writes rows, none begins a transaction, changes the schema or sets a `PRAGMA`. */
  statementsOf(text: string): string[] | undefined {
    const statements = this.engine.split?.(text)
    return statements?.every((statement) => readsOrWritesRows(this.planOf(statement, this.catalog!)))
      ? statements
      : undefined
  }

  /** The statements of `text`, once `isReady()`, when it holds both writes and transaction statements: run one by one,
   *  each write's changes go to the transaction and the savepoint it was made in. */
  transactionSteps(text: string): string[] | undefined {
    const plan = this.planOf(text, this.catalog!)
    if (plan.kind !== 'several' || !plan.plans.some((p) => p.kind === 'transaction')) return undefined
    const writes = plan.plans.some((p) => p.kind === 'logged' || p.kind === 'write' || p.kind === 'unknownWrite')
    return writes ? this.engine.split?.(text) : undefined
  }

  /** Called before each statement, on the connection that runs it, once `isReady()`. Without `rewrite`, the text is
   *  sent as is: its writes count as changes to their whole tables, its reads can't be bounded by their result. With
   *  `sharing`, a live read may join an identical one running (`Statement.joined`). */
  statementFor(
    connection: object,
    text: string,
    values: readonly unknown[],
    { rewrite = true, sharing }: { rewrite?: boolean; sharing?: Sharing } = {},
  ): Statement {
    return this.planned(connection, text, values, rewrite, currentQuery(), sharing)
  }

  /** For a driver that gets a connection after the call (a `pg.Pool`): a live read joins an identical one running, or
   *  reserves its place so that those arriving while it waits for a connection join it. */
  reserveRead(
    text: string,
    values: readonly unknown[],
    rowForm: string,
  ): { joined: Joined } | { reservation: Reservation } | undefined {
    const live = currentQuery()
    if (!live || !this.isReady()) return undefined
    const plan = this.planOf(text, this.catalog!)
    const key = plan.kind === 'read' && plan.shareable ? this.shareKey(text, values, undefined, rowForm) : undefined
    if (!key || plan.kind !== 'read') return undefined
    const joined = this.shared.join(key, live)
    if (joined) return { joined }
    const read = this.watched(plan.shape, text, values, live)
    const lead = this.shared.lead(key, read)
    let settled = false
    const failed = () => {
      if (settled) return
      settled = true
      read.drop()
      lead.failed()
    }
    return { reservation: { read, lead, failed } }
  }

  /** For a statement whose text is known only once it was sent (a postgres.js template with helpers): until then, the
   *  current `.live()` query counts as changed by any write. */
  statementWithTextLater(connection: object): { sent(text: string, values: readonly unknown[]): Statement } {
    const live = currentQuery()
    const anything = live && this.watched(READS_EVERYTHING, '', [], live)
    return {
      sent: (text, values) => {
        const statement = this.planned(connection, text, values, false, live, undefined)
        // What committed while it ran is judged now; the statement's own read takes over.
        if (live && anything) {
          anything.read.resolve(undefined)
          live.remove(anything)
        }
        return statement
      },
    }
  }

  /** After each statement on `connection`, for a driver that tells whether the connection is in a transaction
   *  (`open`): SQLite rolls one back by itself when some statements fail (`INSERT OR ROLLBACK`, a full disk), and a
   *  statement the parser can't read may begin or end one. `ended`: how it ended, if it did. */
  transactionIs(connection: object, open: boolean, ended: 'commit' | 'rollback'): void {
    if (open === this.transactions.isOpen(connection)) return
    if (open) this.transactions.apply(connection, 'begin')
    else this.transactionEnded(connection, ended, false)
  }

  /** `live`: the `.live()` query the statement runs in. */
  private planned(
    connection: object,
    text: string,
    values: readonly unknown[],
    rewrite: boolean,
    live: LiveQuery | undefined,
    sharing: Sharing | undefined,
  ): Statement {
    let plan = withRoleSet(this.planOf(text, this.catalog!), values)
    // A read planned after it on its connection runs after it: it doesn't join a read running before it.
    if (plan.kind !== 'read' || plan.role) this.shared.stopJoining(connection)
    const sessionsStart = changesSession(plan) && !this.sessionsDiffer
    const pathsStart = (sessionsStart || changesPath(plan)) && !this.pathsDiffer
    if (sessionsStart) this.sessionsStartDiffering()
    if (pathsStart) {
      this.pathsStartDiffering()
      // The text's other statements may name tables by a `search_path` it changes.
      plan = withRoleSet(this.planOf(text, this.catalog!), values)
    }
    switch (plan.kind) {
      case 'read':
        return this.read(rewrite ? plan : { ...plan, liveText: undefined }, text, values, connection, live, sharing)
      case 'write':
        // Sent as written, a write changes its table as a whole.
        if (!rewrite || !this.widens(plan, connection)) break
        return this.widenedWrite(connection, plan, values)
      case 'transaction':
        if (plan.action === 'commit') return this.commit(connection, text, plan.chain)
        break
      case 'unknownWrite':
        if (plan.unread && live) return this.readOfAnything(connection, text, plan, live)
        if (rewrite && isUnnamed(plan) && !this.transactions.isOpen(connection))
          return this.inOwnTransaction(connection, text, plan)
    }
    const sent = this.statement(
      connection,
      text,
      0,
      () => this.changesOf(plan, connection),
      () => this.keptChanges(plan, connection),
    )
    const statement = this.learningAccess(connection, this.withChangeLog(connection, sent, plan))
    if (plan.kind !== 'write' || !rewrite || this.roles!.of(connection) !== null) return statement
    // The role, and what it sees unless known: one round trip.
    const known = JSON.stringify([...this.access.keys()])
    const asks = {
      text: ROLE_QUERY,
      values: [known],
      done: (rows: Rows | undefined) => this.roleAnswered(connection, rows),
    }
    return { ...statement, asks }
  }

  /** On a connection with a change log (SQLite): a write reads the rows logged since the last ones read, once it ran,
   *  whether it succeeded or failed. Not read, the write changed what it may have changed. */
  private withChangeLog(connection: object, statement: Statement, plan: Plan): Statement {
    const log = this.engine.changeLog
    if (!log || !(plan.kind === 'logged' || plan.kind === 'unknownWrite' || plan.kind === 'several')) return statement
    const catalog = this.catalog!
    const read = (): FollowUp => {
      const after = this.loggedUpTo.get(connection) ?? 0
      return {
        text: log.read,
        values: [after],
        done: (rows) => {
          this.transactions.record(connection, (rows && log.changesOf(rows, after, catalog)) ?? unloggedChanges(plan))
          // Unless the write changed the schema: the log is set up again, from the count then.
          if (rows?.length && this.loggedUpTo.has(connection)) this.loggedUpTo.set(connection, Number(rows.at(-1)![5]))
        },
      }
    }
    return {
      ...statement,
      succeeded: (rows, command) => [...statement.succeeded(rows, command), read()],
      failed: () => [...statement.failed(), read()],
    }
  }

  /** Files `ROLE_QUERY`'s answer; nothing when it failed (the role stays unknown, and the next write asks again). */
  private roleAnswered(connection: object, rows: Rows | undefined): void {
    const answer = rows?.[0]?.[0]
    if (!answer) return
    const { role, access } = accessAnswered(answer)
    this.roles!.answered(connection, role)
    if (access) this.access.set(role, access)
  }

  /** Whether `plan`'s widened `RETURNING` gives the same answer as the write alone, run as its connection's role: the
   *  role may `SELECT` what it adds (the whole table for an upsert's `xmax` before Postgres 18), and row level security
   *  doesn't filter it, unless the write has a `RETURNING` of its own, filtered the same. */
  private widens(plan: WritePlan, connection: object): boolean {
    const role = this.roles!.of(connection)
    const access = role === null ? undefined : this.access.get(role)?.get(plan.table.id)
    if (!access) return false
    const readable = access.readable === 'table' || (access.readable === 'columns' && !plan.images.includes('inserted'))
    return readable && (!access.rowSecurity || plan.hadReturning)
  }

  /** Learns what its connection's role sees of the tables, right after `statement`, when that isn't known yet. */
  private learningAccess(connection: object, statement: Statement): Statement {
    return {
      ...statement,
      succeeded: (rows) => {
        const followUps = statement.succeeded(rows)
        const role = this.roles!.of(connection)
        if (role === null || this.access.has(role)) return followUps
        return [...followUps, { text: ACCESS_QUERY, values: [], done: (rows) => this.learnAccess(rows) }]
      },
    }
  }

  /** Files `ACCESS_QUERY`'s answer under the role it ran as; nothing when it failed (the next statement asks again). */
  private learnAccess(rows: Rows | undefined): void {
    const answer = rows?.[0]?.[0]
    if (!answer) return
    const { role, access } = accessAnswered(answer)
    if (access) this.access.set(role, access)
  }

  /** A `COMMIT`: when its transaction changed tables the catalog can't name, queries of ours name them first. `chain`:
   *  the next transaction starts once it succeeds (a `COMMIT AND CHAIN` that fails ends without one). */
  private commit(connection: object, text: string, chain = false): Statement {
    let locked: string[] | undefined
    const queries = this.transactions.hasUnnamed(connection) ? namingQueries((tables) => (locked = tables)) : undefined
    return {
      ...this.statement(connection, text, 0, () => []),
      ...(queries ? { beforeCommit: { queries, ownTransaction: false } } : {}),
      succeeded: (_, command) => {
        if (locked) this.transactions.name(connection, locked)
        this.transactionEnded(connection, command === 'ROLLBACK' ? 'rollback' : 'commit', chain)
        return []
      },
      // On Postgres, the transaction is over: rolled back. SQLite keeps it open (a deferred foreign key failed).
      failed: () => {
        if (this.engine.failureAbortsTransaction) this.transactionEnded(connection, 'rollback', false)
        return []
      },
    }
  }

  /** A statement outside a transaction that may change tables the catalog can't name: it runs in a transaction of
   *  ours, whose locks name them. */
  private inOwnTransaction(connection: object, text: string, plan: Plan & { kind: 'unknownWrite' }): Statement {
    let locked: string[] | undefined
    const statement = this.statement(connection, text, 0, () => {
      const changes = this.changesOf(plan, connection)
      return locked ? named(changes, locked) : changes
    })
    return {
      ...statement,
      beforeCommit: { queries: namingQueries((tables) => (locked = tables)), ownTransaction: true },
    }
  }

  private read(
    plan: Plan & { kind: 'read' },
    text: string,
    values: readonly unknown[],
    connection: object,
    live: LiveQuery | undefined,
    sharing: Sharing | undefined,
  ): Statement {
    const keepsSession = !plan.role && !this.transactions.isOpen(connection)
    // A read outside `.live()` is sent as written.
    if (!live)
      return {
        ...this.statement(connection, text, 0, () => this.changesOf(plan, connection)),
        keepsSession,
        unwatched: true,
      }
    const key =
      sharing && !sharing.reservation && plan.shareable
        ? this.shareKey(text, values, connection, sharing.rowForm)
        : undefined
    const joined = key ? this.shared.join(key, live) : undefined
    if (joined) return { ...this.statement(connection, text, 0, () => []), joined, keepsSession }
    const reservation = sharing?.reservation
    const watched = reservation?.read ?? this.watched(plan.shape, text, values, live)
    const lead = reservation?.lead ?? (key ? this.shared.lead(key, watched, connection) : undefined)
    return {
      text: plan.liveText ?? text,
      ...(plan.liveText && plan.liveValues ? { values: plan.liveValues.map((i) => values[i]) } : {}),
      hiddenColumns: plan.liveText ? plan.shape.hiddenColumns.length : 0,
      returnsRows: true,
      succeeded: (hiddenRows) => {
        if (plan.role) this.roles!.changed(connection, plan.role)
        const rows = plan.liveText && hiddenRows.length <= MAX_RESULT_ROWS ? hiddenRows : undefined
        return this.resolve(watched, plan.shape, rows, text, values, connection)
      },
      failed: () => {
        if (reservation) reservation.failed()
        else {
          watched.drop()
          lead?.failed()
        }
        this.statementFailed(connection)
        return []
      },
      leads: lead !== undefined,
      answered: (result, copy) => lead?.answered(result, copy),
      keepsSession,
    }
  }

  /** A statement of a `.live()` query the parser can't read: watched as reading every table, so that any committed
   *  write refetches it. It isn't announced as a write as well: its own change would refetch it, again and again. */
  private readOfAnything(
    connection: object,
    text: string,
    plan: Plan & { kind: 'unknownWrite' },
    live: LiveQuery,
  ): Statement {
    const watched = this.watched(READS_EVERYTHING, '', [], live)
    return {
      text,
      hiddenColumns: 0,
      returnsRows: true,
      succeeded: () => {
        if (plan.role) this.roles!.changed(connection, plan.role)
        watched.resolve(undefined)
        return []
      },
      failed: () => {
        watched.drop()
        this.statementFailed(connection)
        return []
      },
      leads: false,
      answered: () => {},
    }
  }

  /** A live read `live` watches, filed in the router. */
  private watched(shape: ReadShape, text: string, values: readonly unknown[], live: LiveQuery): WatchedRead {
    const operandsKey = this.operandsKey(text, values)
    const operands = !shape.operandQuery ? [] : operandsKey ? this.knownOperands.get(operandsKey) : undefined
    const read = new WatchedRead(shape, values, operands, this.router, this.watchTables(shape))
    read.watch(live)
    return read
  }

  /** Gives a read its result once the queries of ours it needs answered: the state of its tally, and its operands'
   *  values unless known. Returns those queries, for the driver to run. */
  private resolve(
    watched: WatchedRead,
    shape: ReadShape,
    rows: Rows | undefined,
    text: string,
    values: readonly unknown[],
    connection: object,
  ): FollowUp[] {
    // Inside a transaction, a query of ours failing would fail the caller's transaction.
    if (this.transactions.isOpen(connection)) {
      watched.resolve(rows)
      return []
    }
    const followUps: FollowUp[] = []
    const ask = (query: { text: string; params: number[] }) =>
      new Promise<Rows | undefined>((done) =>
        followUps.push({ text: query.text, values: query.params.map((i) => values[i]), done }),
      )
    const { state, operandQuery } = shape
    const stateRows = rows && state && !(state.onlyWhenEmpty && rows.length > 0) ? ask(state) : undefined
    const operandRows = operandQuery && watched.read.operands === undefined ? ask(operandQuery) : undefined
    if (followUps.length === 0) watched.resolve(rows)
    else void this.resolveWhenAnswered(watched, shape, rows, stateRows, operandRows, this.operandsKey(text, values))
    return followUps
  }

  private async resolveWhenAnswered(
    watched: WatchedRead,
    shape: ReadShape,
    rows: Rows | undefined,
    stateRows: Promise<Rows | undefined> | undefined,
    operandRows: Promise<Rows | undefined> | undefined,
    operandsKey: string | undefined,
  ): Promise<void> {
    const [state, operandRow] = await Promise.all([stateRows, operandRows?.then((r) => r?.[0])])
    // Not answered, or not as asked: they stay unknown, and any change to the tables counts.
    const operands = operandRow && operandValues(shape.operands, operandRow)
    if (operands && operandsKey) {
      if (this.knownOperands.size >= MAX_KNOWN_OPERANDS) this.knownOperands.clear()
      this.knownOperands.set(operandsKey, operands)
    }
    watched.resolve(rows, state, operands)
  }

  /** What makes two reads' operands the same values; `undefined` when they may not be. */
  private operandsKey(text: string, values: readonly unknown[]): string | undefined {
    if (this.sessionsDiffer) return undefined
    const valuesKey = keyOfValues(values)
    return valuesKey === undefined ? undefined : `${valuesKey}\0${text}`
  }

  /** What makes two live reads return the same rows (`SharedReads`); `undefined` when they may not. */
  private shareKey(
    text: string,
    values: readonly unknown[],
    connection: object | undefined,
    rowForm: string,
  ): string | undefined {
    if (this.sessionsDiffer || (connection && this.transactions.isOpen(connection))) return undefined
    const valuesKey = keyOfValues(values)
    return valuesKey === undefined ? undefined : `${rowForm}\0${valuesKey}\0${text}`
  }

  /** `kept`: what the statement may have changed when it failed on SQLite (`keptChanges()`). */
  private statement(
    connection: object,
    text: string,
    hiddenColumns: number,
    changesOf: (rows: (string | null)[][]) => Change[],
    kept: () => Change[] = () => [],
  ): Statement {
    return {
      text,
      hiddenColumns,
      returnsRows: true,
      succeeded: (rows) => {
        this.transactions.record(connection, changesOf(rows))
        return []
      },
      failed: () => {
        this.statementFailed(connection, kept)
        return []
      },
      leads: false,
      answered: () => {},
    }
  }

  private statementFailed(connection: object, kept: () => Change[] = () => []): void {
    if (this.engine.failureAbortsTransaction) this.transactions.failed(connection)
    else this.transactions.record(connection, kept())
  }

  /** What a statement that failed on SQLite may still have changed: SQLite keeps the rows an `OR FAIL` statement
   *  changed before its conflict, and what the statements before the one that failed in a text did. */
  private keptChanges(plan: Plan, connection: object): Change[] {
    if (plan.kind === 'several') return plan.plans.flatMap((p) => this.keptChanges(p, connection))
    const writes = plan.kind === 'logged' || plan.kind === 'write' || plan.kind === 'unknownWrite'
    return writes ? this.changesOf(plan, connection) : []
  }

  /** Changes of a statement that isn't a single captured write; applies transaction statements and role changes. */
  private changesOf(plan: Plan, connection: object): Change[] {
    if ('role' in plan && plan.role) this.roles!.changed(connection, plan.role)
    switch (plan.kind) {
      case 'transaction':
        if (plan.action === 'commit' || plan.action === 'rollback')
          this.transactionEnded(connection, plan.action, plan.chain === true)
        else {
          this.transactions.apply(connection, plan.action, plan.savepoint)
          this.roles!.transaction(connection, plan.action)
        }
        return []
      case 'unknownWrite':
        if (plan.schemaChange) {
          this.forgetSchema()
          if (this.transactions.isOpen(connection)) this.schemaChangedIn.add(connection)
        }
        return unknownChanges(plan.tables)
      case 'write':
        return unknownChanges(ofRows(tablesChangedBy([[plan.table.id, undefined]], this.catalog!)))
      case 'logged':
        return unknownChanges(plan.also)
      case 'several':
        // Applied in order, after the whole text succeeded.
        return plan.plans.flatMap((p) => {
          const changes = this.changesOf(p, connection)
          this.transactions.record(connection, changes)
          return []
        })
      default:
        return []
    }
  }

  /** A transaction's end on `connection`; `chain`: the next one begins at once. */
  private transactionEnded(connection: object, action: 'commit' | 'rollback', chain: boolean): void {
    // A `COMMIT` of a transaction a statement failed in rolls it back.
    const ended = this.transactions.hasFailed(connection) ? 'rollback' : action
    if (this.schemaChangedIn.has(connection)) {
      this.schemaChangedIn.delete(connection)
      if (ended === 'rollback') this.forgetSchema()
    }
    this.transactions.apply(connection, ended)
    this.roles!.transaction(connection, ended)
    if (!chain) return
    this.transactions.apply(connection, 'begin')
    this.roles!.transaction(connection, 'begin')
  }

  /** `plan`, sent with images of the rows it writes, and the rows it will write as they are before it when the driver
   *  sends `readsFirst`. */
  private widenedWrite(connection: object, plan: WritePlan, values: readonly unknown[]): Statement {
    let before: Rows | undefined
    const sent = {
      ...this.statement(
        connection,
        plan.text,
        plan.images.length,
        (rows) => this.changesOfWrite(plan, rows, before),
        () => this.changesOf(plan, connection),
      ),
      ...(plan.params && { values: plan.params.map((i) => values[i]) }),
      returnsRows: plan.hadReturning,
    }
    if (!plan.readsFirst) return sent
    const { text, params } = plan.readsFirst
    const readsFirst = { text, values: params.map((i) => values[i]), done: (rows?: Rows) => void (before = rows) }
    return { ...sent, readsFirst }
  }

  /** `before`: what `readsFirst` read, when the driver sent it. */
  private changesOfWrite(plan: WritePlan, rows: Rows, before: Rows | undefined): Change[] {
    const whole = () => unknownChanges(ofRows(tablesChangedBy([[plan.table.id, undefined]], this.catalog!)))
    if (rows.length > MAX_CHANGED_ROWS || (before?.length ?? 0) > MAX_CHANGED_ROWS) return whole()
    // An upsert's `RETURNING` doesn't tell an inserted row from an updated one: the rows it conflicted with do.
    if (plan.readsFirst && plan.statement === 'insert' && !before) return whole()
    const changes = before ? pairedChanges(plan, rows, before) : this.returnedChanges(plan, rows)
    return [...changes, ...cascadedDeletes(plan, changes), ...unknownChanges(plan.alsoWrites)]
  }

  /** The changes of a write whose `RETURNING` gives all it knows of its rows. */
  private returnedChanges(plan: WritePlan, rows: Rows): Change[] {
    const changes: Change[] = []
    for (const row of rows) {
      const image = (which: WritePlan['images'][number]) => row[plan.images.indexOf(which)]
      const now = imageOf(image('new'), plan.table)
      const old = imageOf(image('old'), plan.table)
      // An upsert's `old` is all `NULL` when it inserted.
      const inserted =
        image('inserted') === 'true' ||
        (old !== undefined && plan.table.primaryKey.length > 0 && old[plan.table.primaryKey[0]!] === null)
      if (plan.statement === 'delete') changes.push({ table: plan.table.id, kind: 'delete', old: old! })
      else if ((plan.statement === 'insert' && plan.assigned.length === 0) || inserted)
        changes.push({ table: plan.table.id, kind: 'insert', new: now! })
      else changes.push({ table: plan.table.id, kind: 'update', old: old ?? assumedOld(now!, plan), new: now! })
    }
    return changes
  }

  private announce(changes: Change[]): void {
    this.receive(changes)
    this.feed!.publish(changes)
  }

  private planOf(text: string, catalog: Catalog): Plan {
    let plan = this.plans.get(text)
    if (!plan) {
      if (this.plans.size >= MAX_PLANS) this.plans.clear()
      plan = this.engine.plan(text, catalog)
      this.plans.set(text, plan)
    }
    return plan
  }

  private loaded(catalog: Catalog): void {
    this.catalog = catalog
    if (this.pathsDiffer) this.catalog.searchPath = undefined
    this.roles ??= new ConnectionRoles(this.catalog.role)
    this.access.set(this.catalog.role, this.catalog.access)
    // A database no other process opens is a channel of its own: another one in this process is another database.
    const database = (!this.engine.inProcess && this.catalog.database) || `in-process ${crypto.randomUUID()}`
    this.feed ??= new ChangeFeed(this.transport, database, (changes) => this.receive(changes))
  }

  private receive(changes: Change[]): void {
    for (const change of changes) this.router.route(change)
  }

  /** After a statement that may have changed its session: connections may answer the same read differently. */
  private sessionsStartDiffering(): void {
    this.sessionsDiffer = true
    this.knownOperands.clear()
  }

  /** After a statement that may have changed its session's `search_path`: connections may mean different tables by one
   *  unqualified name, which then means that table in every schema. */
  private pathsStartDiffering(): void {
    this.pathsDiffer = true
    this.catalog!.searchPath = undefined
    this.plans = new Map()
  }

  private forgetSchema(): void {
    this.catalog = undefined
    this.loggedUpTo = new WeakMap()
    this.plans = new Map()
    this.knownOperands = new Map()
    // The tables may be others: each role's access is learned again.
    this.access = new Map()
  }

  /** Listens to the other servers' changes to the tables `shape` reads (`ChangeFeed.watch()`): a row's change goes to
   *  its table only, a change to any row of any table to `*`. */
  private watchTables(shape: ReadShape): Promise<void> | undefined {
    const { aliases, alsoReads } = shape
    const tables =
      alsoReads === 'all' ? this.catalog!.tables.keys() : [...aliases.map((alias) => alias.table.id), ...alsoReads]
    return this.feed!.watch(['*', ...tables])
  }
}

/** The rows a foreign key's `ON DELETE CASCADE` deletes, found by the key of each deleted parent (what deleting them
 *  changes in turn is in `WritePlan.alsoWrites`). */
function cascadedDeletes(plan: WritePlan, changes: Change[]): Change[] {
  const keys = plan.statement === 'delete' ? plan.table.referencedBy.filter((k) => k.onDelete === 'c' && k.column) : []
  return keys.flatMap((key) =>
    changes.flatMap((change): Change[] => {
      if (change.kind !== 'delete') return []
      const old = { [key.column!]: change.old[key.parentColumn!] ?? null }
      return [{ table: key.table, kind: 'delete', old, several: true }]
    }),
  )
}

/** Queries naming the tables a transaction changed, for `Statement.beforeCommit`: `named` gets them. */
function namingQueries(named: (tables: string[]) => void): FollowUp[] {
  return [
    // Its deferred triggers fire now: the tables they write are locked before the locks are read.
    { text: 'SET CONSTRAINTS ALL IMMEDIATE', values: [], done: () => {} },
    { text: LOCKED_TABLES, values: [], done: (rows) => rows && named(rows.map((row) => row[0]!)) },
  ]
}

function readsOrWritesRows(plan: Plan): boolean {
  return plan.kind === 'read' || plan.kind === 'write' || (plan.kind === 'unknownWrite' && !plan.schemaChange)
}

/** Whether `plan` changes rows of tables the catalog can't name: its transaction then reads its locks before it
 *  commits, and one sent on its own runs in a transaction of ours. */
function isUnnamed(plan: Plan): boolean {
  return plan.kind === 'unknownWrite' && plan.tables === 'unnamed'
}

/** Changes to any row of each of `tables`: of the tables the transaction locked, once known, for `'unnamed'`. */
/** The changes of a write whose rows in the change log weren't read: any row of the tables it may change. */
function unloggedChanges(plan: Plan): Change[] {
  if (plan.kind === 'several') return plan.plans.flatMap(unloggedChanges)
  return plan.kind === 'logged' || plan.kind === 'unknownWrite' ? unknownChanges(plan.tables) : []
}

function unknownChanges(tables: ChangedTables): Change[] {
  if (tables === 'unnamed') return [UNNAMED_TABLES]
  if (tables === 'all') return [{ table: '*', kind: 'unknown' }]
  return [...tables].map((table) => ({ table, kind: 'unknown' }))
}

function imageOf(text: string | null | undefined, table: Table): Row | undefined {
  if (typeof text !== 'string') return undefined
  const values = JSON.parse(text) as unknown[]
  return rowOf(table.columns.map((c, i) => [c.name, values[i] ?? null]))
}

/** The changes of a write whose rows `before` it are known (`WritePlan.readsFirst`): each new row is an update of the
 *  old row with its primary key, or an insert. An `UPDATE` changed every row it read first: an old row no new row has
 *  the key of had its key changed. An upsert's update may have been skipped by its `WHERE`. */
function pairedChanges(plan: WritePlan, rows: Rows, before: Rows): Change[] {
  const { table } = plan
  const keyOf = (row: Row) => (table.primaryKey.length > 0 ? JSON.stringify(table.primaryKey.map((c) => row[c])) : '')
  const olds = new Map<string, Row[]>()
  for (const [image] of before) {
    const old = imageOf(image, table)!
    olds.set(keyOf(old), [...(olds.get(keyOf(old)) ?? []), old])
  }
  const changes: Change[] = rows.map((row): Change => {
    const now = imageOf(row[plan.images.indexOf('new')], table)!
    const old = table.primaryKey.length > 0 ? olds.get(keyOf(now))?.pop() : undefined
    return old ? { table: table.id, kind: 'update', old, new: now } : { table: table.id, kind: 'insert', new: now }
  })
  if (plan.statement === 'update')
    for (const old of [...olds.values()].flat()) changes.push({ table: table.id, kind: 'delete', old })
  return changes
}

/** The row before an `UPDATE`, as far as Postgres < 18 tells: the columns it didn't assign kept their value. */
function assumedOld(now: Row, plan: WritePlan): Row {
  const kept = plan.table.updateChangesOtherColumns ? plan.table.primaryKey : Object.keys(now)
  return Object.fromEntries(kept.filter((c) => !plan.assigned.includes(c)).map((c) => [c, now[c]!]))
}

/** A statement after which the connection it ran on may answer reads differently from the others. */
function changesSession(plan: Plan): boolean {
  if (plan.kind === 'several') return plan.plans.some(changesSession)
  return plan.kind === 'session' || (plan.kind === 'unknownWrite' && plan.changesSession === true)
}

/** A statement that may set its session's `search_path`, if only until its transaction ends. */
function changesPath(plan: Plan): boolean {
  if (plan.kind === 'several') return plan.plans.some(changesPath)
  return plan.kind === 'none' && plan.changesPath === true
}

/** `plan` with how its `set_config()` calls change the role, from the values they're given (`Plan.setConfigs`). */
function withRoleSet(plan: Plan, values: readonly unknown[]): Plan {
  // Several statements in one text take no parameters.
  if (plan.kind === 'several') return { ...plan, plans: plan.plans.map((p) => withRoleSet(p, [])) }
  if (plan.kind !== 'unknownWrite' || !plan.setConfigs) return plan
  const role = roleChangeOf(plan.setConfigs, values)
  return role ? { ...plan, role } : plan
}
