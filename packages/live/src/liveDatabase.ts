export { LiveDatabase, MAX_RESULT_ROWS }
export type { Statement, FollowUp, LiveOptions, Sharing, Reservation }

import type { BroadcastAdapter } from 'telefunc'
import { loadCatalog, tablesChangedWith, union, type Catalog, type Table, type TableSet } from './catalog.js'
import { ChangeFeed } from './changeFeed.js'
import { planStatement, type Plan, type WritePlan } from './capture/statement.js'
import { Transactions } from './capture/transactions.js'
import { currentQuery, type LiveQuery } from './liveQuery.js'
import type { Row } from './match/condition.js'
import type { Change } from './match/liveRead.js'
import { operandValues, type OperandValue } from './match/operands.js'
import type { ReadShape } from './match/readShape.js'
import { Router } from './match/router.js'
import { keyOfValues, SharedReads, WatchedRead, type Joined } from './sharedReads.js'
import { loadParser } from './sql.js'

type LiveOptions = {
  /** Carries changes to the other servers, e.g. `new DefaultBroadcastAdapter(new RedisTransport({ redis }))`.
   *  @default process-local */
  transport?: BroadcastAdapter
}

/** How a driver runs one statement: send `text`, give back each row's last `hiddenColumns` values (as text) to
 *  `succeeded` and the rest to the caller (no rows at all unless `returnsRows`), or call `failed`. Run the follow-ups
 *  `succeeded` returns next, in order, on the same connection, before the caller can send anything else. */
type Statement = {
  text: string
  hiddenColumns: number
  returnsRows: boolean
  succeeded(hiddenRows: (string | null)[][]): FollowUp[]
  failed(): void
  /** Identical reads wait for this one's result: give it to them with `answered()`. */
  leads: boolean
  /** Gives the result to the identical reads that joined this one; `copy` makes each its own. */
  answered<R>(result: R, copy: (result: R) => R): void
  /** An identical read is running: don't send this one, give the caller a copy of that one's result. */
  joined?: Joined
}

/** How a driver that can hand its result to identical reads (`Statement.answered()`) returns rows: reads share only
 *  with reads whose rows come in the same form. */
type Sharing = { rowForm: string; reservation?: Reservation }

/** A read that leads identical ones before its driver got a connection (`reserveRead()`). */
type Reservation = { read: WatchedRead; lead: ReturnType<SharedReads['lead']>; failed(): void }

/** A query whose rows (each value as text, in an array) go to `done`; `undefined` when it failed. */
type FollowUp = { text: string; values: unknown[]; done(rows: (string | null)[][] | undefined): void }

type Rows = (string | null)[][]

const READS_EVERYTHING: ReadShape = { kind: 'other', aliases: [], alsoReads: 'all', hiddenColumns: [], operands: [] }
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
  private catalog: Catalog | undefined
  private loadingCatalog: Promise<Catalog> | undefined
  private parserReady = false
  private plans = new Map<string, Plan>()
  private readonly router = new Router()
  private readonly transactions = new Transactions((changes) => this.announce(changes))
  private readonly transport: BroadcastAdapter | undefined
  private feed: ChangeFeed | undefined
  private readonly shared = new SharedReads()
  /** A statement changed a session: connections may answer the same read differently from now on. */
  private sessionsDiffer = false
  /** The values Postgres gave a read's operands, by its text and parameters: an identical read doesn't ask again. */
  private knownOperands = new Map<string, OperandValue[]>()

  constructor(options: LiveOptions) {
    this.transport = options.transport
  }

  /** Whether `statementFor()` can run: the schema and the SQL parser are loaded. */
  isReady(): boolean {
    return this.catalog !== undefined && this.parserReady
  }

  /** Loads what `statementFor()` needs; `query` runs our catalog query on the connection about to be used. */
  async ready(query: (text: string) => Promise<string>): Promise<void> {
    await Promise.all([this.loadCatalog(query), loadParser()])
    this.parserReady = true
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
    const key = plan.kind === 'read' ? this.shareKey(text, values, undefined, rowForm) : undefined
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

  /** `live`: the `.live()` query the statement runs in. */
  private planned(
    connection: object,
    text: string,
    values: readonly unknown[],
    rewrite: boolean,
    live: LiveQuery | undefined,
    sharing: Sharing | undefined,
  ): Statement {
    const plan = this.planOf(text, this.catalog!)
    if (changesSession(plan)) {
      this.sessionsDiffer = true
      this.knownOperands.clear()
    }
    switch (plan.kind) {
      case 'read':
        return this.read(rewrite ? plan : { ...plan, liveText: undefined }, text, values, connection, live, sharing)
      case 'write':
        if (!rewrite) break
        return {
          ...this.statement(connection, plan.text, plan.images.length, (rows) => this.changesOfWrite(plan, rows)),
          returnsRows: plan.hadReturning,
        }
    }
    return this.statement(connection, text, 0, () => this.changesOf(plan, connection))
  }

  private read(
    plan: Plan & { kind: 'read' },
    text: string,
    values: readonly unknown[],
    connection: object,
    live: LiveQuery | undefined,
    sharing: Sharing | undefined,
  ): Statement {
    // A read outside `.live()` is sent as written.
    if (!live) return this.statement(connection, text, 0, () => [])
    const key = sharing && !sharing.reservation ? this.shareKey(text, values, connection, sharing.rowForm) : undefined
    const joined = key ? this.shared.join(key, live) : undefined
    if (joined) return { ...this.statement(connection, text, 0, () => []), joined }
    const reservation = sharing?.reservation
    const watched = reservation?.read ?? this.watched(plan.shape, text, values, live)
    const lead = reservation?.lead ?? (key ? this.shared.lead(key, watched) : undefined)
    return {
      text: plan.liveText ?? text,
      hiddenColumns: plan.liveText ? plan.shape.hiddenColumns.length : 0,
      returnsRows: true,
      succeeded: (hiddenRows) => {
        const rows = plan.liveText && hiddenRows.length <= MAX_RESULT_ROWS ? hiddenRows : undefined
        return this.resolve(watched, plan.shape, rows, text, values, connection)
      },
      failed: () => {
        if (reservation) reservation.failed()
        else {
          watched.drop()
          lead?.failed()
        }
        this.transactions.failed(connection)
      },
      leads: lead !== undefined,
      answered: (result, copy) => lead?.answered(result, copy),
    }
  }

  /** A live read `live` watches, filed in the router. */
  private watched(shape: ReadShape, text: string, values: readonly unknown[], live: LiveQuery): WatchedRead {
    const operandsKey = this.operandsKey(text, values)
    const operands = !shape.operandQuery ? [] : operandsKey ? this.knownOperands.get(operandsKey) : undefined
    const read = new WatchedRead(shape, values, operands, this.router)
    this.watchTables(shape)
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
    // Not answered: they stay unknown, and any change to the tables counts.
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

  private statement(
    connection: object,
    text: string,
    hiddenColumns: number,
    changesOf: (rows: (string | null)[][]) => Change[],
  ): Statement {
    return {
      text,
      hiddenColumns,
      returnsRows: true,
      succeeded: (rows) => {
        this.transactions.record(connection, changesOf(rows))
        return []
      },
      failed: () => this.transactions.failed(connection),
      leads: false,
      answered: () => {},
    }
  }

  /** Changes of a statement that isn't a single captured write; applies transaction statements. */
  private changesOf(plan: Plan, connection: object): Change[] {
    switch (plan.kind) {
      case 'transaction':
        this.transactions.apply(connection, plan.action, plan.savepoint)
        return []
      case 'unknownWrite':
        if (plan.schemaChange) this.forgetSchema()
        return unknownChanges(plan.tables)
      case 'write':
        return unknownChanges(tablesChangedWith(new Set([plan.table.id]), this.catalog!))
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

  private changesOfWrite(plan: WritePlan, rows: (string | null)[][]): Change[] {
    if (rows.length > MAX_CHANGED_ROWS)
      return unknownChanges(tablesChangedWith(new Set([plan.table.id]), this.catalog!))
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
    return [...changes, ...cascades(plan, changes, this.catalog!), ...unknownChanges(plan.alsoWrites)]
  }

  private announce(changes: Change[]): void {
    this.receive(changes)
    this.feed?.publish(changes)
  }

  private planOf(text: string, catalog: Catalog): Plan {
    let plan = this.plans.get(text)
    if (!plan) {
      if (this.plans.size >= MAX_PLANS) this.plans.clear()
      plan = planStatement(text, catalog)
      this.plans.set(text, plan)
    }
    return plan
  }

  private async loadCatalog(query: (text: string) => Promise<string>): Promise<void> {
    if (this.catalog) return
    this.loadingCatalog ??= loadCatalog(query)
    try {
      this.catalog = await this.loadingCatalog
    } finally {
      // A failure fails the statement waiting for it; the next one tries again.
      this.loadingCatalog = undefined
    }
    if (this.transport && !this.feed) {
      this.feed = new ChangeFeed(this.transport, this.catalog.database, (changes) => this.receive(changes))
    }
  }

  private receive(changes: Change[]): void {
    for (const change of changes) this.router.route(change)
  }

  private forgetSchema(): void {
    this.catalog = undefined
    this.plans = new Map()
    this.knownOperands = new Map()
  }

  private watchTables(shape: ReadShape): void {
    if (!this.feed) return
    this.feed.watch('*')
    for (const alias of shape.aliases) this.feed.watch(alias.table.id)
    if (shape.alsoReads !== 'all') for (const table of shape.alsoReads) this.feed.watch(table)
  }
}

/** What foreign key actions change in other tables. A deleted parent's children are found by its key; anything
 *  further (what deleting them changes, `SET NULL`, a key update) counts as a change to any row of the table. */
function cascades(plan: WritePlan, changes: Change[], catalog: Catalog): Change[] {
  const result: Change[] = []
  const wholeTables: TableSet[] = []
  for (const key of plan.table.referencedBy) {
    const child = catalog.tables.get(key.table)!
    if (plan.statement === 'delete' && key.onDelete === 'c' && key.column) {
      for (const change of changes) {
        if (change.kind !== 'delete') continue
        const old = { [key.column]: change.old[key.parentColumn!] ?? null }
        result.push({ table: key.table, kind: 'delete', old, several: true })
      }
      // The deleted children fire their own triggers and foreign key actions.
      wholeTables.push(child.alsoWrites, new Set(child.referencedBy.map((k) => k.table)))
    } else if (plan.statement === 'delete' && ['c', 'n', 'd'].includes(key.onDelete))
      wholeTables.push(new Set([child.id]))
    else if (plan.statement !== 'insert' || plan.assigned.length > 0) {
      const keyAssigned = !key.parentColumn || plan.assigned.includes(key.parentColumn)
      if (['c', 'n', 'd'].includes(key.onUpdate) && keyAssigned) wholeTables.push(new Set([child.id]))
    }
  }
  return [...result, ...unknownChanges(tablesChangedWith(union(wholeTables), catalog))]
}

function unknownChanges(tables: TableSet): Change[] {
  if (tables === 'all') return [{ table: '*', kind: 'unknown' }]
  return [...tables].map((table) => ({ table, kind: 'unknown' }))
}

function imageOf(text: string | null | undefined, table: Table): Row | undefined {
  if (typeof text !== 'string') return undefined
  const values = JSON.parse(text) as (string | null)[]
  return Object.fromEntries(table.columns.map((c, i) => [c.name, values[i] ?? null]))
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
