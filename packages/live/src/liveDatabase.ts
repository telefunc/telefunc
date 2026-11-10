export { LiveDatabase, MAX_RESULT_ROWS }
export type { Statement, FollowUp, LiveQueriesOptions }

import type { BroadcastAdapter } from 'telefunc'
import { loadCatalog, type Catalog, type Table, type TableSet } from './catalog.js'
import { ChangeFeed } from './changeFeed.js'
import { planStatement, type Plan, type WritePlan } from './capture/statement.js'
import { Transactions } from './capture/transactions.js'
import { getCallReads } from './callReads.js'
import type { Row } from './match/condition.js'
import { LiveRead, type Change } from './match/liveRead.js'
import { Router } from './match/router.js'
import { loadParser } from './sql.js'

type LiveQueriesOptions = {
  /** Carries changes to the other servers, e.g. `new DefaultBroadcastAdapter(new RedisTransport({ redis }))`.
   *  @default process-local */
  transport?: BroadcastAdapter
}

/** How a driver runs one statement: send `text`, give back each row's last `hiddenColumns` values (as text) to
 *  `succeeded` and the rest to the caller (no rows at all unless `returnsRows`), or call `failed`. When `succeeded`
 *  returns a follow-up, run it next on the same connection, before the caller can send anything else. */
type Statement = {
  text: string
  hiddenColumns: number
  returnsRows: boolean
  succeeded(hiddenRows: (string | null)[][]): FollowUp | void
  failed(): void
}

/** A query whose rows (each value as text, in an array) go to `done`; `undefined` when it failed. */
type FollowUp = { text: string; values: unknown[]; done(rows: (string | null)[][] | undefined): void }

const MAX_PLANS = 10_000
/** A read returning more rows keeps no result keys: any change to a matching row counts. */
const MAX_RESULT_ROWS = 10_000

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

  constructor(options: LiveQueriesOptions) {
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
   *  sent as is: its writes count as changes to their whole tables, its reads can't be bounded by their result. */
  statementFor(connection: object, text: string, values: readonly unknown[], rewrite = true): Statement {
    const plan = this.planOf(text, this.catalog!)
    switch (plan.kind) {
      case 'read':
        return this.read(rewrite ? plan : { ...plan, liveText: undefined }, text, values, connection)
      case 'write':
        if (!rewrite) break
        return {
          ...this.statement(connection, plan.text, plan.images.length, (rows) => this.changesOfWrite(plan, rows)),
          returnsRows: plan.hadReturning,
        }
    }
    return this.statement(connection, text, 0, () => this.changesOf(plan, connection))
  }

  private read(plan: Plan & { kind: 'read' }, text: string, values: readonly unknown[], connection: object): Statement {
    const call = getCallReads()
    // Outside a telefunction call nobody can watch the result.
    if (!call) return this.statement(connection, text, 0, () => [])
    const read = new LiveRead(plan.shape, values, () => call.changed())
    this.router.add(read)
    this.watchTables(plan.shape)
    call.add(read, this.router)
    return {
      text: plan.liveText ?? text,
      hiddenColumns: plan.liveText ? plan.shape.hiddenColumns.length : 0,
      returnsRows: true,
      succeeded: (hiddenRows) => {
        const rows = plan.liveText && hiddenRows.length <= MAX_RESULT_ROWS ? hiddenRows : undefined
        const { state } = plan.shape
        // Inside a transaction, a query of ours failing would fail the caller's transaction.
        if (!rows || !state || (state.onlyWhenEmpty && rows.length > 0) || this.transactions.isOpen(connection))
          return read.resolve(rows)
        const params = state.params.map((i) => values[i])
        return { text: state.text, values: params, done: (stateRows) => read.resolve(rows, stateRows) }
      },
      failed: () => {
        call.remove(read)
        this.transactions.failed(connection)
      },
    }
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
      succeeded: (rows) => this.transactions.record(connection, changesOf(rows)),
      failed: () => this.transactions.failed(connection),
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
        return unknownChanges(new Set([plan.table.id, ...(plan.alsoWrites === 'all' ? ['*'] : plan.alsoWrites)]))
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
  }

  private watchTables(shape: LiveRead['shape']): void {
    if (!this.feed) return
    this.feed.watch('*')
    for (const alias of shape.aliases) this.feed.watch(alias.table.id)
    if (shape.alsoReads !== 'all') for (const table of shape.alsoReads) this.feed.watch(table)
  }
}

/** What foreign key actions change in other tables. A deleted parent's children are found by its key; anything
 *  further (their own children, `SET NULL`, a key update) counts as a change to any row of the table. */
function cascades(plan: WritePlan, changes: Change[], catalog: Catalog): Change[] {
  const result: Change[] = []
  const wholeTables = new Set<string>()
  const addWholeTable = (id: string) => {
    if (wholeTables.has(id)) return
    wholeTables.add(id)
    for (const key of catalog.tables.get(id)?.referencedBy ?? []) addWholeTable(key.table)
  }
  for (const key of plan.table.referencedBy) {
    if (plan.statement === 'delete' && key.onDelete === 'c' && key.column) {
      for (const change of changes) {
        if (change.kind !== 'delete') continue
        const old = { [key.column]: change.old[key.parentColumn!] ?? null }
        result.push({ table: key.table, kind: 'delete', old, several: true })
      }
      for (const grandchild of catalog.tables.get(key.table)?.referencedBy ?? []) addWholeTable(grandchild.table)
    } else if (plan.statement === 'delete' && ['c', 'n', 'd'].includes(key.onDelete)) addWholeTable(key.table)
    else if (plan.statement !== 'insert' || plan.assigned.length > 0) {
      const keyAssigned = !key.parentColumn || plan.assigned.includes(key.parentColumn)
      if (['c', 'n', 'd'].includes(key.onUpdate) && keyAssigned) addWholeTable(key.table)
    }
  }
  return [...result, ...unknownChanges(wholeTables)]
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
