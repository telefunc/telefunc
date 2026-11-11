export { planStatement, planRead, alsoWrites, ofRows, HIDDEN_PREFIX, READS_EVERYTHING }
export type { Plan, ReadPlan, WritePlan, TransactionAction, ChangedTables }

import {
  actionOf,
  sideEffects,
  tablesChangedBy,
  type Catalog,
  type Table,
  type TableSet,
  type Written,
} from '../catalog.js'
import { analyzeRead, type ReadShape } from '../match/readShape.js'
import { names, parseStatements, quote, spliceBytes, statementEnd, walk, type Node } from '../sql.js'
import { ROLE_SETTINGS, type RoleChange, type SettingName } from './roles.js'

type TransactionAction = 'begin' | 'commit' | 'rollback' | 'savepoint' | 'release' | 'rollbackTo'

type ReadPlan = {
  kind: 'read'
  shape: ReadShape
  /** The statement with the hidden columns appended to its select list; `undefined` when it has none. */
  liveText: string | undefined
  /** The index of the caller's value each parameter of `liveText` takes, when they aren't the caller's values. */
  liveValues?: number[]
  /** Identical reads may share one run: it calls no volatile function (`Catalog.volatileFunctions`). */
  shareable: boolean
  /** How its role changes: a function of the app's it calls may set it. */
  role?: RoleChange
}

type WritePlan = {
  kind: 'write'
  table: Table
  statement: 'insert' | 'update' | 'delete'
  /** The statement with images of the written rows appended to its `RETURNING` list. */
  text: string
  hadReturning: boolean
  /** Which hidden columns the rows carry, in order: row images, and whether an upsert inserted (Postgres < 18, which
   *  has no `old` image). */
  images: ('new' | 'old' | 'inserted')[]
  /** The columns an `UPDATE` (or an upsert's update) assigns. */
  assigned: string[]
  /** What the write changes besides the rows it returns and those a cascade deletes by key (`cascadedDeletes()`): what
   *  the table's triggers write and the rows its foreign key actions change, then what those changes change in turn. */
  alsoWrites: ReadonlySet<string>
}

/** What a statement may have changed: rows of these tables, or anything (`'all'`, schema changes included).
 *  `'unnamed'`: rows only, of tables the catalog can't name (what a rule or a function of the app's writes); the locks
 *  its transaction holds name them (`isUnnamed()`). */
type ChangedTables = TableSet | 'unnamed'

type Plan =
  | ReadPlan
  | WritePlan
  /** `chain`: a `COMMIT` or `ROLLBACK` `AND CHAIN`, which starts the next transaction at once. */
  | { kind: 'transaction'; action: TransactionAction; savepoint?: string; chain?: boolean }
  /** A statement whose changes aren't captured row by row: every listed table may have changed. `changesSession`: it
   *  may also change settings of its session (`set_config()`, `DISCARD`), or those new sessions start with; `role`:
   *  its role among them, or `setConfigs`: the settings its `set_config()` calls set, which may name it. `unread`: the
   *  parser couldn't read it, so it may be a read too. */
  | {
      kind: 'unknownWrite'
      tables: ChangedTables
      schemaChange: boolean
      changesSession?: boolean
      role?: RoleChange
      setConfigs?: SettingName[]
      unread?: true
    }
  /** `SET`, `RESET`: the settings of its session change (a role, a `search_path`, what a row level security policy
   *  reads). `role`: how its role changes. */
  | { kind: 'session'; role?: RoleChange }
  /** `SET LOCAL` is one too, until its transaction ends. `changesPath`: it sets the `search_path`. */
  | { kind: 'none'; role?: RoleChange; changesPath?: boolean }
  | { kind: 'several'; plans: Plan[] }
  /** A write whose rows its connection's change log holds (SQLite); `also`: the tables it may change besides. */
  | { kind: 'logged'; also: ChangedTables }

const HIDDEN_PREFIX = '__live_'

/** A read of every table, whose result can't be bounded. */
const READS_EVERYTHING: ReadShape = { kind: 'other', aliases: [], alsoReads: 'all', hiddenColumns: [], operands: [] }

/** Statements that change rows only, and can run in a transaction of ours. */
const DATA_STATEMENTS = new Set(['SelectStmt', 'InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt'])

const UNKNOWN_ROLE: RoleChange = { kind: 'unknown' }

/** What a statement whose changes can't be told may have changed. */
const ANYTHING: Plan = {
  kind: 'unknownWrite',
  tables: 'all',
  schemaChange: true,
  changesSession: true,
  role: UNKNOWN_ROLE,
  unread: true,
}

/** Statements that may set a session's settings (they run any code), or those new sessions start with. */
const SETS_SESSIONS = new Set(['DoStmt', 'CallStmt', 'ExecuteStmt', 'AlterRoleSetStmt', 'AlterDatabaseSetStmt'])

function planStatement(text: string, catalog: Catalog): Plan {
  const statements = parseStatements(text)
  // A statement the parser can't read may still run: the parser and the database can be of different versions. It may
  // have changed anything, a table, the schema or its session.
  if (!statements) return ANYTHING
  if (statements.length !== 1) return { kind: 'several', plans: statements.map((s) => planOne(s, undefined, catalog)) }
  return planOne(statements[0]!, text, catalog)
}

/** `text` is set when the statement is the whole text, so that it can be rewritten. */
function planOne(statement: Node, text: string | undefined, catalog: Catalog): Plan {
  const [kind, node] = Object.entries(statement)[0] as [string, Node]
  // Without the catalog, which a schema change just before may have dropped: a driver's own `COMMIT` doesn't load it.
  if (kind === 'TransactionStmt') return planTransaction(node)
  const functions = catalog.calls(node)
  const writingCte = ((node.withClause?.ctes ?? []) as Node[]).some((c) => !c.CommonTableExpr.ctequery.SelectStmt)
  // A read is judged by the volatility of what it calls as well (`Catalog.volatileWritingFunctions`).
  const read = kind === 'SelectStmt' && !node.intoClause && !writingCte
  const writers = read ? catalog.volatileWritingFunctions : catalog.writingFunctions
  const writing = functions.some((name) => writers.has(name))
  // A function of the app's may set its session's role (`set_config()`), whatever it is declared to do. Only those the
  // statement calls count: a trigger, a column default or a row level security policy that switched the session's
  // role would be a bug of the app, and counting them would cost a round trip before every write after a write to a
  // table with an `updated_at` trigger.
  const roleOfCalls = functions.some((name) => catalog.writingFunctions.has(name)) ? { role: UNKNOWN_ROLE } : {}
  // `set_config()` sets a setting of the session; so may a function of the app's that may write.
  const changesSession = writing || functions.includes('set_config')
  if (changesSession || writingCte) {
    const tables = writing ? 'all' : writtenTables(node, catalog)
    const setConfigs = setConfigsOf(node)
    return {
      kind: 'unknownWrite',
      tables: DATA_STATEMENTS.has(kind) ? ofRows(tables) : 'all',
      schemaChange: false,
      changesSession,
      ...(setConfigs.length > 0 ? { setConfigs } : {}),
      ...roleOfCalls,
    }
  }
  switch (kind) {
    case 'SelectStmt':
      if (node.intoClause) return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
      return { ...planRead(node, text, catalog), ...roleOfCalls }
    case 'InsertStmt':
    case 'UpdateStmt':
    case 'DeleteStmt':
      return planWrite(kind, node, text, catalog)
    case 'MergeStmt':
    case 'CopyStmt':
    case 'TruncateStmt':
      if (kind === 'CopyStmt' && !node.is_from) return { kind: 'none' }
      // `TRUNCATE … CASCADE` empties the tables referencing them, whatever their foreign keys' actions.
      if (node.behavior === 'DROP_CASCADE') return { kind: 'unknownWrite', tables: 'unnamed', schemaChange: false }
      return { kind: 'unknownWrite', tables: ofRows(writtenTables(node, catalog)), schemaChange: false }
    case 'VariableSetStmt': {
      const role = roleChangeOf(node)
      const sets = role ? { role } : {}
      // `SET LOCAL` ends with its transaction, whose reads never share; its `search_path` names tables meanwhile.
      if (node.is_local) return { kind: 'none', ...sets, changesPath: node.name === 'search_path' }
      return { kind: 'session', ...sets }
    }
    case 'DiscardStmt':
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true, changesSession: true, role: UNKNOWN_ROLE }
    case 'VariableShowStmt':
    case 'ListenStmt':
    case 'UnlistenStmt':
    case 'NotifyStmt':
    case 'DeallocateStmt':
    case 'DeclareCursorStmt':
    case 'FetchStmt':
    case 'ClosePortalStmt':
    case 'LockStmt':
      return { kind: 'none' }
    case 'ExplainStmt': {
      const analyze = ((node.options ?? []) as Node[]).some((o) => o.DefElem?.defname === 'analyze')
      return analyze ? planOne(node.query, undefined, catalog) : { kind: 'none' }
    }
    default: {
      // Schema changes, `CALL`, `DO`, `EXECUTE`, `REFRESH MATERIALIZED VIEW`, ...: anything may have changed.
      const changesSession = SETS_SESSIONS.has(kind)
      const sets = changesSession ? { role: UNKNOWN_ROLE } : {}
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true, changesSession, ...sets }
    }
  }
}

function planRead(select: Node, text: string | undefined, catalog: Catalog): ReadPlan {
  const shape = analyzeRead(select, catalog, text)
  const shareable = !catalog.calls(select).some((name) => catalog.volatileFunctions.has(name))
  if (shape.hiddenColumns.length === 0 || !text) return { kind: 'read', shape, liveText: undefined, shareable }
  const { dialect } = catalog
  const splices = shape.hiddenAt ?? [{ from: dialect.topLevelFrom(text), columns: shape.hiddenColumns }]
  if (splices.some((s) => s.from === undefined)) return { kind: 'read', shape, liveText: undefined, shareable }
  let liveText = text
  // From the last: a splice leaves the byte offsets before it as they were.
  for (const { from, columns } of splices.toSorted((a, b) => b.from! - a.from!)) {
    const hidden = columns.map((expression, i) => `${expression} AS "${HIDDEN_PREFIX}${i}"`)
    liveText = dialect.splice(liveText, from!, `, ${hidden.join(', ')} `)
  }
  return { kind: 'read', shape, liveText, shareable }
}

function planWrite(kind: string, node: Node, text: string | undefined, catalog: Catalog): Plan {
  const table = catalog.find(node.relation)
  const statement = kind === 'InsertStmt' ? 'insert' : kind === 'UpdateStmt' ? 'update' : 'delete'
  const upsert = node.onConflictClause?.action === 'ONCONFLICT_UPDATE'
  const assignments = (
    statement === 'update' ? node.targetList : upsert ? node.onConflictClause.targetList : []
  ) as Node[]
  const assigned = (assignments ?? []).map((t) => t.ResTarget.name as string)
  const also = table ? alsoWrites(table, upsert ? ['insert', 'update'] : [statement], assigned, catalog) : 'all'
  // Sent as written when the catalog can't name what else it changes: the locks naming that name its table too.
  if (!table || !text || also === 'all')
    return { kind: 'unknownWrite', tables: ofRows(writtenTables(node, catalog)), schemaChange: false }
  const hasOld = catalog.serverVersion >= 180000
  const target = quote(node.relation.alias?.aliasname ?? node.relation.relname)
  // `RETURNING WITH (OLD AS o)` renames the old row: `old` no longer names it.
  const options = (node.returningClause?.options ?? []) as Node[]
  const oldName = options.find((o) => o.ReturningOption.option === 'RETURNING_OPTION_OLD')?.ReturningOption.value
  const old = quote(oldName ?? 'old')
  const images: WritePlan['images'] = statement === 'delete' ? ['old'] : ['new']
  if (hasOld && (statement === 'update' || upsert)) images.push('old')
  if (!hasOld && upsert) images.push('inserted')
  const image = (source: string) =>
    `to_json(ARRAY[${table.columns.map((c) => `${source}.${quote(c.name)}::text`).join(', ')}])::text`
  const hidden = images.map((which, i) => {
    // A row an upsert inserted has no deleting transaction yet.
    if (which === 'inserted') return `(${target}.xmax::text = '0')::text AS "${HIDDEN_PREFIX}${i}"`
    const source = which === 'old' && statement !== 'delete' ? old : target
    return `${image(source)} AS "${HIDDEN_PREFIX}${i}"`
  })
  const returning = node.returningClause?.exprs ?? node.returningList
  const hadReturning = Array.isArray(returning) && returning.length > 0
  const end = statementEnd(text)
  const widened = spliceBytes(text, end, `${hadReturning ? ',' : ' RETURNING'} ${hidden.join(', ')}`)
  return {
    kind: 'write',
    table,
    statement,
    text: widened,
    hadReturning,
    images,
    assigned,
    alsoWrites: also,
  }
}

/** What `writes` to `table` change besides their rows: `WritePlan.alsoWrites`. */
function alsoWrites(
  table: Table,
  writes: ('insert' | 'update' | 'delete')[],
  assigned: string[],
  catalog: Catalog,
): TableSet {
  const written: Written[] = []
  for (const write of writes) {
    const also = table.alsoWrites[write]
    if (also === 'all') return 'all'
    for (const id of also) written.push([id, undefined])
    for (const key of table.referencedBy) {
      const child = catalog.tables.get(key.table)!
      if (write === 'delete' && key.onDelete === 'c' && key.column) {
        // Its deleted rows are found by key; what deleting them changes counts as a whole.
        const effects = sideEffects(child, 'delete')
        if (effects === 'all') return 'all'
        written.push(...effects)
        continue
      }
      // A trigger or a generated column may change a key the `SET` list doesn't name.
      const action = actionOf(key, write, table.updateChangesOtherColumns ? undefined : assigned)
      if (action) written.push([child.id, action])
    }
  }
  return tablesChangedBy(written, catalog)
}

function planTransaction(node: Node): Plan {
  const savepoint = node.savepoint_name as string | undefined
  switch (node.kind) {
    case 'TRANS_STMT_BEGIN':
    case 'TRANS_STMT_START':
      return { kind: 'transaction', action: 'begin' }
    case 'TRANS_STMT_COMMIT':
      return { kind: 'transaction', action: 'commit', chain: node.chain === true }
    case 'TRANS_STMT_ROLLBACK':
      return { kind: 'transaction', action: 'rollback', chain: node.chain === true }
    case 'TRANS_STMT_SAVEPOINT':
      return { kind: 'transaction', action: 'savepoint', savepoint }
    case 'TRANS_STMT_RELEASE':
      return { kind: 'transaction', action: 'release', savepoint }
    case 'TRANS_STMT_ROLLBACK_TO':
      return { kind: 'transaction', action: 'rollbackTo', savepoint }
    default:
      // Two-phase commit: the transaction may commit from another session, later.
      return { kind: 'unknownWrite', tables: 'all', schemaChange: false }
  }
}

/** How `SET` or `RESET` changes its session's role: `SET [LOCAL] ROLE` names it. What `RESET ROLE`, `SET ROLE NONE`,
 *  `SESSION AUTHORIZATION` and `RESET ALL` go back to depends on how the session started and what it ran since. */
function roleChangeOf(node: Node): RoleChange | undefined {
  if (node.kind === 'VAR_RESET_ALL') return UNKNOWN_ROLE
  if (!ROLE_SETTINGS.has(node.name)) return undefined
  const named = node.name === 'role' && node.kind === 'VAR_SET_VALUE' ? node.args?.[0]?.A_Const?.sval?.sval : undefined
  if (typeof named !== 'string' || named === 'none') return UNKNOWN_ROLE
  return { kind: 'set', role: named, local: node.is_local === true }
}

/** The settings `node`'s `set_config()` calls set, as far as the text tells. */
function setConfigsOf(node: Node): SettingName[] {
  const settings: SettingName[] = []
  walk(node, (key, value) => {
    if (key === 'FuncCall' && names(value.funcname).at(-1) === 'set_config') settings.push(settingOf(value.args?.[0]))
  })
  return settings
}

function settingOf(node: Node | undefined): SettingName {
  const argument = node?.TypeCast?.arg ?? node
  if (argument?.ParamRef) return { param: argument.ParamRef.number - 1 }
  const constant = argument?.A_Const
  if (!constant) return undefined
  return { constant: constant.isnull ? null : (constant.sval?.sval ?? null) }
}

/** What a statement that changes rows only may change: `'unnamed'` for every table. */
function ofRows(tables: TableSet): ChangedTables {
  return tables === 'all' ? 'unnamed' : tables
}

/** The tables a statement may write, with what their writes reach (tables it only reads are included too). */
function writtenTables(node: Node, catalog: Catalog): TableSet {
  const ids = new Set<string>()
  const targets: Node[] = []
  walk({ node }, (key, value) => {
    if (key === 'relation' || (key === 'RangeVar' && value.relname)) targets.push(value)
  })
  for (const target of targets) {
    const named = catalog.named(target as never)
    if (named.length === 0) return 'all'
    for (const table of named) ids.add(table.id)
  }
  return tablesChangedBy(
    [...ids].map((id) => [id, undefined]),
    catalog,
  )
}
