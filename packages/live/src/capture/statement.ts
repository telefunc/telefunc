export { planStatement, ofRows, HIDDEN_PREFIX }
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
import { parseStatements, quote, spliceBytes, statementEnd, topLevelFrom, walk, type Node } from '../sql.js'

type TransactionAction = 'begin' | 'commit' | 'rollback' | 'savepoint' | 'release' | 'rollbackTo'

type ReadPlan = {
  kind: 'read'
  shape: ReadShape
  /** The statement with the hidden columns appended to its select list; `undefined` when it has none. */
  liveText: string | undefined
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
  | { kind: 'transaction'; action: TransactionAction; savepoint?: string }
  /** A statement whose changes aren't captured row by row: every listed table may have changed. `changesSession`: it
   *  may also change settings of its session (`set_config()`, `DISCARD`), or those new sessions start with. */
  | { kind: 'unknownWrite'; tables: ChangedTables; schemaChange: boolean; changesSession?: boolean }
  /** `SET`, `RESET`: the settings of its session change (a role, a `search_path`, what a row level security policy
   *  reads). */
  | { kind: 'session' }
  | { kind: 'none' }
  | { kind: 'several'; plans: Plan[] }

const HIDDEN_PREFIX = '__live_'

/** Statements that change rows only, and can run in a transaction of ours. */
const DATA_STATEMENTS = new Set(['SelectStmt', 'InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt'])

/** Statements that may set a session's settings (they run any code), or those new sessions start with. */
const SETS_SESSIONS = new Set(['DoStmt', 'CallStmt', 'ExecuteStmt', 'AlterRoleSetStmt', 'AlterDatabaseSetStmt'])

function planStatement(text: string, catalog: Catalog): Plan {
  const statements = parseStatements(text)
  // The database rejects it too, and runs nothing.
  if (!statements) return { kind: 'none' }
  if (statements.length !== 1) return { kind: 'several', plans: statements.map((s) => planOne(s, undefined, catalog)) }
  return planOne(statements[0]!, text, catalog)
}

/** `text` is set when the statement is the whole text, so that it can be rewritten. */
function planOne(statement: Node, text: string | undefined, catalog: Catalog): Plan {
  const [kind, node] = Object.entries(statement)[0] as [string, Node]
  const functions = catalog.calls(node)
  const writingCte = ((node.withClause?.ctes ?? []) as Node[]).some((c) => !c.CommonTableExpr.ctequery.SelectStmt)
  // A read is judged by the volatility of what it calls as well (`Catalog.volatileWritingFunctions`).
  const read = kind === 'SelectStmt' && !node.intoClause && !writingCte
  const writers = read ? catalog.volatileWritingFunctions : catalog.writingFunctions
  const writing = functions.some((name) => writers.has(name))
  // `set_config()` sets a setting of the session; so may a function of the app's that may write.
  const changesSession = writing || functions.includes('set_config')
  if (changesSession || writingCte) {
    const tables = writing ? 'all' : writtenTables(node, catalog)
    return {
      kind: 'unknownWrite',
      tables: DATA_STATEMENTS.has(kind) ? ofRows(tables) : 'all',
      schemaChange: false,
      changesSession,
    }
  }
  switch (kind) {
    case 'SelectStmt':
      if (node.intoClause) return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
      return planRead(node, text, catalog)
    case 'InsertStmt':
    case 'UpdateStmt':
    case 'DeleteStmt':
      return planWrite(kind, node, text, catalog)
    case 'TransactionStmt':
      return planTransaction(node)
    case 'MergeStmt':
    case 'CopyStmt':
    case 'TruncateStmt':
      if (kind === 'CopyStmt' && !node.is_from) return { kind: 'none' }
      // `TRUNCATE … CASCADE` empties the tables referencing them, whatever their foreign keys' actions.
      if (node.behavior === 'DROP_CASCADE') return { kind: 'unknownWrite', tables: 'unnamed', schemaChange: false }
      return { kind: 'unknownWrite', tables: ofRows(writtenTables(node, catalog)), schemaChange: false }
    case 'VariableSetStmt':
      // `SET LOCAL` ends with its transaction, whose reads never share.
      return node.is_local ? { kind: 'none' } : { kind: 'session' }
    case 'DiscardStmt':
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true, changesSession: true }
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
    default:
      // Schema changes, `CALL`, `DO`, `EXECUTE`, `REFRESH MATERIALIZED VIEW`, ...: anything may have changed.
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true, changesSession: SETS_SESSIONS.has(kind) }
  }
}

function planRead(select: Node, text: string | undefined, catalog: Catalog): ReadPlan {
  const shape = analyzeRead(select, catalog, text)
  if (shape.hiddenColumns.length === 0 || !text) return { kind: 'read', shape, liveText: undefined }
  const splices = shape.hiddenAt ?? [{ from: topLevelFrom(text), columns: shape.hiddenColumns }]
  if (splices.some((s) => s.from === undefined)) return { kind: 'read', shape, liveText: undefined }
  let liveText = text
  // From the last: a splice leaves the byte offsets before it as they were.
  for (const { from, columns } of splices.toSorted((a, b) => b.from! - a.from!)) {
    const hidden = columns.map((expression, i) => `${expression} AS "${HIDDEN_PREFIX}${i}"`)
    liveText = spliceBytes(liveText, from!, `, ${hidden.join(', ')} `)
  }
  return { kind: 'read', shape, liveText }
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
  const images: WritePlan['images'] = statement === 'delete' ? ['old'] : ['new']
  if (hasOld && (statement === 'update' || upsert)) images.push('old')
  if (!hasOld && upsert) images.push('inserted')
  const image = (source: string) =>
    `to_json(ARRAY[${table.columns.map((c) => `${source}.${quote(c.name)}::text`).join(', ')}])::text`
  const hidden = images.map((which, i) => {
    // A row an upsert inserted has no deleting transaction yet.
    if (which === 'inserted') return `(${target}.xmax::text = '0')::text AS "${HIDDEN_PREFIX}${i}"`
    const source = which === 'old' && statement !== 'delete' ? 'old' : target
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
      const action = actionOf(key, write, assigned)
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
      return { kind: 'transaction', action: 'commit' }
    case 'TRANS_STMT_ROLLBACK':
      return { kind: 'transaction', action: 'rollback' }
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
