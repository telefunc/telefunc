export { planStatement, HIDDEN_PREFIX }
export type { Plan, ReadPlan, WritePlan, TransactionAction }

import { scanSync } from 'libpg-query'
import type { Catalog, Table, TableSet } from '../catalog.js'
import { analyzeRead, type ReadShape } from '../match/readShape.js'
import { functionNamesIn, parseStatements, walk, type Node } from '../sql.js'

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
  /** Which hidden columns the rows carry, in order. */
  images: ('new' | 'old')[]
  /** The columns an `UPDATE` (or an upsert's update) assigns. */
  assigned: string[]
  /** What the table's triggers write. */
  alsoWrites: TableSet
}

type Plan =
  | ReadPlan
  | WritePlan
  | { kind: 'transaction'; action: TransactionAction; savepoint?: string }
  /** A statement whose changes aren't captured row by row: every listed table may have changed. */
  | { kind: 'unknownWrite'; tables: TableSet; schemaChange: boolean }
  | { kind: 'none' }
  | { kind: 'several'; plans: Plan[] }

const HIDDEN_PREFIX = '__live_'

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
  const volatile = functionNamesIn(node).some((name) => catalog.volatileFunctions.has(name))
  const writingCte = ((node.withClause?.ctes ?? []) as Node[]).some((c) => !c.CommonTableExpr.ctequery.SelectStmt)
  if (volatile || writingCte)
    return { kind: 'unknownWrite', tables: volatile ? 'all' : writtenTables(node, catalog), schemaChange: false }
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
      return { kind: 'unknownWrite', tables: writtenTables(node, catalog), schemaChange: false }
    case 'VariableSetStmt':
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
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
  }
}

function planRead(select: Node, text: string | undefined, catalog: Catalog): ReadPlan {
  const shape = analyzeRead(select, catalog)
  if (shape.hiddenColumns.length === 0 || !text) return { kind: 'read', shape, liveText: undefined }
  const from = topLevelFrom(text)
  if (from === undefined) return { kind: 'read', shape, liveText: undefined }
  const hidden = shape.hiddenColumns.map((expression, i) => `${expression} AS "${HIDDEN_PREFIX}${i}"`)
  return { kind: 'read', shape, liveText: spliceBytes(text, from, `, ${hidden.join(', ')} `) }
}

function planWrite(kind: string, node: Node, text: string | undefined, catalog: Catalog): Plan {
  const table = catalog.find(node.relation)
  const tables = writtenTables(node, catalog)
  if (!table || table.isView || !text) return { kind: 'unknownWrite', tables, schemaChange: false }
  const statement = kind === 'InsertStmt' ? 'insert' : kind === 'UpdateStmt' ? 'update' : 'delete'
  const upsert = node.onConflictClause?.action === 'ONCONFLICT_UPDATE'
  const assignments = (
    statement === 'update' ? node.targetList : upsert ? node.onConflictClause.targetList : []
  ) as Node[]
  const assigned = (assignments ?? []).map((t) => t.ResTarget.name as string)
  const hasOld = catalog.serverVersion >= 180000
  const target = `"${(node.relation.alias?.aliasname ?? node.relation.relname).replace(/"/g, '""')}"`
  const images: ('new' | 'old')[] = statement === 'delete' ? ['old'] : ['new']
  if (hasOld && (statement === 'update' || upsert)) images.push('old')
  const image = (source: string) =>
    `to_json(ARRAY[${table.columns.map((c) => `${source}."${c.name.replace(/"/g, '""')}"::text`).join(', ')}])::text`
  const hidden = images.map((which, i) => {
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
    alsoWrites: table.alsoWrites,
  }
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

/** The tables a statement may write, with what their writes reach (tables it only reads are included too). */
function writtenTables(node: Node, catalog: Catalog): TableSet {
  const ids = new Set<string>()
  const targets: Node[] = []
  walk({ node }, (key, value) => {
    if (key === 'relation' || (key === 'RangeVar' && value.relname)) targets.push(value)
  })
  for (const target of targets) {
    const table = catalog.find(target as never)
    if (!table) return 'all'
    if (table.alsoWrites === 'all') return 'all'
    ids.add(table.id)
    for (const id of table.alsoWrites) ids.add(id)
    for (const key of table.referencedBy) ids.add(key.table)
  }
  return ids
}

// ── Text splicing (scanner offsets are UTF-8 byte offsets) ──

/** Byte offset of the `FROM` keyword of the outermost `SELECT`. */
function topLevelFrom(text: string): number | undefined {
  let depth = 0
  let seenSelect = false
  for (const token of scanSync(text).tokens) {
    const keyword = token.keywordKind !== 0 ? token.text.toLowerCase() : undefined
    if (token.text === '(') depth++
    else if (token.text === ')') depth--
    else if (depth === 0 && keyword === 'select') seenSelect = true
    else if (depth === 0 && seenSelect && keyword === 'from') return token.start
  }
  return undefined
}

/** Byte offset just after the statement's last token (before a trailing `;` or comment). */
function statementEnd(text: string): number {
  const tokens = scanSync(text).tokens.filter((t) => t.text !== ';' && !/COMMENT/.test(t.tokenName))
  return tokens.at(-1)!.end
}

function spliceBytes(text: string, at: number, insert: string): string {
  const bytes = new TextEncoder().encode(text)
  const decoder = new TextDecoder()
  return decoder.decode(bytes.subarray(0, at)) + insert + decoder.decode(bytes.subarray(at))
}
