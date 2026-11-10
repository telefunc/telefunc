export { analyzeTally, MAX_TALLY_ROWS }
export type { TallyShape, TallyContext, ColumnAggregate, Comparison, SetTree }

import { fromClauseAt, isAggregateCall, names, quote, splitAnd, walk, type Node } from '../sql.js'
import { COMPARISONS } from './condition.js'
import { isOperand, type Operands } from './operands.js'
import type { Alias } from './readShape.js'
import { hasCanonicalText, type Column } from './values.js'

/** A read decided by how many matching rows have which values: a grouped aggregate with `HAVING`, `min`, `max` or
 *  `count(DISTINCT)`, or a set operation (`UNION`, `INTERSECT`, `EXCEPT`). A query run right after the read counts
 *  the rows; each change then moves the counts, and counts only when they can change what the read returned. */
type TallyShape = {
  /** The one table of an aggregate, or each branch of a set operation. */
  sources: TallySource[]
  rule: GroupRule | SetRule
}

type TallySource = {
  alias: Alias
  /** The columns its rows are counted by: the group's (`groupColumns` of them), then the columns `min`, `max` and
   *  `count(DISTINCT)` read. A set operation's branch has only group columns: its output. */
  columns: Column[]
  groupColumns: number
}

type GroupRule = {
  kind: 'groups'
  /** Without `GROUP BY` there is one group, in the result even without rows. */
  grouped: boolean
  /** The hidden columns holding each result row's group columns. */
  groupHidden: number[]
  /** `min`, `max` and `count(DISTINCT)` in the select list, and the hidden column holding each value. */
  outputs: { aggregate: ColumnAggregate; hidden: number }[]
  /** Another aggregate is in the select list: any change to its group can change it. */
  otherOutputs: boolean
  /** `HAVING`, as comparisons that all hold. */
  having: Comparison[]
}

/** `count(*)`, or an aggregate of the source's `columns[column]`. */
type Aggregate = { kind: 'count' } | ColumnAggregate
type ColumnAggregate = { kind: 'min' | 'max' | 'countDistinct'; column: number }
/** `aggregate operator value`, `value` an operand of the read (`Operands`). */
type Comparison = { aggregate: Aggregate; operator: string; operand: number }

/** The result's rows are the hidden columns: a set operation's output. */
type SetRule = { kind: 'set'; tree: SetTree }

/** A branch, by its source's index, or a set operation of two. */
type SetTree = number | { op: 'union' | 'intersect' | 'except'; left: SetTree; right: SetTree }

/** What the analysis of the whole statement tells. */
type TallyContext = {
  text: string
  /** Every table occurrence in the statement. */
  aliases: Alias[]
  /** The one table a plain `SELECT` reads, without subqueries or functions in its `FROM`. */
  tableOf: (select: Node) => Alias | undefined
  /** The column of that table `ref` names. */
  columnOf: (select: Node, ref: Node) => Column | undefined
  operands: Operands
}

type TallyAnalysis = {
  shape: TallyShape
  hiddenColumns: string[]
  hiddenAt?: { from: number; columns: string[] }[]
  /** Counts the matching rows of each source by its columns: `[source, ...texts]` as JSON, and a count. */
  query: string
}

/** More rows of counts aren't kept: any change to a matching row counts. */
const MAX_TALLY_ROWS = 10_000

const SET_OPERATIONS: Record<string, 'union' | 'intersect' | 'except'> = {
  SETOP_UNION: 'union',
  SETOP_INTERSECT: 'intersect',
  SETOP_EXCEPT: 'except',
}

function analyzeTally(select: Node, context: TallyContext): TallyAnalysis | undefined {
  return select.op in SET_OPERATIONS ? analyzeSetOperation(select, context) : analyzeGroups(select, context)
}

/** A grouped (or whole-table) aggregate of one table, whose `HAVING` and outputs the counts decide. */
function analyzeGroups(select: Node, context: TallyContext): TallyAnalysis | undefined {
  const alias = context.tableOf(select)
  if (!alias || context.aliases.length !== 1 || select.distinctClause || select.windowClause) return undefined
  if (select.limitCount || select.limitOffset || select.withClause) return undefined
  const column = (node: Node | undefined) => (node?.ColumnRef ? context.columnOf(select, node) : undefined)
  const columns: Column[] = []
  for (const item of (select.groupClause ?? []) as Node[]) {
    const c = column(item)
    if (!c) return undefined
    columns.push(c)
  }
  const groupColumns = columns.length
  const aggregateOf = (node: Node): Aggregate | undefined => {
    const call = node.FuncCall
    if (!call || call.over || call.agg_filter || call.agg_order) return undefined
    const name = names(call.funcname).at(-1)
    if (name === 'count' && call.agg_star) return { kind: 'count' }
    const [arg, ...more] = (call.args ?? []) as Node[]
    const c = more.length === 0 ? column(arg) : undefined
    const kind = name === 'min' || name === 'max' ? name : name === 'count' && call.agg_distinct ? 'countDistinct' : ''
    if (!c || !kind) return undefined
    const known = columns.indexOf(c, groupColumns)
    return { kind, column: known !== -1 ? known : columns.push(c) - 1 }
  }

  const qualified = (c: Column) => `${quote(alias.name)}.${quote(c.name)}`
  const hiddenColumns = columns.map((c) => `${qualified(c)}::text`)
  const outputs: GroupRule['outputs'] = []
  let otherOutputs = false
  for (const target of select.targetList as Node[]) {
    const value = target.ResTarget.val
    const aggregate = aggregateOf(value)
    if (aggregate && aggregate.kind !== 'count') {
      const c = qualified(columns[aggregate.column]!)
      const call = aggregate.kind === 'countDistinct' ? `count(DISTINCT ${c})` : `${aggregate.kind}(${c})`
      outputs.push({ aggregate, hidden: hiddenColumns.push(`${call}::text`) - 1 })
      continue
    }
    let window = false
    walk(value, (key, node) => {
      if (key !== 'FuncCall') return
      if (node.over) window = true
      if (isAggregateCall(node)) otherOutputs = true
    })
    if (window) return undefined
  }

  const having: GroupRule['having'] = []
  for (const conjunct of splitAnd(select.havingClause)) {
    const expr = conjunct.A_Expr
    const operator = expr?.kind === 'AEXPR_OP' ? names(expr.name)[0]! : ''
    const aggregate = expr && aggregateOf(expr.lexpr)
    if (!aggregate || !(operator in COMPARISONS) || !isOperand(expr.rexpr)) return
    // An untyped operand is read as the aggregate's type: `count` is a `bigint`, `min` and `max` the column's.
    const as =
      aggregate.kind === 'count' || aggregate.kind === 'countDistinct'
        ? 'pg_catalog.int8'
        : columns[aggregate.column]!.sqlType
    const operand = context.operands.of(expr.rexpr, { as })
    if (operand === undefined) return
    having.push({ aggregate, operator, operand })
  }
  // `count`, `sum` and `avg` alone change with any row of their group: the counts can't rule anything out.
  if (having.length === 0 && outputs.length === 0) return undefined

  const from = fromClauseAt(context.text, select.fromClause[0].RangeVar.location)
  if (!from) return undefined
  const source = { alias, columns, groupColumns }
  const groupHidden = columns.slice(0, groupColumns).map((_, i) => i)
  return {
    shape: {
      sources: [source],
      rule: { kind: 'groups', grouped: groupColumns > 0, groupHidden, outputs, otherOutputs, having },
    },
    hiddenColumns,
    query: countsQuery([source], [from.text]),
  }
}

/** `UNION`, `INTERSECT` and `EXCEPT` (without `ALL`) of plain `SELECT`s of one table's columns. */
function analyzeSetOperation(select: Node, context: TallyContext): TallyAnalysis | undefined {
  if (select.limitCount || select.limitOffset || select.withClause) return undefined
  const sources: TallySource[] = []
  const fromClauses: string[] = []
  const hiddenAt: { from: number; columns: string[] }[] = []
  const treeOf = (node: Node): SetTree | undefined => {
    if (node.limitCount || node.limitOffset || node.withClause) return undefined
    if (node.op !== 'SETOP_NONE') {
      // spellcheck-ignore: `larg` and `rarg` are the parse tree's names
      const [left, right] = [treeOf(node.larg), treeOf(node.rarg)]
      const op = SET_OPERATIONS[node.op]
      return op && !node.all && left !== undefined && right !== undefined ? { op, left, right } : undefined
    }
    const alias = context.tableOf(node)
    if (!alias || node.groupClause || node.havingClause || node.distinctClause || node.windowClause) return undefined
    const columns: Column[] = []
    for (const target of node.targetList as Node[]) {
      const value = target.ResTarget.val
      const c = value.ColumnRef ? context.columnOf(node, value) : undefined
      // The hidden columns are part of each output row: equal rows must stay equal.
      if (!c || !hasCanonicalText(c)) return undefined
      columns.push(c)
    }
    const from = fromClauseAt(context.text, node.fromClause[0].RangeVar.location)
    if (!from) return undefined
    fromClauses.push(from.text)
    hiddenAt.push({ from: from.start, columns: columns.map((c) => `${quote(alias.name)}.${quote(c.name)}::text`) })
    return sources.push({ alias, columns, groupColumns: columns.length }) - 1
  }
  const tree = treeOf(select)
  // A subquery in a branch reads tables the counts don't follow.
  if (tree === undefined || context.aliases.length !== sources.length) return undefined
  return {
    shape: { sources, rule: { kind: 'set', tree } },
    hiddenColumns: hiddenAt[0]!.columns,
    hiddenAt,
    query: countsQuery(sources, fromClauses),
  }
}

function countsQuery(sources: TallySource[], fromClauses: string[]): string {
  const counts = sources.map((source, i) => {
    const texts = source.columns.map((c) => `${quote(source.alias.name)}.${quote(c.name)}::text`)
    return `SELECT to_json(ARRAY[${[`'${i}'`, ...texts].join(', ')}])::text, count(*)::text ${fromClauses[i]} GROUP BY 1`
  })
  return `${counts.join(' UNION ALL ')} LIMIT ${MAX_TALLY_ROWS + 1}`
}
