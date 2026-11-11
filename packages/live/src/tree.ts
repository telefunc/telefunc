export { names, walk, tablesIn, isAggregateCall, splitAnd, quote }
export type { Node, RangeVar }

// The parse tree the matcher reads, Postgres's (SQLite's statements are lowered into it), and what reads it: no parser.

/** A node of the Postgres parse tree: `{ SelectStmt: { ... } }`, `{ ColumnRef: { ... } }`, ... */
type Node = Record<string, any>
type RangeVar = { schemaname?: string; relname: string; alias?: { aliasname: string } }

/** `[{ String: { sval: 'public' } }, { String: { sval: 'todos' } }]` → `['public', 'todos']`; `*` for `A_Star`. */
function names(list: Node[] | undefined): string[] {
  return (list ?? []).map((n) => (n.String ? (n.String.sval as string) : '*'))
}

/** Calls `visit` on every node object below `node`, depth first; `visit` returning `false` skips the node's children. */
function walk(node: unknown, visit: (key: string, value: Node) => boolean | void): void {
  if (Array.isArray(node)) {
    for (const item of node) walk(item, visit)
    return
  }
  if (!node || typeof node !== 'object') return
  for (const [key, value] of Object.entries(node)) {
    if (!value || typeof value !== 'object') continue
    if (!Array.isArray(value) && visit(key, value) === false) continue
    walk(value, visit)
  }
}

/** Every table name mentioned below `node`, CTE names included (the catalog tells them apart). */
function tablesIn(node: unknown): RangeVar[] {
  const tables: RangeVar[] = []
  walk(node, (key, value) => {
    if (key === 'RangeVar' || key === 'relation') tables.push(value as RangeVar)
  })
  return tables
}

const AGGREGATES = new Set([
  'count',
  'sum',
  'avg',
  'min',
  'max',
  'array_agg',
  'json_agg',
  'jsonb_agg',
  'string_agg',
  'json_object_agg',
  'jsonb_object_agg',
  'bool_and',
  'bool_or',
  'every',
  'bit_and',
  'bit_or',
  'stddev',
  'variance',
])

/** Whether a `FuncCall` node calls an aggregate. */
function isAggregateCall(call: Node): boolean {
  return Boolean(call.agg_star) || AGGREGATES.has(names(call.funcname).at(-1)!)
}

/** The conjuncts of `a AND b AND …`. */
function splitAnd(node: Node | undefined): Node[] {
  if (!node) return []
  if (node.BoolExpr?.boolop === 'AND_EXPR') return (node.BoolExpr.args as Node[]).flatMap(splitAnd)
  return [node]
}

/** `identifier` as a quoted SQL identifier. */
function quote(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}
