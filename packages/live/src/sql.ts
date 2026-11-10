export { loadParser, parseStatements, names, walk, tablesIn, functionNamesIn, isAggregateCall, splitAnd, quote }
export { topLevelFrom, fromClauseAt, parenthesizedAt, statementEnd, spliceBytes, renumberParams }
export type { Node, RangeVar }

import { loadModule, parseSync, scanSync } from 'libpg-query'

/** A node of the Postgres parse tree: `{ SelectStmt: { ... } }`, `{ ColumnRef: { ... } }`, ... */
type Node = Record<string, any>
type RangeVar = { schemaname?: string; relname: string; alias?: { aliasname: string } }

let parserLoaded: Promise<void> | undefined
function loadParser(): Promise<void> {
  parserLoaded ??= loadModule()
  return parserLoaded
}

/** The statements of `text`, or `null` when Postgres's own parser rejects it (the database will report why). */
function parseStatements(text: string): Node[] | null {
  try {
    return parseSync(text).stmts?.map((s) => s.stmt as Node) ?? []
  } catch {
    return null
  }
}

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

/** The last part of every function name called below `node`. */
function functionNamesIn(node: unknown): string[] {
  const found: string[] = []
  walk(node, (key, value) => {
    if (key === 'FuncCall') found.push(names(value.funcname).at(-1)!)
  })
  return found
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

// ── Text (the scanner's offsets are UTF-8 byte offsets) ──

/** Keywords ending a `FROM … WHERE …` at its own depth. */
const AFTER_WHERE = new Set([
  'group',
  'having',
  'window',
  'order',
  'limit',
  'offset',
  'fetch',
  'for',
  'union',
  'intersect',
  'except',
])

/** Byte offset of the `FROM` keyword of the outermost `SELECT`. */
function topLevelFrom(text: string): number | undefined {
  let depth = 0
  let seenSelect = false
  for (const token of scanSync(text).tokens) {
    if (token.text === '(') depth++
    else if (token.text === ')') depth--
    else if (depth === 0 && keyword(token) === 'select') seenSelect = true
    else if (depth === 0 && seenSelect && keyword(token) === 'from') return token.start
  }
  return undefined
}

/** The `FROM` naming a table at byte `location`, up to the end of its `WHERE`: the byte offset of the keyword, and
 *  the text. */
function fromClauseAt(text: string, location: number): { start: number; text: string } | undefined {
  const tokens = scanSync(text).tokens
  const from = tokens.findLastIndex((t) => t.start < location && keyword(t) === 'from')
  if (from === -1) return undefined
  const bytes = new TextEncoder().encode(text)
  let end = bytes.length
  let depth = 0
  for (const token of tokens.slice(from + 1)) {
    if (token.text === '(') depth++
    else if (token.text === ')' && depth > 0) depth--
    else if (token.text === ')' || (depth === 0 && (token.text === ';' || AFTER_WHERE.has(keyword(token)!)))) {
      end = token.start
      break
    }
  }
  const start = tokens[from]!.start
  return { start, text: new TextDecoder().decode(bytes.subarray(start, end)).trim() }
}

/** The text from the first `(` at or after byte `location` to its matching `)`. */
function parenthesizedAt(text: string, location: number): string | undefined {
  const tokens = scanSync(text).tokens
  const open = tokens.findIndex((t) => t.start >= location && t.text === '(')
  if (open === -1) return undefined
  let depth = 0
  for (const token of tokens.slice(open)) {
    if (token.text === '(') depth++
    else if (token.text === ')') depth--
    if (depth === 0)
      return new TextDecoder().decode(new TextEncoder().encode(text).subarray(tokens[open]!.start, token.end))
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

/** `text` with its parameters renumbered `$1`, `$2`, … in order of first use, and the index each had before. */
function renumberParams(text: string): { text: string; params: number[] } {
  const bytes = new TextEncoder().encode(text)
  const decoder = new TextDecoder()
  const params: number[] = []
  let renumbered = ''
  let copied = 0
  for (const token of scanSync(text).tokens) {
    if (token.tokenName !== 'PARAM') continue
    const index = Number(token.text.slice(1)) - 1
    if (!params.includes(index)) params.push(index)
    renumbered += `${decoder.decode(bytes.subarray(copied, token.start))}$${params.indexOf(index) + 1}`
    copied = token.end
  }
  return { text: renumbered + decoder.decode(bytes.subarray(copied)), params }
}

function keyword(token: { text: string; keywordKind: number }): string | undefined {
  return token.keywordKind !== 0 ? token.text.toLowerCase() : undefined
}
