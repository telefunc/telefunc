export { loadParser, parseStatements, names, walk, tablesIn, functionNamesIn }
export type { Node, RangeVar }

import { loadModule, parseSync } from 'libpg-query'

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
