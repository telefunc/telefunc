export { parseSqlite, offsetOf }
export type { Ast, Span }

import { assertUsage } from '../assert.js'
import { processWide } from '../processWide.js'
import { tokensOf } from './dialect.js'

/** A node of sqlparser-rs's SQLite tree, as `@guanmingchiu/sqlparser-ts` gives it (serde's JSON of the Rust AST). */
type Ast = Record<string, any>
/** Where a node starts and ends: lines and columns from 1, columns counting characters. */
type Span = { start: { line: number; column: number }; end: { line: number; column: number } }

type Parser = { parse_sql(dialect: string, sql: string): Ast[] }

/** The statements of `text`, or `null` when the parser can't read it. */
function parseSqlite(text: string): Ast[] | null {
  const parser = processWide(`sqlparser ${import.meta.url}`, loadParser)
  const statements = parseWith(parser, text)
  if (statements) return statements
  const bare = bareConflictTargets(text)
  return bare === null ? null : parseWith(parser, bare)
}

function parseWith(parser: Parser, text: string): Ast[] | null {
  try {
    return parser.parse_sql('sqlite', text)
  } catch {
    return null
  }
}

/**
 * `text` with the columns of each upsert's conflict target unqualified, at the same offsets, or `null` when it has
 * none to unqualify. sqlparser-rs takes bare names only there, while Drizzle writes `ON CONFLICT ("todos"."id")`,
 * which SQLite reads as the column `id` of the table inserted into, the only table in scope.
 */
function bareConflictTargets(text: string): string | null {
  const tokens = tokensOf(text)
  let bare = text
  for (let i = 0; i + 2 < tokens.length; i++) {
    if (tokens[i]!.word !== 'on' || tokens[i + 1]!.word !== 'conflict' || tokens[i + 2]!.text !== '(') continue
    for (let j = i + 3, depth = 1; j < tokens.length && depth > 0; j++) {
      const { text: token, end } = tokens[j]!
      if (token === '(') depth++
      if (token === ')') depth--
      if (token !== '.') continue
      const start = tokens[j - 1]!.start
      bare = bare.slice(0, start) + ' '.repeat(end - start) + bare.slice(end)
    }
  }
  return bare === text ? null : bare
}

/** Loads the parser's WASM at once: better-sqlite3 and bun:sqlite plan a statement before it runs, without waiting.
 *  The package's `init()` reads the same file, asynchronously; its glue loads it synchronously. */
function loadParser(): Parser {
  const builtin = (globalThis as { process?: { getBuiltinModule?(name: string): any } }).process?.getBuiltinModule
  assertUsage(
    builtin,
    'SQLite support needs Node.js 22.3+ or Bun: the SQL parser loads through process.getBuiltinModule().',
  )
  const { createRequire } = builtin('node:module')
  const { readFileSync } = builtin('node:fs')
  const { fileURLToPath, pathToFileURL } = builtin('node:url')
  const require = createRequire(import.meta.url)
  const main = pathToFileURL(require.resolve('@guanmingchiu/sqlparser-ts'))
  const glue = require(fileURLToPath(new URL('../wasm/sqlparser_rs_wasm.js', main)))
  glue.initSync({ module: readFileSync(new URL('../wasm/sqlparser_rs_wasm_bg.wasm', main)) })
  return glue
}

/** The offset in `text` of a span's line and column. */
function offsetOf(text: string, { line, column }: Span['start']): number {
  let at = 0
  for (let l = 1; l < line; l++) at = text.indexOf('\n', at) + 1
  for (let c = 1; c < column; c++) at += text.codePointAt(at)! > 0xffff ? 2 : 1
  return at
}
