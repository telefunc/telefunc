export { tokenize, textOf, isText, splitOn, stripParens, readAggCall }
export type { SqlToken }

import { Column, Param, Placeholder, SQL, StringChunk, is } from 'drizzle-orm'
import type { AggFn } from '../../ir/types.js'

// drizzle conditions carry no AST — only `SQL.queryChunks`: interleaved string
// fragments, Column/Param objects, nested SQL (the operands of and/or/not), raw
// value arrays (inArray lists) and Placeholders. `tokenize` normalizes one node's
// chunks into a flat token stream; the predicate grammar walks it with `ChunkCursor`.

type SqlToken =
  | { kind: 'text'; text: string }
  | { kind: 'column'; column: Column }
  | { kind: 'param'; value: unknown } // a bound Param or a bare interpolated literal (like patterns)
  | { kind: 'placeholder'; name: string }
  | { kind: 'list'; items: SqlToken[] } // an inArray value list
  | { kind: 'sql'; tokens: SqlToken[]; sql: SQL } // a nested SQL node, pre-tokenized (the source SQL kept for hydration)
  | { kind: 'opaque'; value: unknown } // subquery, embedded builder, table, raw SQL — not evaluable per row

function tokenize(chunks: readonly unknown[]): SqlToken[] {
  const tokens: SqlToken[] = []
  for (const chunk of chunks) {
    const token = toToken(chunk)
    if (token) tokens.push(token)
  }
  return tokens
}

function toToken(chunk: unknown): SqlToken | null {
  if (is(chunk, StringChunk)) {
    const text = chunk.value.join('')
    return text === '' ? null : { kind: 'text', text }
  }
  if (is(chunk, Column)) return { kind: 'column', column: chunk }
  if (is(chunk, Param)) return { kind: 'param', value: chunk.value }
  if (is(chunk, Placeholder)) return { kind: 'placeholder', name: chunk.name }
  if (is(chunk, SQL)) return { kind: 'sql', tokens: tokenize(chunk.queryChunks), sql: chunk }
  if (Array.isArray(chunk))
    return { kind: 'list', items: chunk.map((item) => toToken(item) ?? { kind: 'opaque', value: item }) }
  if (chunk === undefined) return null
  if (typeof chunk === 'object' && chunk !== null) return { kind: 'opaque', value: chunk }
  // a bare primitive interpolated without a Param wrapper (drizzle does this for like/ilike patterns)
  return { kind: 'param', value: chunk }
}

function textOf(token: SqlToken | undefined): string | null {
  return token?.kind === 'text' ? token.text : null
}

function isText(token: SqlToken | undefined, text: string): boolean {
  return textOf(token) === text
}

/** Split a token list on a top-level text separator (` and ` / ` or `). */
function splitOn(tokens: SqlToken[], separator: string): SqlToken[][] {
  const groups: SqlToken[][] = [[]]
  for (const token of tokens) {
    if (isText(token, separator)) groups.push([])
    else groups[groups.length - 1]!.push(token)
  }
  return groups
}

/** Peel one balanced wrapping paren pair (`( … )`, or `not ( … )`) off a token list.
 *  Only strips when the parens are dedicated markers, so a fused `is null)` is left
 *  for the atom parser and `(a) or (b)` is not wrongly stripped. */
function stripParens(tokens: SqlToken[]): SqlToken[] {
  const first = tokens[0]
  const last = tokens[tokens.length - 1]
  if (
    tokens.length >= 2 &&
    first?.kind === 'text' &&
    last?.kind === 'text' &&
    first.text.endsWith('(') &&
    last.text.startsWith(')')
  ) {
    const stripped = tokens.slice(1, -1)
    if (first.text !== '(') stripped.unshift({ kind: 'text', text: first.text.slice(0, -1) })
    if (last.text !== ')') stripped.push({ kind: 'text', text: last.text.slice(1) })
    return stripped
  }
  return tokens
}

const AGG_FNS = new Set<AggFn>(['count', 'sum', 'avg', 'min', 'max'])

/** Read an aggregate call SQL (`count(*)`, `sum(col)`, `count(distinct col)`) into
 *  its function, argument column and flags, or null when it isn't one. */
function readAggCall(sql: SQL): { fn: AggFn; arg?: Column; star: boolean; distinct: boolean } | null {
  const tokens = tokenize(sql.queryChunks)
  const head = textOf(tokens[0])
  const match = head && /^([a-z]+)\(\s*(distinct\s*)?$/i.exec(head)
  if (!match) return null
  const fn = match[1]!.toLowerCase() as AggFn
  if (!AGG_FNS.has(fn)) return null
  const distinct = Boolean(match[2])
  const arg = tokens[1]
  if (arg?.kind === 'column') return { fn, arg: arg.column, star: false, distinct }
  if (arg?.kind === 'sql' && arg.tokens.length === 1 && textOf(arg.tokens[0]) === '*')
    return { fn, star: true, distinct }
  return { fn, star: head!.includes('*'), distinct }
}
