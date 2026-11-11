export { loadParser, parseStatements, topLevelFrom, fromClauseAt, parenthesizedAt, statementEnd, spliceBytes }
export { renumberParams }

import { loadModule, parseSync, scanSync } from 'libpg-query'
import type { Node } from '../tree.js'

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
