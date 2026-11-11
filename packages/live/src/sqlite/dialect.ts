export { SQLITE, numberParams, paramValues, tokensOf }

import type { Dialect } from '../dialect.js'

/**
 * SQLite's SQL. Offsets are JavaScript string offsets into the text with its parameters numbered (`numberParams()`),
 * which is the text the planner parses. A query of ours names a parameter `?NNN`, and is sent with every parameter
 * anonymous (`?`), its values in the order they appear: drivers bind an array of values to `?` in that order only.
 */
const SQLITE: Dialect = {
  topLevelFrom(text) {
    let depth = 0
    let seenSelect = false
    for (const token of tokensOf(text)) {
      if (token.text === '(') depth++
      else if (token.text === ')') depth--
      else if (depth === 0 && token.word === 'select') seenSelect = true
      else if (depth === 0 && seenSelect && token.word === 'from') return token.start
    }
    return undefined
  },
  fromClauseAt(text, location) {
    const tokens = tokensOf(text)
    const from = tokens.findLastIndex((t) => t.start < location && t.word === 'from')
    if (from === -1) return undefined
    let end = text.length
    let depth = 0
    for (const token of tokens.slice(from + 1)) {
      if (token.text === '(') depth++
      else if (token.text === ')' && depth > 0) depth--
      else if (token.text === ')' || (depth === 0 && (token.text === ';' || AFTER_WHERE.has(token.word!)))) {
        end = token.start
        break
      }
    }
    const start = tokens[from]!.start
    return { start, text: text.slice(start, end).trim() }
  },
  parenthesizedAt(text, location) {
    const tokens = tokensOf(text)
    const open = tokens.findIndex((t) => t.start >= location && t.text === '(')
    if (open === -1) return undefined
    let depth = 0
    for (const token of tokens.slice(open)) {
      if (token.text === '(') depth++
      else if (token.text === ')') depth--
      if (depth === 0) return text.slice(tokens[open]!.start, token.end)
    }
    return undefined
  },
  statementEnd(text) {
    return tokensOf(text)
      .filter((t) => t.text !== ';')
      .at(-1)!.end
  },
  splice(text, at, insert) {
    return text.slice(0, at) + insert + text.slice(at)
  },
  bindParams(text) {
    const params: number[] = []
    let bound = ''
    let copied = 0
    for (const token of tokensOf(text)) {
      if (!token.param) continue
      params.push(Number(token.text.slice(1)) - 1)
      bound += `${text.slice(copied, token.start)}?`
      copied = token.end
    }
    return { text: bound + text.slice(copied), params }
  },
  param: (number) => `?${number}`,
  likeEscape: undefined,
  text: (expression) => `CAST(${expression} AS TEXT)`,
  jsonArray: (expressions) => `json_array(${expressions.join(', ')})`,
  jsonAgg: (expression) => `coalesce(json_group_array(${expression}), '[]')`,
  // SQLite converts an operand to the affinity of the column it's compared with as it compares: the value it ends up
  // with has the text form `CAST` gives the operand (a REAL bound to a TEXT column compares as `'10.0'`).
  operand: (expression, _node, { form }) =>
    form === 'list'
      ? undefined
      : form === 'nullness'
        ? `CASE WHEN (${expression}) IS NULL THEN NULL ELSE '' END`
        : `CAST((${expression}) AS TEXT)`,
}

/** Keywords ending a `FROM … WHERE …` at its own depth. */
const AFTER_WHERE = new Set([
  'group',
  'having',
  'window',
  'order',
  'limit',
  'union',
  'intersect',
  'except',
  'returning',
])

/** `text` with each parameter written `?NNN`, by its number (`paramsOf()`). */
function numberParams(text: string): string {
  let numbered = ''
  let copied = 0
  for (const { token, number } of paramsOf(text)) {
    numbered += `${text.slice(copied, token.start)}?${number}`
    copied = token.end
  }
  return numbered + text.slice(copied)
}

/**
 * The values of `text`'s parameters by number, as SQLite binds the app's: a named one by its name, with or without
 * its prefix, each other number the next of `anonymous`. `undefined` when `named` lacks a name.
 */
function paramValues(
  text: string,
  anonymous: readonly unknown[],
  named: Record<string, unknown>,
): unknown[] | undefined {
  const names: (string | undefined)[] = []
  for (const { token, number } of paramsOf(text)) {
    names.length = Math.max(names.length, number)
    if (token.text[0] !== '?') names[number - 1] = token.text
  }
  const values: unknown[] = []
  let next = 0
  for (const name of names) {
    if (name === undefined) {
      values.push(anonymous[next++])
      continue
    }
    const key = [name, name.slice(1)].find((key) => Object.hasOwn(named, key))
    if (key === undefined) return undefined
    values.push(named[key])
  }
  return values
}

/** `text`'s parameters, each with the number SQLite gives it: `?` one more than the largest number before it, `?NNN`
 *  its own, `:name`, `@name` and `$name` the same number at each use. */
function* paramsOf(text: string): Generator<{ token: Token; number: number }> {
  let largest = 0
  const names = new Map<string, number>()
  for (const token of tokensOf(text)) {
    if (!token.param) continue
    let number: number
    if (token.text === '?') number = largest + 1
    else if (token.text[0] === '?') number = Number(token.text.slice(1))
    else {
      number = names.get(token.text) ?? largest + 1
      names.set(token.text, number)
    }
    largest = Math.max(largest, number)
    yield { token, number }
  }
}

type Token = { text: string; start: number; end: number; word?: string; param?: true }

// Comments and whitespace (skipped), quoted strings and names, blobs, parameters, numbers, words, any other character.
const TOKEN =
  /\s+|--[^\n]*|\/\*[\s\S]*?(?:\*\/|$)|'(?:[^']|'')*'?|"(?:[^"]|"")*"?|`(?:[^`]|``)*`?|\[[^\]]*\]?|[xX]'[^']*'?|\?\d*|[:@$][\p{L}\p{N}_$]+|\d[\d.]*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?|[\p{L}_][\p{L}\p{N}_$]*|[\s\S]/uy

/** The tokens of `text`, as SQLite's tokenizer splits it, without comments and whitespace. */
function tokensOf(text: string): Token[] {
  const tokens: Token[] = []
  TOKEN.lastIndex = 0
  for (let match = TOKEN.exec(text); match; match = TOKEN.exec(text)) {
    const [token] = match
    const start = match.index
    const first = token[0]!
    if (/\s/.test(first) || token.startsWith('--') || token.startsWith('/*')) continue
    const end = start + token.length
    if (first === '?' || (/[:@$]/.test(first) && token.length > 1))
      tokens.push({ text: token, start, end, param: true })
    else if (/[\p{L}_]/u.test(first) && !/^[xX]'/.test(token))
      tokens.push({ text: token, start, end, word: token.toLowerCase() })
    else tokens.push({ text: token, start, end })
  }
  return tokens
}
