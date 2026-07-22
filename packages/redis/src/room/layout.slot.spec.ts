import { describe, expect, it } from 'vitest'
import {
  cellKey,
  channelKey,
  directoryIndexKey,
  DIRECTORY_PUT_LUA,
  directoryTagsKey,
  generationInvalidationChannel,
  generationTokensKey,
  gensKey,
  headKey,
  headRevKey,
  orderKey,
  REDIS_ROOM_COMMAND_KEYS,
  REDIS_ROOM_COMMANDS,
  retainedKey,
  retainedSizeKey,
  revKey,
  routeCaptureExpiriesKey,
  routeCapturesKey,
} from './layout.js'

// Redis Cluster's CRC16/XMODEM key-slot algorithm. Keeping it here makes the one-room-slot claim
// executable without claiming the real Cluster behavior that remains W4-R.
export function redisSlot(key: string): number {
  const start = key.indexOf('{')
  const end = start < 0 ? -1 : key.indexOf('}', start + 1)
  const tagged = start >= 0 && end > start + 1 ? key.slice(start + 1, end) : key
  let crc = 0
  for (const byte of new TextEncoder().encode(tagged)) {
    crc ^= byte << 8
    for (let bit = 0; bit < 8; bit++) crc = (crc & 0x8000) === 0 ? crc << 1 : (crc << 1) ^ 0x1021
  }
  return crc & 0x3fff
}

type Provenance = 'KEYS' | 'ARGV' | 'data' | 'unknown' | `data-record:${number}`

type LuaToken = {
  kind: 'identifier' | 'number' | 'string' | 'symbol' | 'newline'
  value: string
}

type LuaStatement =
  | { kind: 'simple'; tokens: readonly LuaToken[] }
  | { kind: 'function'; local: boolean; name: string; parameters: readonly string[]; body: readonly LuaStatement[] }
  | {
      kind: 'if'
      branches: readonly { condition: readonly LuaToken[]; body: readonly LuaStatement[] }[]
      elseBody?: readonly LuaStatement[]
    }
  | { kind: 'loop'; loopKind: 'for' | 'while'; header: readonly LuaToken[]; body: readonly LuaStatement[] }
  | { kind: 'do'; body: readonly LuaStatement[] }

const OPEN = new Map([
  ['(', ')'],
  ['[', ']'],
  ['{', '}'],
])

const UNSUPPORTED_STATEMENTS = new Set(['goto', 'repeat', 'until'])
const LUA_KEYWORDS = new Set([
  'and',
  'break',
  'do',
  'else',
  'elseif',
  'end',
  'false',
  'for',
  'function',
  'if',
  'in',
  'local',
  'nil',
  'not',
  'or',
  'return',
  'then',
  'true',
  'while',
])

function lexLua(lua: string): LuaToken[] {
  const tokens: LuaToken[] = []
  for (let index = 0; index < lua.length; ) {
    const char = lua[index] as string
    if (char === '\r' || char === '\n') {
      if (char === '\r' && lua[index + 1] === '\n') index++
      tokens.push({ kind: 'newline', value: '\n' })
      index++
    } else if (/\s/.test(char)) {
      index++
    } else if (char === '-' && lua[index + 1] === '-') {
      while (index < lua.length && lua[index] !== '\r' && lua[index] !== '\n') index++
    } else if (char === "'" || char === '"') {
      const quote = char
      let value = ''
      index++
      while (index < lua.length && lua[index] !== quote) {
        if (lua[index] === '\\' && index + 1 < lua.length) {
          value += lua[index + 1]
          index += 2
        } else {
          value += lua[index]
          index++
        }
      }
      if (lua[index] !== quote) throw new Error('unterminated Lua string')
      index++
      tokens.push({ kind: 'string', value })
    } else if (/[A-Za-z_]/.test(char)) {
      const start = index++
      while (index < lua.length && /[A-Za-z0-9_]/.test(lua[index] as string)) index++
      tokens.push({ kind: 'identifier', value: lua.slice(start, index) })
    } else if (/\d/.test(char)) {
      const start = index++
      while (index < lua.length && /[\d.]/.test(lua[index] as string)) index++
      tokens.push({ kind: 'number', value: lua.slice(start, index) })
    } else {
      if ((char === '[' && lua[index + 1] === '[') || (char === ']' && lua[index + 1] === ']')) {
        throw new Error('unsupported Lua long-string syntax')
      }
      const pair = lua.slice(index, index + 2)
      if (['==', '~=', '<=', '>=', '..'].includes(pair)) {
        tokens.push({ kind: 'symbol', value: pair })
        index += 2
      } else {
        tokens.push({ kind: 'symbol', value: char })
        index++
      }
    }
  }
  return tokens
}

class LuaStatementParser {
  readonly #tokens: readonly LuaToken[]
  #index = 0

  constructor(tokens: readonly LuaToken[]) {
    this.#tokens = tokens
  }

  parse(): readonly LuaStatement[] {
    const statements = this.#parseBlock(new Set())
    if (this.#index !== this.#tokens.length) throw new Error(`unexpected Lua token '${this.#peek()?.value ?? ''}'`)
    return statements
  }

  #parseBlock(stops: ReadonlySet<string>): LuaStatement[] {
    const statements: LuaStatement[] = []
    while (this.#index < this.#tokens.length) {
      this.#skipSeparators()
      const token = this.#peek()
      if (token === undefined || stops.has(token.value)) break
      if (UNSUPPORTED_STATEMENTS.has(token.value) || token.value === ':') {
        throw new Error(`unsupported Lua control syntax '${token.value}'`)
      }
      if (token.value === 'local' && this.#peek(1)?.value === 'function') {
        this.#index++
        statements.push(this.#parseFunction(true))
      } else if (token.value === 'function') {
        statements.push(this.#parseFunction(false))
      } else if (token.value === 'if') {
        statements.push(this.#parseIf())
      } else if (token.value === 'for' || token.value === 'while') {
        statements.push(this.#parseLoop())
      } else if (token.value === 'do') {
        this.#index++
        const body = this.#parseBlock(new Set(['end']))
        this.#expect('end')
        statements.push({ kind: 'do', body })
      } else {
        const tokens = this.#takeSimple(stops)
        if (tokens.length === 0) throw new Error(`unsupported Lua statement '${token.value}'`)
        statements.push({ kind: 'simple', tokens })
      }
    }
    return statements
  }

  #parseFunction(local: boolean): LuaStatement {
    this.#expect('function')
    const name = this.#takeIdentifier()
    this.#expect('(')
    const parameters: string[] = []
    while (this.#peek()?.value !== ')') {
      parameters.push(this.#takeIdentifier())
      if (this.#peek()?.value !== ',') break
      this.#index++
    }
    this.#expect(')')
    const body = this.#parseBlock(new Set(['end']))
    this.#expect('end')
    return { kind: 'function', local, name, parameters, body }
  }

  #parseIf(): LuaStatement {
    this.#expect('if')
    const branches: { condition: readonly LuaToken[]; body: readonly LuaStatement[] }[] = []
    let condition = this.#takeUntil('then')
    this.#expect('then')
    branches.push({ condition, body: this.#parseBlock(new Set(['elseif', 'else', 'end'])) })
    while (this.#peek()?.value === 'elseif') {
      this.#index++
      condition = this.#takeUntil('then')
      this.#expect('then')
      branches.push({ condition, body: this.#parseBlock(new Set(['elseif', 'else', 'end'])) })
    }
    let elseBody: readonly LuaStatement[] | undefined
    if (this.#peek()?.value === 'else') {
      this.#index++
      elseBody = this.#parseBlock(new Set(['end']))
    }
    this.#expect('end')
    return { kind: 'if', branches, ...(elseBody === undefined ? {} : { elseBody }) }
  }

  #parseLoop(): LuaStatement {
    const loopKind = this.#peek()?.value
    if (loopKind !== 'for' && loopKind !== 'while') throw new Error(`unsupported Lua loop '${loopKind ?? ''}'`)
    this.#index++
    const header = this.#takeUntil('do')
    this.#expect('do')
    const body = this.#parseBlock(new Set(['end']))
    this.#expect('end')
    return { kind: 'loop', loopKind, header, body }
  }

  #takeUntil(value: string): LuaToken[] {
    const result: LuaToken[] = []
    const closers: string[] = []
    while (this.#index < this.#tokens.length) {
      const token = this.#peek() as LuaToken
      if (closers.length === 0 && token.value === value) break
      this.#updateDelimiters(token, closers)
      if (token.kind !== 'newline') result.push(token)
      this.#index++
    }
    if (this.#peek()?.value !== value) throw new Error(`expected Lua '${value}'`)
    return result
  }

  #takeSimple(stops: ReadonlySet<string>): LuaToken[] {
    const result: LuaToken[] = []
    const closers: string[] = []
    while (this.#index < this.#tokens.length) {
      const token = this.#peek() as LuaToken
      if (closers.length === 0 && (token.kind === 'newline' || token.value === ';' || stops.has(token.value))) {
        break
      }
      this.#updateDelimiters(token, closers)
      result.push(token)
      this.#index++
    }
    return result
  }

  #updateDelimiters(token: LuaToken, closers: string[]): void {
    const closer = OPEN.get(token.value)
    if (closer !== undefined) closers.push(closer)
    else if (closers.at(-1) === token.value) closers.pop()
  }

  #skipSeparators(): void {
    while (this.#peek()?.kind === 'newline' || this.#peek()?.value === ';') this.#index++
  }

  #takeIdentifier(): string {
    const token = this.#peek()
    if (token?.kind !== 'identifier') throw new Error(`expected Lua identifier, got '${token?.value ?? ''}'`)
    this.#index++
    return token.value
  }

  #expect(value: string): void {
    if (this.#peek()?.value !== value) throw new Error(`expected Lua '${value}', got '${this.#peek()?.value ?? ''}'`)
    this.#index++
  }

  #peek(offset = 0): LuaToken | undefined {
    return this.#tokens[this.#index + offset]
  }
}

type LuaFunction = {
  parameters: readonly string[]
  body: readonly LuaStatement[]
}

function splitTokens(tokens: readonly LuaToken[], separator: string): LuaToken[][] {
  const parts: LuaToken[][] = [[]]
  const closers: string[] = []
  for (const token of tokens) {
    const closer = OPEN.get(token.value)
    if (closer !== undefined) closers.push(closer)
    else if (closers.at(-1) === token.value) closers.pop()
    if (token.value === separator && closers.length === 0) parts.push([])
    else (parts.at(-1) as LuaToken[]).push(token)
  }
  return parts
}

function topLevelTokenIndex(tokens: readonly LuaToken[], value: string): number {
  const closers: string[] = []
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index] as LuaToken
    if (token.value === value && closers.length === 0) return index
    const closer = OPEN.get(token.value)
    if (closer !== undefined) closers.push(closer)
    else if (closers.at(-1) === token.value) closers.pop()
  }
  return -1
}

function withoutOuterParens(tokens: readonly LuaToken[]): readonly LuaToken[] {
  let result = tokens
  while (result[0]?.value === '(' && result.at(-1)?.value === ')') {
    let depth = 0
    let closesAtEnd = false
    for (let index = 0; index < result.length; index++) {
      if (result[index]?.value === '(') depth++
      else if (result[index]?.value === ')') depth--
      if (depth === 0) {
        closesAtEnd = index === result.length - 1
        break
      }
    }
    if (!closesAtEnd) break
    result = result.slice(1, -1)
  }
  return result
}

function sourceOf(tokens: readonly LuaToken[], aliases: ReadonlyMap<string, Provenance>): Provenance {
  const expression = withoutOuterParens(tokens.filter((token) => token.kind !== 'newline'))
  const recordSource = directDataRecordSource(expression)
  if (recordSource !== undefined) return recordSource
  if (
    expression.length === 1 &&
    (expression[0]?.kind === 'number' ||
      expression[0]?.kind === 'string' ||
      LUA_KEYWORDS.has(expression[0]?.value ?? ''))
  ) {
    return 'data'
  }
  if (expression.length === 1 && expression[0]?.kind === 'identifier') {
    return aliases.get(expression[0].value) ?? 'unknown'
  }
  if (
    expression.length >= 4 &&
    (expression[0]?.value === 'KEYS' || expression[0]?.value === 'ARGV') &&
    expression[1]?.value === '[' &&
    expression.at(-1)?.value === ']'
  ) {
    let depth = 0
    for (let index = 1; index < expression.length; index++) {
      if (expression[index]?.value === '[') depth++
      else if (expression[index]?.value === ']') depth--
      if (depth === 0 && index !== expression.length - 1) return 'unknown'
    }
    return expression[0].value as Provenance
  }
  return 'unknown'
}

const DATA_RECORD_ORIGINS = new WeakMap<LuaToken, number>()
let nextDataRecordOrigin = 1

function isDataRecord(source: Provenance): source is `data-record:${number}` {
  return source.startsWith('data-record:')
}

function directDataRecordSource(tokens: readonly LuaToken[]): `data-record:${number}` | undefined {
  const expression = withoutOuterParens(tokens.filter((token) => token.kind !== 'newline'))
  const isDecode =
    expression.length >= 5 &&
    expression[0]?.value === 'cjson' &&
    expression[1]?.value === '.' &&
    expression[2]?.value === 'decode' &&
    expression[3]?.value === '(' &&
    findCallClose(expression, 3) === expression.length - 1
  const isTableLiteral = expression[0]?.value === '{' && expression.at(-1)?.value === '}'
  if (!isDecode && !isTableLiteral) return undefined
  const anchor = expression[0] as LuaToken
  let origin = DATA_RECORD_ORIGINS.get(anchor)
  if (origin === undefined) {
    origin = nextDataRecordOrigin++
    DATA_RECORD_ORIGINS.set(anchor, origin)
  }
  return `data-record:${origin}`
}

function directDataRecordMemberTarget(tokens: readonly LuaToken[]): { base: string; field: string } | undefined {
  if (
    tokens.length !== 3 ||
    tokens[0]?.kind !== 'identifier' ||
    tokens[1]?.value !== '.' ||
    tokens[2]?.kind !== 'identifier'
  ) {
    return undefined
  }
  return { base: tokens[0].value, field: tokens[2].value }
}

function referenceOf(tokens: readonly LuaToken[]): string | undefined {
  const expression = withoutOuterParens(tokens.filter((token) => token.kind !== 'newline'))
  if (expression.length % 2 === 0) return undefined
  const names: string[] = []
  for (let index = 0; index < expression.length; index++) {
    const token = expression[index] as LuaToken
    if (index % 2 === 0) {
      if (token.kind !== 'identifier') return undefined
      names.push(token.value)
    } else if (token.value !== '.') return undefined
  }
  return names.join('.')
}

function renderTokens(tokens: readonly LuaToken[]): string {
  return tokens.map((token) => (token.kind === 'string' ? `'${token.value}'` : token.value)).join(' ')
}

function findCallClose(tokens: readonly LuaToken[], open: number): number {
  let depth = 0
  for (let index = open; index < tokens.length; index++) {
    if (tokens[index]?.value === '(') depth++
    else if (tokens[index]?.value === ')') depth--
    if (depth === 0) return index
  }
  throw new Error('unterminated Lua call')
}

const SINGLE_KEY_COMMANDS = new Set([
  'GET',
  'HDEL',
  'HGET',
  'HSET',
  'INCR',
  'PEXPIRE',
  'PUBLISH',
  'SADD',
  'SET',
  'SISMEMBER',
  'SREM',
  'STRLEN',
  'ZADD',
  'ZRANGEBYSCORE',
  'ZREM',
])

const SAFE_NON_KEY_CALLS = new Set([
  'cjson.decode',
  'cjson.encode',
  'ipairs',
  'math.floor',
  'redis.error_reply',
  'string.len',
  'string.match',
  'struct.pack',
  'tonumber',
  'tostring',
  'unpack',
])

const PROTECTED_CALL_BINDINGS = new Set([
  ...SAFE_NON_KEY_CALLS,
  'cjson',
  'math',
  'redis',
  'redis.call',
  'string',
  'struct',
])

function joinedAliases(outcomes: readonly ReadonlyMap<string, Provenance>[]): Map<string, Provenance> {
  const joined = new Map<string, Provenance>()
  const names = new Set(outcomes.flatMap((outcome) => [...outcome.keys()]))
  for (const name of names) {
    const sources = new Set(outcomes.map((outcome) => outcome.get(name) ?? 'unknown'))
    joined.set(name, sources.size === 1 ? ([...sources][0] as Provenance) : 'unknown')
  }
  return joined
}

function sameAliases(left: ReadonlyMap<string, Provenance>, right: ReadonlyMap<string, Provenance>): boolean {
  return left.size === right.size && [...left].every(([name, source]) => right.get(name) === source)
}

function latticePassBound(statements: readonly LuaStatement[]): number {
  let identifiers = 0
  for (const statement of statements) {
    if (statement.kind === 'simple')
      identifiers += statement.tokens.filter((token) => token.kind === 'identifier').length
    else if (statement.kind === 'function' || statement.kind === 'loop' || statement.kind === 'do') {
      identifiers += latticePassBound(statement.body)
    } else {
      for (const branch of statement.branches) identifiers += latticePassBound(branch.body)
      if (statement.elseBody !== undefined) identifiers += latticePassBound(statement.elseBody)
    }
  }
  // Each finite alias slot can only retain its source or rise once to `unknown` under join.
  return identifiers + 2
}

function mergeAliases(target: Map<string, Provenance>, outcomes: readonly ReadonlyMap<string, Provenance>[]): void {
  const joined = joinedAliases(outcomes)
  target.clear()
  for (const [name, source] of joined) target.set(name, source)
}

function assertEveryRedisKeyComesFromKeys(lua: string): void {
  const functions = new Map<string, LuaFunction>()
  let redisCallCount = 0

  function assertDataOnlyExpression(
    tokens: readonly LuaToken[],
    aliases: ReadonlyMap<string, Provenance>,
    allowUnknown = false,
  ): void {
    for (let index = 0; index < tokens.length; index++) {
      const token = tokens[index] as LuaToken
      if (token.kind !== 'identifier' || LUA_KEYWORDS.has(token.value)) continue
      if (tokens[index - 1]?.value === '.') continue
      const isTableField =
        tokens[index + 1]?.value === '=' && (tokens[index - 1]?.value === '{' || tokens[index - 1]?.value === ',')
      if (isTableField) continue

      const names = [token.value]
      let end = index + 1
      while (tokens[end]?.value === '.' && tokens[end + 1]?.kind === 'identifier') {
        names.push(tokens[end + 1]?.value as string)
        end += 2
      }
      const reference = names.join('.')
      const isCall = tokens[end]?.value === '('
      const rootSource = aliases.get(names[0] as string)
      if (isCall) {
        if (reference === 'cjson.encode') index = findCallClose(tokens, end)
        else index = end - 1
        continue
      }
      if (PROTECTED_CALL_BINDINGS.has(reference) || PROTECTED_CALL_BINDINGS.has(names[0] as string)) {
        throw new Error(`protected or callable value '${reference}' is not data-only`)
      }
      if (functions.has(reference) || functions.has(names[0] as string)) {
        throw new Error(`local function value '${reference}' is not data-only`)
      }
      if (isDataRecord(rootSource ?? 'unknown') && names.length === 1) {
        throw new Error(`local data record '${reference}' cannot escape as an assigned value`)
      }
      if (!allowUnknown && rootSource === 'unknown') throw new Error(`unknown value '${reference}' is not data-only`)
      if (!allowUnknown && rootSource === undefined && names[0] !== 'ARGV' && names[0] !== 'KEYS') {
        throw new Error(`unknown value '${reference}' is not data-only`)
      }
      index = end - 1
    }
  }

  function valueSourceOf(tokens: readonly LuaToken[], aliases: ReadonlyMap<string, Provenance>): Provenance {
    const directSource = sourceOf(tokens, aliases)
    if (directSource !== 'unknown') return directSource
    try {
      assertDataOnlyExpression(tokens, aliases)
      return 'data'
    } catch {
      return 'unknown'
    }
  }

  function statementsReferenceBinding(statements: readonly LuaStatement[], binding: string): boolean {
    for (const statement of statements) {
      if (
        statement.kind === 'simple' &&
        statement.tokens.some((token) => token.kind === 'identifier' && token.value === binding)
      ) {
        return true
      }
      if (statement.kind === 'function' || statement.kind === 'loop' || statement.kind === 'do') {
        if (statementsReferenceBinding(statement.body, binding)) return true
      } else if (statement.kind === 'if') {
        for (const branch of statement.branches) {
          if (
            branch.condition.some((token) => token.kind === 'identifier' && token.value === binding) ||
            statementsReferenceBinding(branch.body, binding)
          ) {
            return true
          }
        }
        if (statement.elseBody !== undefined && statementsReferenceBinding(statement.elseBody, binding)) return true
      }
    }
    return false
  }

  function visitCalls(tokens: readonly LuaToken[], aliases: Map<string, Provenance>, stack: readonly string[]): void {
    for (let index = 0; index < tokens.length; index++) {
      if (tokens[index]?.value === '(' && [')', ']', '}'].includes(tokens[index - 1]?.value ?? '')) {
        throw new Error(`unsupported indirect Lua call near '${renderTokens(tokens.slice(Math.max(0, index - 3)))}'`)
      }
      if (tokens[index]?.kind !== 'identifier') continue
      if (LUA_KEYWORDS.has(tokens[index]?.value as string)) continue
      if (tokens[index - 1]?.value === ':') {
        throw new Error(`unsupported Lua method call '${tokens[index]?.value as string}'`)
      }
      if (tokens[index + 1]?.kind === 'string' || tokens[index + 1]?.value === '{') {
        throw new Error(`unsupported parenthesis-free Lua call '${tokens[index]?.value as string}'`)
      }
      let callee = tokens[index]?.value as string
      let open = index + 1
      while (tokens[open]?.value === '.' && tokens[open + 1]?.kind === 'identifier') {
        callee += `.${tokens[open + 1]?.value as string}`
        open += 2
      }
      if (tokens[open]?.value !== '(') continue
      const close = findCallClose(tokens, open)
      const args = splitTokens(tokens.slice(open + 1, close), ',')
      for (const argument of args) {
        const argumentSource = sourceOf(argument, aliases)
        if (isDataRecord(argumentSource) && callee !== 'cjson.encode') {
          throw new Error(`local data record passed to non-encoding callee '${callee}'`)
        }
      }
      for (const argument of args) visitCalls(argument, aliases, stack)

      if (callee === 'redis.call') {
        redisCallCount++
        const command = args[0]?.length === 1 && args[0]?.[0]?.kind === 'string' ? args[0][0].value : undefined
        if (command === undefined) throw new Error('dynamic redis.call command cannot be certified')
        if (command === 'TIME' && args.length !== 1) throw new Error('TIME unexpectedly has operands')
        if (command === 'DEL' && args.length < 2) throw new Error('DEL has no key operand')
        const keyIndexes = command === 'TIME' ? [] : command === 'DEL' ? args.slice(1).map((_, i) => i + 1) : [1]
        if (command !== 'TIME' && command !== 'DEL' && !SINGLE_KEY_COMMANDS.has(command)) {
          throw new Error(`unclassified Redis key signature for ${command}`)
        }
        for (const keyIndex of keyIndexes) {
          const key = args[keyIndex] ?? []
          const provenance = sourceOf(key, aliases)
          if (provenance !== 'KEYS') {
            throw new Error(`${command} key operand '${renderTokens(key)}' comes from ${provenance}`)
          }
        }
      } else {
        const fn = functions.get(callee)
        if (fn !== undefined) {
          if (stack.includes(callee)) throw new Error(`recursive Lua function '${callee}' cannot be certified`)
          // Only parameter provenance crosses a function boundary. Treating captured or global values as
          // unknown is deliberately conservative: a script must thread a Redis key through a certified call.
          const localAliases = new Map<string, Provenance>()
          for (let parameter = 0; parameter < fn.parameters.length; parameter++) {
            localAliases.set(fn.parameters[parameter] as string, valueSourceOf(args[parameter] ?? [], aliases))
          }
          analyzeBlock(fn.body, localAliases, [...stack, callee], false, false)
        } else if (!SAFE_NON_KEY_CALLS.has(callee)) {
          throw new Error(`unsupported Lua callee '${callee}' cannot be certified`)
        }
      }
      index = close
    }
  }

  function analyzeSimple(
    tokens: readonly LuaToken[],
    aliases: Map<string, Provenance>,
    stack: readonly string[],
  ): readonly string[] {
    if (tokens.some((token) => token.value === 'function' || token.value === '...')) {
      throw new Error('unsupported Lua expression syntax cannot be certified')
    }
    const assignment = topLevelTokenIndex(tokens, '=')
    if (assignment < 0) {
      if (tokens[0]?.value === 'local') {
        const names = splitTokens(tokens.slice(1), ',').map((part) =>
          part.length === 1 && part[0]?.kind === 'identifier' ? part[0].value : undefined,
        )
        if (names.some((name) => name === undefined)) throw new Error('unsupported Lua local declaration')
        for (const name of names as string[]) {
          if (PROTECTED_CALL_BINDINGS.has(name)) {
            throw new Error(`assignment to certified callee '${name}' is unsupported`)
          }
          aliases.set(name, 'unknown')
        }
        return names as string[]
      }
      if (tokens[0]?.value === 'return') {
        const returnedValue = tokens.slice(1)
        const returnedSource = sourceOf(returnedValue, aliases)
        if (isDataRecord(returnedSource)) {
          for (const [name, source] of aliases) {
            if (source === returnedSource) aliases.set(name, 'unknown')
          }
        } else assertDataOnlyExpression(returnedValue, aliases, true)
      }
      visitCalls(tokens, aliases, stack)
      return []
    }
    const isLocal = tokens[0]?.value === 'local'
    const left = tokens.slice(isLocal ? 1 : 0, assignment)
    const targets = splitTokens(left, ',')
    const names = targets.map((part) =>
      part.length === 1 && part[0]?.kind === 'identifier' ? part[0].value : undefined,
    )
    if (names.some((name) => name === undefined)) {
      if (isLocal || targets.length !== 1) {
        throw new Error(`unsupported mixed or declared assignment target '${renderTokens(left)}'`)
      }
      const target = directDataRecordMemberTarget(targets[0] as readonly LuaToken[])
      if (target === undefined) {
        throw new Error(`unsupported member or dynamic assignment target '${renderTokens(left)}'`)
      }
      const targetReference = `${target.base}.${target.field}`
      if (PROTECTED_CALL_BINDINGS.has(target.base) || PROTECTED_CALL_BINDINGS.has(targetReference)) {
        throw new Error(`assignment to certified callee '${targetReference}' is unsupported`)
      }
      const baseSource = aliases.get(target.base) ?? 'unknown'
      if (!isDataRecord(baseSource)) {
        throw new Error(
          `member assignment base '${target.base}' is not a proven private local data record (${baseSource})`,
        )
      }
      for (const value of splitTokens(tokens.slice(assignment + 1), ',')) {
        visitCalls(value, aliases, stack)
        assertDataOnlyExpression(value, aliases)
      }
      return []
    }
    const values = splitTokens(tokens.slice(assignment + 1), ',')
    for (let index = 0; index < names.length; index++) {
      const name = names[index] as string
      if (!isLocal && !aliases.has(name)) {
        throw new Error(`global assignment '${name}' cannot be certified`)
      }
      if (PROTECTED_CALL_BINDINGS.has(name)) {
        throw new Error(`assignment to certified callee '${name}' is unsupported`)
      }
      if (referenceOf(values[index] ?? []) === 'redis.call') {
        throw new Error(`redis.call alias '${name}' cannot be certified`)
      }
      const assignedReference = referenceOf(values[index] ?? [])
      if (assignedReference !== undefined && PROTECTED_CALL_BINDINGS.has(assignedReference)) {
        throw new Error(`certified callee alias '${name}' cannot reference '${assignedReference}'`)
      }
      if (assignedReference !== undefined && functions.has(assignedReference)) {
        throw new Error(`local function alias '${name}' cannot be certified`)
      }
    }
    for (const value of values) visitCalls(value, aliases, stack)
    const sources = values.map((value) => valueSourceOf(value, aliases))
    for (let index = 0; index < sources.length; index++) {
      const source = sources[index] as Provenance
      if (!isDataRecord(source)) continue
      const value = values[index] ?? []
      if (isLocal && directDataRecordSource(value) !== undefined) {
        assertDataOnlyExpression(value, aliases)
        continue
      }
      for (const [alias, aliasSource] of aliases) {
        if (aliasSource === source) aliases.set(alias, 'unknown')
      }
      sources[index] = 'unknown'
    }
    for (let index = 0; index < names.length; index++) {
      const source = sources[index] ?? 'unknown'
      aliases.set(names[index] as string, !isLocal && isDataRecord(source) ? 'unknown' : source)
    }
    return isLocal ? (names as string[]) : []
  }

  function analyzeBlock(
    statements: readonly LuaStatement[],
    aliases: Map<string, Provenance>,
    stack: readonly string[],
    restoreLocals = false,
    allowFunctionDefinitions = true,
  ): void {
    const incoming = new Map(aliases)
    const locals = new Set<string>()
    for (const statement of statements) {
      if (statement.kind === 'simple') {
        for (const name of analyzeSimple(statement.tokens, aliases, stack)) locals.add(name)
      } else if (statement.kind === 'function') {
        if (!allowFunctionDefinitions) {
          throw new Error(`nested or conditional Lua function '${statement.name}' cannot be certified`)
        }
        if (!statement.local) throw new Error(`global Lua function '${statement.name}' cannot be certified`)
        if (PROTECTED_CALL_BINDINGS.has(statement.name)) {
          throw new Error(`assignment to certified callee '${statement.name}' is unsupported`)
        }
        for (const parameter of statement.parameters) {
          if (PROTECTED_CALL_BINDINGS.has(parameter)) {
            throw new Error(`local function parameter '${parameter}' shadows a certified callee`)
          }
        }
        for (const [name, source] of aliases) {
          if (isDataRecord(source) && statementsReferenceBinding(statement.body, name)) {
            throw new Error(`local data record '${name}' is captured by function '${statement.name}'`)
          }
        }
        functions.set(statement.name, {
          parameters: statement.parameters,
          body: statement.body,
        })
      } else if (statement.kind === 'if') {
        const outcomes: Map<string, Provenance>[] = []
        for (const branch of statement.branches) {
          visitCalls(branch.condition, aliases, stack)
          const branchAliases = new Map(aliases)
          analyzeBlock(branch.body, branchAliases, stack, true, false)
          outcomes.push(branchAliases)
        }
        if (statement.elseBody === undefined) outcomes.push(new Map(aliases))
        else {
          const elseAliases = new Map(aliases)
          analyzeBlock(statement.elseBody, elseAliases, stack, true, false)
          outcomes.push(elseAliases)
        }
        mergeAliases(aliases, outcomes)
      } else if (statement.kind === 'loop') {
        visitCalls(statement.header, aliases, stack)
        const outsideAliases = new Map(aliases)
        let loopEntry = new Map(aliases)
        const loopBindings: string[] = []
        const bindingEnd = statement.header.findIndex((token) => token.value === '=' || token.value === 'in')
        if (statement.loopKind === 'for' && bindingEnd < 1) throw new Error('unsupported Lua for-loop binding')
        if (statement.loopKind === 'for') {
          for (const part of splitTokens(statement.header.slice(0, bindingEnd), ',')) {
            if (part.length !== 1 || part[0]?.kind !== 'identifier') throw new Error('unsupported Lua for-loop binding')
            if (PROTECTED_CALL_BINDINGS.has(part[0].value)) {
              throw new Error(`Lua for-loop binding '${part[0].value}' shadows a certified callee`)
            }
            loopBindings.push(part[0].value)
            loopEntry.set(part[0].value, 'unknown')
          }
        }
        let stabilized = false
        const maxPasses = aliases.size + latticePassBound(statement.body)
        for (let pass = 0; pass < maxPasses; pass++) {
          const backEdge = new Map(loopEntry)
          analyzeBlock(statement.body, backEdge, stack, true, false)
          const nextEntry = joinedAliases([loopEntry, backEdge])
          for (const name of loopBindings) nextEntry.set(name, 'unknown')
          if (sameAliases(loopEntry, nextEntry)) {
            loopEntry = nextEntry
            stabilized = true
            break
          }
          loopEntry = nextEntry
        }
        if (!stabilized) throw new Error(`Lua loop provenance did not stabilize within ${maxPasses} passes`)
        for (const name of loopBindings) {
          const before = outsideAliases.get(name)
          if (before === undefined) loopEntry.delete(name)
          else loopEntry.set(name, before)
        }
        mergeAliases(aliases, [outsideAliases, loopEntry])
      } else {
        const innerAliases = new Map(aliases)
        analyzeBlock(statement.body, innerAliases, stack, true, false)
        mergeAliases(aliases, [innerAliases])
      }
    }
    if (restoreLocals) {
      for (const name of locals) {
        const before = incoming.get(name)
        if (before === undefined) aliases.delete(name)
        else aliases.set(name, before)
      }
    }
  }

  analyzeBlock(new LuaStatementParser(lexLua(lua)).parse(), new Map(), [])
  if (redisCallCount === 0) throw new Error('Lua program has no redis.call to certify')
}

function expectOneSlot(keys: readonly string[]): void {
  expect(keys.length).toBeGreaterThan(0)
  expect(new Set(keys.map(redisSlot))).toEqual(new Set([redisSlot(keys[0] as string)]))
}

describe('Redis Room key-slot and Lua key declarations', () => {
  const prefix = 'tf:'
  const roomId = 'room} with space'
  const inc = 'generation'
  const lane = { kind: 'semantic' } as const

  it('keeps every final key helper in its escaped room or directory slot', () => {
    const roomKeys = [
      headKey(prefix, roomId),
      headRevKey(prefix, roomId),
      gensKey(prefix, roomId),
      generationTokensKey(prefix, roomId),
      routeCapturesKey(prefix, roomId),
      routeCaptureExpiriesKey(prefix, roomId),
      revKey(prefix, roomId, inc),
      cellKey(prefix, roomId, inc, 'cell} escape'),
      orderKey(prefix, roomId, inc, 'semantic'),
      retainedKey(prefix, roomId, inc, 'semantic'),
      retainedSizeKey(prefix, roomId, inc),
      channelKey(prefix, roomId, inc, 'semantic'),
      generationInvalidationChannel(prefix, roomId, inc),
    ]
    expectOneSlot(roomKeys)
    expect(roomKeys.every((key) => key.includes('{room%7D%20with%20space}'))).toBe(true)
    expectOneSlot([directoryIndexKey(prefix), directoryTagsKey(prefix)])
  })

  it('checks every runtime command descriptor and its production key builder', () => {
    const retained = retainedKey(prefix, roomId, inc, 'semantic')
    const groups = {
      headCx: REDIS_ROOM_COMMAND_KEYS.headCx(prefix, roomId),
      captureGeneration: REDIS_ROOM_COMMAND_KEYS.captureGeneration(prefix, roomId),
      validateGeneration: REDIS_ROOM_COMMAND_KEYS.validateGeneration(prefix, roomId),
      dropGenerationFinalize: REDIS_ROOM_COMMAND_KEYS.dropGenerationFinalize(prefix, roomId),
      cellsCx: REDIS_ROOM_COMMAND_KEYS.cellsCx(prefix, roomId, inc, ['cell} escape']),
      commit: REDIS_ROOM_COMMAND_KEYS.commit(prefix, roomId, inc, lane),
      retainedDelete: REDIS_ROOM_COMMAND_KEYS.retainedDelete(prefix, roomId, inc, [retained]),
      directoryPut: REDIS_ROOM_COMMAND_KEYS.directoryPut(prefix),
      directoryDelete: REDIS_ROOM_COMMAND_KEYS.directoryDelete(prefix),
    }
    expect(Object.keys(REDIS_ROOM_COMMANDS)).toEqual(Object.keys(groups))
    expect(Object.keys(REDIS_ROOM_COMMANDS)).toHaveLength(9)
    for (const [id, descriptor] of Object.entries(REDIS_ROOM_COMMANDS)) {
      const keys = groups[id as keyof typeof groups]
      expectOneSlot(keys)
      if (descriptor.numberOfKeys !== null) expect(keys).toHaveLength(descriptor.numberOfKeys)
    }
  })

  it('traces every Lua Redis key operand back to KEYS, including aliases', () => {
    expect(Object.keys(REDIS_ROOM_COMMANDS)).toHaveLength(9)
    for (const descriptor of Object.values(REDIS_ROOM_COMMANDS)) {
      assertEveryRedisKeyComesFromKeys(descriptor.lua)
    }
  })

  it('snapshots alias provenance when each Redis key operand executes', () => {
    const insertionPoint = "redis.call('ZADD', KEYS[1], 0, ARGV[1])"
    const maskingMutation = DIRECTORY_PUT_LUA.replace(
      insertionPoint,
      `local target = ARGV[1]
redis.call('GET', target)
target = KEYS[1]
${insertionPoint}`,
    )
    expect(maskingMutation).not.toBe(DIRECTORY_PUT_LUA)
    expect(() => assertEveryRedisKeyComesFromKeys(maskingMutation)).toThrow("GET key operand 'target' comes from ARGV")
  })

  it('joins loop entry and back-edge provenance before validating later iterations', () => {
    const insertionPoint = "redis.call('ZADD', KEYS[1], 0, ARGV[1])"
    const loopMutation = DIRECTORY_PUT_LUA.replace(
      insertionPoint,
      `local target = KEYS[2]
for i = 1, 2 do
  redis.call('HGET', target, ARGV[1])
  target = ARGV[1]
end
${insertionPoint}`,
    )
    expect(loopMutation).not.toBe(DIRECTORY_PUT_LUA)
    expect(() => assertEveryRedisKeyComesFromKeys(loopMutation)).toThrow("HGET key operand 'target' comes from unknown")
  })

  it('rejects aliases of redis.call before an indirect key access can be hidden', () => {
    const insertionPoint = "redis.call('ZADD', KEYS[1], 0, ARGV[1])"
    const indirectMutation = DIRECTORY_PUT_LUA.replace(
      insertionPoint,
      `local invoke = redis.call
invoke('HGET', ARGV[1], ARGV[1])
${insertionPoint}`,
    )
    expect(indirectMutation).not.toBe(DIRECTORY_PUT_LUA)
    expect(() => assertEveryRedisKeyComesFromKeys(indirectMutation)).toThrow(
      "redis.call alias 'invoke' cannot be certified",
    )
  })

  it('covers zero-, one-, and many-iteration loop joins', () => {
    const zeroOrMany = `
local target = KEYS[1]
while ARGV[2] ~= '' do
  target = ARGV[1]
end
redis.call('GET', target)
`
    expect(() => assertEveryRedisKeyComesFromKeys(zeroOrMany)).toThrow("GET key operand 'target' comes from unknown")

    const stableOneOrMany = `
local target = KEYS[1]
for i = 1, 1 do
  redis.call('GET', target)
  target = KEYS[2]
end
redis.call('GET', target)
`
    expect(() => assertEveryRedisKeyComesFromKeys(stableOneOrMany)).not.toThrow()

    const unsafeMany = `
local target = KEYS[1]
for i = 1, 2 do
  redis.call('GET', target)
  target = ARGV[1]
end
`
    expect(() => assertEveryRedisKeyComesFromKeys(unsafeMany)).toThrow("GET key operand 'target' comes from unknown")
  })

  it('keeps nested loop and branch locals scoped while joining outer assignments', () => {
    const scoped = `
local target = KEYS[1]
for i = 1, 2 do
  if ARGV[1] ~= '' then
    local target = KEYS[2]
    for j = 1, 2 do
      redis.call('HGET', target, ARGV[1])
    end
  end
end
redis.call('GET', target)
`
    expect(() => assertEveryRedisKeyComesFromKeys(scoped)).not.toThrow()

    const outerAssignment = `
local target = KEYS[1]
for i = 1, 2 do
  if ARGV[1] ~= '' then target = ARGV[1] end
  redis.call('GET', target)
end
`
    expect(() => assertEveryRedisKeyComesFromKeys(outerAssignment)).toThrow(
      "GET key operand 'target' comes from unknown",
    )
  })

  it('analyzes local function parameters and rejects uncaptured key authority', () => {
    const safeFunction = `
local function read(key)
  return redis.call('GET', key)
end
local value = tonumber(read(KEYS[1]))
`
    expect(() => assertEveryRedisKeyComesFromKeys(safeFunction)).not.toThrow()

    const unsafeArgument = safeFunction.replace('read(KEYS[1])', 'read(ARGV[1])')
    expect(() => assertEveryRedisKeyComesFromKeys(unsafeArgument)).toThrow("GET key operand 'key' comes from ARGV")

    const unsupportedCapture = `
local captured = KEYS[1]
local function read()
  return redis.call('GET', captured)
end
read()
`
    expect(() => assertEveryRedisKeyComesFromKeys(unsupportedCapture)).toThrow(
      "GET key operand 'captured' comes from unknown",
    )

    const capturedWrite = `
local target = KEYS[1]
local function poison()
  target = ARGV[1]
end
for i = 1, 2 do
  redis.call('GET', target)
  poison()
end
`
    expect(() => assertEveryRedisKeyComesFromKeys(capturedWrite)).toThrow(
      "global assignment 'target' cannot be certified",
    )
  })

  it('allows only private local data-record member writes and data-only values', () => {
    const accepted = [
      `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local prior = cjson.decode(raw)
prior.expiresAt = tonumber(ARGV[2])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(prior))
`,
      `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local capture = cjson.decode(raw)
capture.expiresAt = tonumber(ARGV[2])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(capture))
`,
      `
local stored = { rev = ARGV[1], state = 'open' }
stored.inc = ARGV[2]
stored.lease = { id = ARGV[3], ['until'] = tonumber(ARGV[4]) }
stored.exp = tonumber(ARGV[5])
redis.call('SET', KEYS[1], cjson.encode(stored))
`,
      `
local function encodeRecord(value)
  local record = { value = value }
  record.expiresAt = tonumber(ARGV[1])
  return cjson.encode(record)
end
redis.call('SET', KEYS[1], encodeRecord(ARGV[2]))
`,
    ]
    for (const lua of accepted) expect(() => assertEveryRedisKeyComesFromKeys(lua)).not.toThrow()

    const hiddenFunction = `
local function hidden(key)
  return redis.call('HGET', key, ARGV[1])
end
`
    const directoryPutInsertion = "redis.call('ZADD', KEYS[1], 0, ARGV[1])"
    const decodedRecord = `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local record = cjson.decode(raw)
`
    const rejected = [
      {
        name: 'protected dotted namespace',
        lua: `${hiddenFunction}\nmath.floor = hidden\nmath.floor(ARGV[1])`,
        message: "assignment to certified callee 'math.floor' is unsupported",
      },
      {
        name: 'exact bracket-hidden function escape',
        lua: DIRECTORY_PUT_LUA.replace(
          directoryPutInsertion,
          `${hiddenFunction}\nmath['floor'] = hidden\nmath.floor(ARGV[1])\n${directoryPutInsertion}`,
        ),
        message: "unsupported member or dynamic assignment target 'math [ 'floor' ]'",
      },
      {
        name: 'decoded-record bracket literal',
        lua: `${decodedRecord}\nrecord['expiresAt'] = ARGV[2]`,
        message: 'unsupported member or dynamic assignment target',
      },
      {
        name: 'decoded-record bracket dynamic',
        lua: `${decodedRecord}\nlocal field = ARGV[2]\nrecord[field] = ARGV[3]`,
        message: 'unsupported member or dynamic assignment target',
      },
      {
        name: 'decoded-record numeric index',
        lua: `${decodedRecord}\nrecord[1] = ARGV[2]`,
        message: 'unsupported member or dynamic assignment target',
      },
      {
        name: 'decoded-record nested member',
        lua: `${decodedRecord}\nrecord.meta.expiresAt = ARGV[2]`,
        message: 'unsupported member or dynamic assignment target',
      },
      {
        name: 'global member',
        lua: "global.expiresAt = ARGV[1]\nredis.call('GET', KEYS[1])",
        message: "member assignment base 'global' is not a proven private local data record",
      },
      {
        name: 'plain global assignment',
        lua: "global = ARGV[1]\nredis.call('GET', KEYS[1])",
        message: "global assignment 'global' cannot be certified",
      },
      {
        name: 'captured record member',
        lua: `${decodedRecord}\nlocal function mutate()\n  record.expiresAt = ARGV[2]\nend\nmutate()`,
        message: "local data record 'record' is captured by function 'mutate'",
      },
      {
        name: 'unknown local member',
        lua: "local record = tonumber(ARGV[1])\nrecord.expiresAt = ARGV[1]\nredis.call('GET', KEYS[1])",
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'callable member',
        lua: `${hiddenFunction}\nhidden.expiresAt = ARGV[1]\nhidden(KEYS[1])`,
        message: "member assignment base 'hidden' is not a proven private local data record",
      },
      {
        name: 'callable field injection',
        lua: `${hiddenFunction}\n${decodedRecord}\nrecord.callback = hidden`,
        message: "local function value 'hidden' is not data-only",
      },
      {
        name: 'redis.call field injection',
        lua: `${decodedRecord}\nrecord.callback = redis.call`,
        message: "protected or callable value 'redis.call' is not data-only",
      },
      {
        name: 'protected field injection',
        lua: `${decodedRecord}\nrecord.callback = math.floor`,
        message: "protected or callable value 'math.floor' is not data-only",
      },
      {
        name: 'unknown effectful field value',
        lua: `${decodedRecord}\nrecord.expiresAt = mystery()`,
        message: "unsupported Lua callee 'mystery' cannot be certified",
      },
      {
        name: 'unknown bare field value',
        lua: `${decodedRecord}\nlocal mysteryValue = globalValue\nrecord.expiresAt = mysteryValue`,
        message: "unknown value 'mysteryValue' is not data-only",
      },
      {
        name: 'mixed member and identifier targets',
        lua: `${decodedRecord}\nrecord.expiresAt, other = ARGV[2], ARGV[3]`,
        message: 'unsupported mixed or declared assignment target',
      },
      {
        name: 'decoded-record alias target',
        lua: `${decodedRecord}\nlocal alias = record\nalias.expiresAt = ARGV[2]`,
        message: "member assignment base 'alias' is not a proven private local data record",
      },
      {
        name: 'decoded-record escape invalidates its source',
        lua: `${decodedRecord}\nlocal alias = record\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'decoded-record branch rebind',
        lua: `${decodedRecord}\nif ARGV[2] ~= '' then\n  record = cjson.decode(raw)\nend\nrecord.expiresAt = ARGV[3]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'decoded-record direct rebind',
        lua: `${decodedRecord}\nrecord = cjson.decode(raw)\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'fresh-record alias invalidates its source',
        lua: "local record = {}\nlocal alias = record\nrecord.expiresAt = ARGV[1]\nredis.call('GET', KEYS[1])",
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'record passed to analyzed local function',
        lua: `${decodedRecord}\nlocal function consume(value)\n  return tostring(value)\nend\nconsume(record)`,
        message: "local data record passed to non-encoding callee 'consume'",
      },
      {
        name: 'returned record invalidates later writes',
        lua: `${decodedRecord}\nreturn record\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'record stored into a global object',
        lua: `${decodedRecord}\nglobal.value = record`,
        message: "member assignment base 'global' is not a proven private local data record",
      },
      {
        name: 'fresh record initialized with callable data',
        lua: `${hiddenFunction}\nlocal record = { callback = hidden }\nrecord.expiresAt = ARGV[1]`,
        message: "local function value 'hidden' is not data-only",
      },
      {
        name: 'local function cannot return a callable field value',
        lua: `${hiddenFunction}\nlocal function reveal()\n  return hidden\nend\n${decodedRecord}\nrecord.callback = reveal()`,
        message: "local function value 'hidden' is not data-only",
      },
      {
        name: 'record joined with scalar provenance',
        lua: `${decodedRecord}\nif ARGV[2] ~= '' then\n  record = tonumber(ARGV[2])\nend\nrecord.expiresAt = ARGV[3]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
    ]
    for (const { name, lua, message } of rejected) {
      expect(() => assertEveryRedisKeyComesFromKeys(lua), name).toThrow(message)
    }
  })

  it('fails closed on unknown callees and unsupported call or control syntax', () => {
    expect(() => assertEveryRedisKeyComesFromKeys("mystery('GET', KEYS[1])")).toThrow(
      "unsupported Lua callee 'mystery' cannot be certified",
    )
    expect(() => assertEveryRedisKeyComesFromKeys("redis.pcall('GET', KEYS[1])")).toThrow(
      "unsupported Lua callee 'redis.pcall' cannot be certified",
    )
    expect(() => assertEveryRedisKeyComesFromKeys("(redis.call)('GET', KEYS[1])")).toThrow(
      'unsupported indirect Lua call',
    )
    expect(() => assertEveryRedisKeyComesFromKeys("print 'GET'")).toThrow(
      "unsupported parenthesis-free Lua call 'print'",
    )
    expect(() => assertEveryRedisKeyComesFromKeys("client:read('GET', KEYS[1])")).toThrow(
      "unsupported Lua method call 'read'",
    )
    expect(() => assertEveryRedisKeyComesFromKeys("local redis\nredis.call('GET', KEYS[1])")).toThrow(
      "assignment to certified callee 'redis' is unsupported",
    )
    expect(() => assertEveryRedisKeyComesFromKeys('invoke [[GET]]')).toThrow('unsupported Lua long-string syntax')
    expect(() =>
      assertEveryRedisKeyComesFromKeys("if true then local function read() redis.call('GET', KEYS[1]) end end"),
    ).toThrow("nested or conditional Lua function 'read' cannot be certified")
    expect(() => assertEveryRedisKeyComesFromKeys("function read() redis.call('GET', KEYS[1]) end\nread()")).toThrow(
      "global Lua function 'read' cannot be certified",
    )
    expect(() => assertEveryRedisKeyComesFromKeys("repeat redis.call('GET', KEYS[1]) until true")).toThrow(
      "unsupported Lua control syntax 'repeat'",
    )
  })
})
