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

// Redis Cluster's CRC16/XMODEM key-slot algorithm. This keeps the one-room-slot structural claim
// executable alongside the separate W4-R real-Cluster behavior certification.
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

type DataRecordIdentity = `data-record:${number}`
type Provenance = 'KEYS' | 'ARGV' | 'data' | 'unknown' | DataRecordIdentity

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

type LuaFunction = {
  parameters: readonly string[]
  body: readonly LuaStatement[]
}

type LuaBinding = { kind: 'function'; definition: LuaFunction } | { kind: 'value'; provenance: Provenance }

type LuaBindings = Map<string, LuaBinding>
type ReadonlyLuaBindings = ReadonlyMap<string, LuaBinding>

type LuaAnalysisState = {
  bindings: LuaBindings
  invalidatedRecords: Set<DataRecordIdentity>
}

type ReadonlyLuaAnalysisState = {
  bindings: ReadonlyLuaBindings
  invalidatedRecords: ReadonlySet<DataRecordIdentity>
}

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

function valueProvenance(state: ReadonlyLuaAnalysisState, name: string): Provenance | undefined {
  const binding = state.bindings.get(name)
  if (binding?.kind === 'function') return 'unknown'
  if (binding?.kind !== 'value') return undefined
  return isDataRecord(binding.provenance) && state.invalidatedRecords.has(binding.provenance)
    ? 'unknown'
    : binding.provenance
}

function localFunction(state: ReadonlyLuaAnalysisState, name: string): LuaFunction | undefined {
  const binding = state.bindings.get(name)
  return binding?.kind === 'function' ? binding.definition : undefined
}

function valueBinding(provenance: Provenance): LuaBinding {
  return { kind: 'value', provenance }
}

function sourceOf(tokens: readonly LuaToken[], state: ReadonlyLuaAnalysisState): Provenance {
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
    return valueProvenance(state, expression[0].value) ?? 'unknown'
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

type LuaExpressionAst =
  | { kind: 'identifier'; path: readonly string[]; role: 'field-label' | 'reference' }
  | { kind: 'group'; delimiter: '(' | '[' | '{'; children: readonly LuaExpressionAst[] }
  | { kind: 'token'; token: LuaToken }

function expressionAst(tokens: readonly LuaToken[], delimiter?: '(' | '[' | '{'): readonly LuaExpressionAst[] {
  const nodes: LuaExpressionAst[] = []
  let tableFieldStart = delimiter === '{'
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index] as LuaToken
    const close = OPEN.get(token.value)
    if (close !== undefined) {
      const end = groupClose(tokens, index, close)
      nodes.push({
        kind: 'group',
        delimiter: token.value as '(' | '[' | '{',
        children: expressionAst(tokens.slice(index + 1, end), token.value as '(' | '[' | '{'),
      })
      index = end
      tableFieldStart = false
      continue
    }
    if (token.kind === 'identifier') {
      const path = [token.value]
      let end = index + 1
      while (tokens[end]?.value === '.' && tokens[end + 1]?.kind === 'identifier') {
        path.push(tokens[end + 1]?.value as string)
        end += 2
      }
      nodes.push({
        kind: 'identifier',
        path,
        role: delimiter === '{' && tableFieldStart && tokens[end]?.value === '=' ? 'field-label' : 'reference',
      })
      index = end - 1
      tableFieldStart = false
      continue
    }
    nodes.push({ kind: 'token', token })
    if (delimiter === '{' && (token.value === ',' || token.value === ';')) tableFieldStart = true
    else if (delimiter === '{' && token.value !== '=') tableFieldStart = false
  }
  return nodes
}

function groupClose(tokens: readonly LuaToken[], start: number, expected: string): number {
  const closers = [expected]
  for (let index = start + 1; index < tokens.length; index++) {
    const token = tokens[index] as LuaToken
    const close = OPEN.get(token.value)
    if (close !== undefined) closers.push(close)
    else if (closers.at(-1) === token.value) {
      closers.pop()
      if (closers.length === 0) return index
    } else if ([')', ']', '}'].includes(token.value)) {
      throw new Error(`mismatched Lua expression delimiter '${token.value}', expected '${closers.at(-1)}'`)
    }
  }
  throw new Error(`unterminated Lua expression group, expected '${expected}'`)
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
  'string.format',
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

function sameBinding(left: LuaBinding, right: LuaBinding): boolean {
  if (left.kind !== right.kind) return false
  if (left.kind === 'function') return right.kind === 'function' && left.definition === right.definition
  return right.kind === 'value' && left.provenance === right.provenance
}

function joinedBindings(outcomes: readonly ReadonlyLuaBindings[]): LuaBindings {
  const joined: LuaBindings = new Map()
  const names = new Set(outcomes.flatMap((outcome) => [...outcome.keys()]))
  for (const name of names) {
    const candidates = outcomes.map((outcome) => outcome.get(name) ?? valueBinding('unknown'))
    const first = candidates[0] as LuaBinding
    joined.set(name, candidates.every((candidate) => sameBinding(first, candidate)) ? first : valueBinding('unknown'))
  }
  return joined
}

function sameBindings(left: ReadonlyLuaBindings, right: ReadonlyLuaBindings): boolean {
  return (
    left.size === right.size &&
    [...left].every(([name, binding]) => {
      const other = right.get(name)
      return other !== undefined && sameBinding(binding, other)
    })
  )
}

function cloneState(state: ReadonlyLuaAnalysisState): LuaAnalysisState {
  return {
    bindings: new Map(state.bindings),
    invalidatedRecords: new Set(state.invalidatedRecords),
  }
}

function joinedState(outcomes: readonly ReadonlyLuaAnalysisState[]): LuaAnalysisState {
  return {
    bindings: joinedBindings(outcomes.map((outcome) => outcome.bindings)),
    invalidatedRecords: new Set(outcomes.flatMap((outcome) => [...outcome.invalidatedRecords])),
  }
}

function sameState(left: ReadonlyLuaAnalysisState, right: ReadonlyLuaAnalysisState): boolean {
  return (
    sameBindings(left.bindings, right.bindings) &&
    left.invalidatedRecords.size === right.invalidatedRecords.size &&
    [...left.invalidatedRecords].every((identity) => right.invalidatedRecords.has(identity))
  )
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
  // Each finite binding can rise once to `unknown`, and each syntactic record identity can invalidate once.
  return identifiers + 2
}

function mergeState(target: LuaAnalysisState, outcomes: readonly ReadonlyLuaAnalysisState[]): void {
  const joined = joinedState(outcomes)
  target.bindings.clear()
  for (const [name, binding] of joined.bindings) target.bindings.set(name, binding)
  target.invalidatedRecords.clear()
  for (const identity of joined.invalidatedRecords) target.invalidatedRecords.add(identity)
}

function assertEveryRedisKeyComesFromKeys(lua: string): void {
  let redisCallCount = 0

  function assertDataOnlyExpression(
    tokens: readonly LuaToken[],
    state: ReadonlyLuaAnalysisState,
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
      const rootBinding = state.bindings.get(names[0] as string)
      const rootSource = valueProvenance(state, names[0] as string)
      if (isCall) {
        if (reference === 'cjson.encode') index = findCallClose(tokens, end)
        else index = end - 1
        continue
      }
      if (PROTECTED_CALL_BINDINGS.has(reference) || PROTECTED_CALL_BINDINGS.has(names[0] as string)) {
        throw new Error(`protected or callable value '${reference}' is not data-only`)
      }
      if (localFunction(state, reference) !== undefined || rootBinding?.kind === 'function') {
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

  function valueSourceOf(tokens: readonly LuaToken[], state: ReadonlyLuaAnalysisState): Provenance {
    const directSource = sourceOf(tokens, state)
    if (directSource !== 'unknown') return directSource
    try {
      assertDataOnlyExpression(tokens, state)
      return 'data'
    } catch {
      return 'unknown'
    }
  }

  type PrivateRecordReference = {
    fieldRead: boolean
    value: boolean
  }

  function walkPrivateRecordReferences(
    tokens: readonly LuaToken[],
    state: ReadonlyLuaAnalysisState,
    exemptExactCjsonEncode: boolean,
  ): Map<`data-record:${number}`, PrivateRecordReference> {
    const references = new Map<`data-record:${number}`, PrivateRecordReference>()

    function recordUse(identity: `data-record:${number}`, fieldRead: boolean): void {
      const current = references.get(identity) ?? { fieldRead: false, value: false }
      if (fieldRead) current.fieldRead = true
      else current.value = true
      references.set(identity, current)
    }

    function exactEncodedIdentity(
      group: Extract<LuaExpressionAst, { kind: 'group' }>,
    ): `data-record:${number}` | undefined {
      if (group.children.length !== 1) return undefined
      const argument = group.children[0]
      if (argument?.kind !== 'identifier' || argument.role !== 'reference' || argument.path.length !== 1) {
        return undefined
      }
      const source = valueProvenance(state, argument.path[0] as string) ?? 'unknown'
      return isDataRecord(source) ? source : undefined
    }

    function walk(nodes: readonly LuaExpressionAst[]): void {
      for (let index = 0; index < nodes.length; index++) {
        const node = nodes[index] as LuaExpressionAst
        if (node.kind === 'identifier') {
          if (node.role === 'field-label') continue
          const next = nodes[index + 1]
          if (
            exemptExactCjsonEncode &&
            node.path.join('.') === 'cjson.encode' &&
            next?.kind === 'group' &&
            next.delimiter === '(' &&
            exactEncodedIdentity(next) !== undefined
          ) {
            index++
            continue
          }
          const source = valueProvenance(state, node.path[0] as string) ?? 'unknown'
          if (isDataRecord(source)) recordUse(source, node.path.length > 1)
        } else if (node.kind === 'group') walk(node.children)
      }
    }

    walk(expressionAst(tokens.filter((token) => token.kind !== 'newline')))
    return references
  }

  function escapingRecordIdentities(
    tokens: readonly LuaToken[],
    state: ReadonlyLuaAnalysisState,
    exemptExactCjsonEncode = true,
  ): Set<`data-record:${number}`> {
    return new Set(
      [...walkPrivateRecordReferences(tokens, state, exemptExactCjsonEncode)]
        .filter(([, reference]) => reference.value)
        .map(([identity]) => identity),
    )
  }

  function exactPrivateRecordValue(
    tokens: readonly LuaToken[],
    state: ReadonlyLuaAnalysisState,
  ): `data-record:${number}` | undefined {
    const expression = tokens.filter((token) => token.kind !== 'newline')
    if (expression.length !== 1 || expression[0]?.kind !== 'identifier') return undefined
    const source = valueProvenance(state, expression[0].value) ?? 'unknown'
    return isDataRecord(source) ? source : undefined
  }

  function invalidateRecordIdentities(state: LuaAnalysisState, identities: ReadonlySet<DataRecordIdentity>): void {
    for (const identity of identities) state.invalidatedRecords.add(identity)
  }

  function installBinding(
    state: LuaAnalysisState,
    name: string,
    binding: LuaBinding,
    mode: 'declare' | 'rebind',
  ): void {
    if (mode === 'rebind') {
      const replaced = state.bindings.get(name)
      if (replaced?.kind === 'value' && isDataRecord(replaced.provenance)) {
        invalidateRecordIdentities(state, new Set([replaced.provenance]))
      }
    }
    state.bindings.set(name, binding)
  }

  function restoreBinding(state: LuaAnalysisState, name: string, previous: LuaBinding | undefined): void {
    if (previous === undefined) state.bindings.delete(name)
    else installBinding(state, name, previous, 'declare')
  }

  type LexicalDeclaration = { readonly identity: symbol }
  type LexicalScope = Map<string, LexicalDeclaration>

  function declareLexicalBinding(scope: LexicalScope, name: string): LexicalDeclaration {
    const declaration = { identity: Symbol(name) }
    scope.set(name, declaration)
    return declaration
  }

  function tokensReferenceDeclaration(
    tokens: readonly LuaToken[],
    binding: string,
    declaration: LexicalDeclaration,
    scope: ReadonlyMap<string, LexicalDeclaration>,
  ): boolean {
    for (let index = 0; index < tokens.length; index++) {
      const token = tokens[index]
      if (token?.kind !== 'identifier' || token.value !== binding || LUA_KEYWORDS.has(token.value)) continue
      const previous = tokens.slice(0, index).findLast((candidate) => candidate.kind !== 'newline')
      const next = tokens.slice(index + 1).find((candidate) => candidate.kind !== 'newline')
      if (previous?.value === '.') continue
      if (next?.value === '=' && ['{', ',', ';'].includes(previous?.value ?? '')) {
        continue
      }
      if (scope.get(token.value) === declaration) return true
    }
    return false
  }

  function functionReferencesDeclaration(
    statement: Extract<LuaStatement, { kind: 'function' }>,
    binding: string,
    declaration: LexicalDeclaration,
    enclosingScope: ReadonlyMap<string, LexicalDeclaration>,
  ): boolean {
    const functionScope = new Map(enclosingScope)
    declareLexicalBinding(functionScope, statement.name)
    for (const parameter of statement.parameters) declareLexicalBinding(functionScope, parameter)
    return statementsReferenceDeclaration(statement.body, binding, declaration, functionScope)
  }

  function statementsReferenceDeclaration(
    statements: readonly LuaStatement[],
    binding: string,
    declaration: LexicalDeclaration,
    scope: LexicalScope,
  ): boolean {
    for (const statement of statements) {
      if (statement.kind === 'simple') {
        const assignment = topLevelTokenIndex(statement.tokens, '=')
        const isLocal = statement.tokens[0]?.value === 'local'
        if (isLocal) {
          if (assignment >= 0) {
            if (tokensReferenceDeclaration(statement.tokens.slice(assignment + 1), binding, declaration, scope)) {
              return true
            }
          }
          const declarationEnd = assignment >= 0 ? assignment : statement.tokens.length
          const names = splitTokens(statement.tokens.slice(1, declarationEnd), ',').map((part) =>
            part.length === 1 && part[0]?.kind === 'identifier' ? part[0].value : undefined,
          )
          if (names.some((name) => name === undefined)) {
            throw new Error('unsupported Lua local declaration')
          }
          for (const name of names as string[]) declareLexicalBinding(scope, name)
          continue
        }
        if (tokensReferenceDeclaration(statement.tokens, binding, declaration, scope)) return true
        continue
      }

      if (statement.kind === 'function') {
        if (functionReferencesDeclaration(statement, binding, declaration, scope)) return true
        declareLexicalBinding(scope, statement.name)
        continue
      }

      if (statement.kind === 'if') {
        for (const branch of statement.branches) {
          if (tokensReferenceDeclaration(branch.condition, binding, declaration, scope)) return true
          if (statementsReferenceDeclaration(branch.body, binding, declaration, new Map(scope))) return true
        }
        if (
          statement.elseBody !== undefined &&
          statementsReferenceDeclaration(statement.elseBody, binding, declaration, new Map(scope))
        ) {
          return true
        }
        continue
      }

      if (statement.kind === 'loop') {
        const bodyScope = new Map(scope)
        if (statement.loopKind === 'for') {
          const bindingEnd = statement.header.findIndex((token) => token.value === '=' || token.value === 'in')
          if (bindingEnd < 1) throw new Error('unsupported Lua for-loop binding')
          if (tokensReferenceDeclaration(statement.header.slice(bindingEnd + 1), binding, declaration, scope)) {
            return true
          }
          const iterators = splitTokens(statement.header.slice(0, bindingEnd), ',').map((part) =>
            part.length === 1 && part[0]?.kind === 'identifier' ? part[0].value : undefined,
          )
          if (iterators.some((name) => name === undefined)) throw new Error('unsupported Lua for-loop binding')
          for (const iterator of iterators as string[]) declareLexicalBinding(bodyScope, iterator)
        } else if (tokensReferenceDeclaration(statement.header, binding, declaration, scope)) {
          return true
        }
        if (statementsReferenceDeclaration(statement.body, binding, declaration, bodyScope)) return true
        continue
      }

      if (statementsReferenceDeclaration(statement.body, binding, declaration, new Map(scope))) return true
    }
    return false
  }

  function functionCapturesBinding(statement: Extract<LuaStatement, { kind: 'function' }>, binding: string): boolean {
    const declaration = { identity: Symbol(binding) }
    const enclosingScope: LexicalScope = new Map([[binding, declaration]])
    return functionReferencesDeclaration(statement, binding, declaration, enclosingScope)
  }

  function visitCalls(tokens: readonly LuaToken[], state: LuaAnalysisState, stack: readonly string[]): void {
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
      const escapedRecords = new Set<`data-record:${number}`>()
      for (const argument of args) {
        const exactEncoding =
          callee === 'cjson.encode' && args.length === 1 ? exactPrivateRecordValue(argument, state) : undefined
        if (exactEncoding !== undefined) continue
        for (const identity of escapingRecordIdentities(argument, state)) escapedRecords.add(identity)
      }
      invalidateRecordIdentities(state, escapedRecords)
      for (const argument of args) visitCalls(argument, state, stack)

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
          const provenance = sourceOf(key, state)
          if (provenance !== 'KEYS') {
            throw new Error(`${command} key operand '${renderTokens(key)}' comes from ${provenance}`)
          }
        }
      } else {
        const fn = localFunction(state, callee)
        if (fn !== undefined) {
          if (stack.includes(callee)) throw new Error(`recursive Lua function '${callee}' cannot be certified`)
          // Only parameter provenance crosses a function boundary. Treating captured or global values as
          // unknown is deliberately conservative: a script must thread a Redis key through a certified call.
          const localState: LuaAnalysisState = {
            bindings: new Map(),
            invalidatedRecords: new Set(state.invalidatedRecords),
          }
          for (const [name, binding] of state.bindings) {
            if (binding.kind === 'function') localState.bindings.set(name, binding)
          }
          const parameters = new Set<string>()
          for (let parameter = 0; parameter < fn.parameters.length; parameter++) {
            const name = fn.parameters[parameter] as string
            installBinding(
              localState,
              name,
              valueBinding(valueSourceOf(args[parameter] ?? [], state)),
              parameters.has(name) ? 'rebind' : 'declare',
            )
            parameters.add(name)
          }
          analyzeBlock(fn.body, localState, [...stack, callee])
          for (const identity of localState.invalidatedRecords) state.invalidatedRecords.add(identity)
        } else if (!SAFE_NON_KEY_CALLS.has(callee)) {
          throw new Error(`unsupported Lua callee '${callee}' cannot be certified`)
        }
      }
      index = close
    }
  }

  function analyzeSimple(
    tokens: readonly LuaToken[],
    state: LuaAnalysisState,
    stack: readonly string[],
    scopeLocals: ReadonlySet<string>,
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
        const declared = new Set(scopeLocals)
        for (const name of names as string[]) {
          if (PROTECTED_CALL_BINDINGS.has(name)) {
            throw new Error(`assignment to certified callee '${name}' is unsupported`)
          }
          installBinding(state, name, valueBinding('unknown'), declared.has(name) ? 'rebind' : 'declare')
          declared.add(name)
        }
        return names as string[]
      }
      if (tokens[0]?.value === 'return') {
        const returnedValue = tokens.slice(1)
        invalidateRecordIdentities(state, escapingRecordIdentities(returnedValue, state))
        assertDataOnlyExpression(returnedValue, state, true)
      }
      visitCalls(tokens, state, stack)
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
      const values = splitTokens(tokens.slice(assignment + 1), ',')
      const escapedRecords = new Set<`data-record:${number}`>()
      for (const value of values) {
        for (const identity of escapingRecordIdentities(value, state)) escapedRecords.add(identity)
      }
      invalidateRecordIdentities(state, escapedRecords)
      for (const value of values) visitCalls(value, state, stack)
      const baseSource = valueProvenance(state, target.base) ?? 'unknown'
      if (!isDataRecord(baseSource)) {
        throw new Error(
          `member assignment base '${target.base}' is not a proven private local data record (${baseSource})`,
        )
      }
      for (const value of values) assertDataOnlyExpression(value, state)
      return []
    }
    const values = splitTokens(tokens.slice(assignment + 1), ',')
    for (let index = 0; index < names.length; index++) {
      const name = names[index] as string
      if (!isLocal && !state.bindings.has(name)) {
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
      if (assignedReference !== undefined && localFunction(state, assignedReference) !== undefined) {
        throw new Error(`local function alias '${name}' cannot be certified`)
      }
    }
    const escapedRecords = new Set<`data-record:${number}`>()
    for (const value of values) {
      for (const identity of escapingRecordIdentities(value, state)) escapedRecords.add(identity)
    }
    invalidateRecordIdentities(state, escapedRecords)
    for (const value of values) visitCalls(value, state, stack)
    const sources = values.map((value) => valueSourceOf(value, state))
    for (let index = 0; index < sources.length; index++) {
      const source = sources[index] as Provenance
      if (!isDataRecord(source)) continue
      const value = values[index] ?? []
      if (isLocal && directDataRecordSource(value) !== undefined) {
        assertDataOnlyExpression(value, state)
        continue
      }
      sources[index] = 'unknown'
    }
    const declared = new Set(scopeLocals)
    for (let index = 0; index < names.length; index++) {
      const source = sources[index] ?? 'unknown'
      const name = names[index] as string
      installBinding(
        state,
        name,
        valueBinding(!isLocal && isDataRecord(source) ? 'unknown' : source),
        isLocal && !declared.has(name) ? 'declare' : 'rebind',
      )
      if (isLocal) declared.add(name)
    }
    return isLocal ? (names as string[]) : []
  }

  function analyzeBlock(
    statements: readonly LuaStatement[],
    state: LuaAnalysisState,
    stack: readonly string[],
    restoreLocals = false,
  ): void {
    const incomingBindings = new Map(state.bindings)
    const locals = new Set<string>()
    for (const statement of statements) {
      if (statement.kind === 'simple') {
        for (const name of analyzeSimple(statement.tokens, state, stack, locals)) locals.add(name)
      } else if (statement.kind === 'function') {
        if (!statement.local) throw new Error(`global Lua function '${statement.name}' cannot be certified`)
        if (PROTECTED_CALL_BINDINGS.has(statement.name)) {
          throw new Error(`assignment to certified callee '${statement.name}' is unsupported`)
        }
        for (const parameter of statement.parameters) {
          if (PROTECTED_CALL_BINDINGS.has(parameter)) {
            throw new Error(`local function parameter '${parameter}' shadows a certified callee`)
          }
        }
        const capturedRecords = new Set<DataRecordIdentity>()
        for (const [name, binding] of state.bindings) {
          if (
            binding.kind === 'value' &&
            isDataRecord(binding.provenance) &&
            functionCapturesBinding(statement, name)
          ) {
            capturedRecords.add(binding.provenance)
          }
        }
        invalidateRecordIdentities(state, capturedRecords)
        installBinding(
          state,
          statement.name,
          { kind: 'function', definition: { parameters: statement.parameters, body: statement.body } },
          locals.has(statement.name) ? 'rebind' : 'declare',
        )
        locals.add(statement.name)
      } else if (statement.kind === 'if') {
        const outcomes: LuaAnalysisState[] = []
        for (const branch of statement.branches) {
          visitCalls(branch.condition, state, stack)
          const branchState = cloneState(state)
          analyzeBlock(branch.body, branchState, stack, true)
          outcomes.push(branchState)
        }
        if (statement.elseBody === undefined) outcomes.push(cloneState(state))
        else {
          const elseState = cloneState(state)
          analyzeBlock(statement.elseBody, elseState, stack, true)
          outcomes.push(elseState)
        }
        mergeState(state, outcomes)
      } else if (statement.kind === 'loop') {
        visitCalls(statement.header, state, stack)
        const outsideState = cloneState(state)
        let loopEntry = cloneState(state)
        const loopBindings: string[] = []
        const declaredLoopBindings = new Set<string>()
        const bindingEnd = statement.header.findIndex((token) => token.value === '=' || token.value === 'in')
        if (statement.loopKind === 'for' && bindingEnd < 1) throw new Error('unsupported Lua for-loop binding')
        if (statement.loopKind === 'for') {
          for (const part of splitTokens(statement.header.slice(0, bindingEnd), ',')) {
            if (part.length !== 1 || part[0]?.kind !== 'identifier') throw new Error('unsupported Lua for-loop binding')
            if (PROTECTED_CALL_BINDINGS.has(part[0].value)) {
              throw new Error(`Lua for-loop binding '${part[0].value}' shadows a certified callee`)
            }
            loopBindings.push(part[0].value)
            installBinding(
              loopEntry,
              part[0].value,
              valueBinding('unknown'),
              declaredLoopBindings.has(part[0].value) ? 'rebind' : 'declare',
            )
            declaredLoopBindings.add(part[0].value)
          }
        }
        let stabilized = false
        const maxPasses = 2 * (state.bindings.size + latticePassBound(statement.body))
        for (let pass = 0; pass < maxPasses; pass++) {
          const backEdge = cloneState(loopEntry)
          analyzeBlock(statement.body, backEdge, stack, true)
          const nextEntry = joinedState([loopEntry, backEdge])
          for (const name of loopBindings) installBinding(nextEntry, name, valueBinding('unknown'), 'declare')
          if (sameState(loopEntry, nextEntry)) {
            loopEntry = nextEntry
            stabilized = true
            break
          }
          loopEntry = nextEntry
        }
        if (!stabilized) throw new Error(`Lua loop provenance did not stabilize within ${maxPasses} passes`)
        for (const name of loopBindings) {
          const before = outsideState.bindings.get(name)
          restoreBinding(loopEntry, name, before)
        }
        mergeState(state, [outsideState, loopEntry])
      } else {
        const innerState = cloneState(state)
        analyzeBlock(statement.body, innerState, stack, true)
        mergeState(state, [innerState])
      }
    }
    if (restoreLocals) {
      for (const name of locals) {
        restoreBinding(state, name, incomingBindings.get(name))
      }
    }
  }

  analyzeBlock(new LuaStatementParser(lexLua(lua)).parse(), { bindings: new Map(), invalidatedRecords: new Set() }, [])
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
      `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local record = cjson.decode(raw)
local encoded = cjson.encode(record)
local scalar = record.expiresAt or record.token
local container = { value = record.expiresAt }
local function consume(value)
  return tostring(value)
end
consume(record.expiresAt)
record.expiresAt = tonumber(ARGV[2])
redis.call('SET', KEYS[1], encoded .. scalar .. cjson.encode(container))
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
        message: "member assignment base 'record' is not a proven private local data record",
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
        lua: `${decodedRecord}\nlocal function consume(value)\n  return tostring(value)\nend\nconsume(record)\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'exact compound alias escape',
        lua: `${decodedRecord}\nlocal alias = record or nil\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'exact compound call escape',
        lua: `${decodedRecord}\nlocal function consume(value)\n  return tostring(value)\nend\nconsume(record or nil)\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'nested boolean alias escape',
        lua: `${decodedRecord}\nlocal alias = false or (record and record)\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'concatenation escape',
        lua: `${decodedRecord}\nlocal alias = '' .. record\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'table insertion escape',
        lua: `${decodedRecord}\nlocal container = { value = record or nil }\nrecord.expiresAt = ARGV[2]`,
        message: "unknown value 'record' is not data-only",
      },
      {
        name: 'nested call escape',
        lua: `${decodedRecord}\nlocal function consume(value)\n  return tostring(value)\nend\nconsume(false or (record and record))\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'compound return escape',
        lua: `${decodedRecord}\nreturn false or record\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'multiple records escape together invalidates the first',
        lua: `${decodedRecord}\nlocal other = cjson.decode(raw)\nlocal alias = record or other\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'multiple records escape together invalidates the second',
        lua: `${decodedRecord}\nlocal other = cjson.decode(raw)\nlocal alias = record or other\nother.expiresAt = ARGV[2]`,
        message: "member assignment base 'other' is not a proven private local data record",
      },
      {
        name: 'wrapped cjson.encode argument escapes',
        lua: `${decodedRecord}\nlocal encoded = cjson.encode(record or nil)\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'parenthesized cjson.encode argument escapes',
        lua: `${decodedRecord}\nlocal encoded = cjson.encode((record))\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'branch escape joins monotonically',
        lua: `${decodedRecord}\nif ARGV[2] ~= '' then\n  local alias = record or nil\nend\nrecord.expiresAt = ARGV[3]`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'loop escape joins monotonically',
        lua: `${decodedRecord}\nfor i = 1, 2 do\n  local alias = record or nil\nend\nrecord.expiresAt = ARGV[2]`,
        message: "member assignment base 'record' is not a proven private local data record",
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

  it('keeps one coherent scoped binding per Lua identifier', () => {
    const decodedRecord = `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local record = cjson.decode(raw)`
    const accepted = [
      {
        name: 'function parameter shadows and then reveals the outer record',
        lua: `${decodedRecord}
local function inspect(record)
  return tostring(record)
end
inspect(ARGV[2])
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'loop iterator shadows and then reveals the outer record',
        lua: `${decodedRecord}
for record = 1, 2 do
  local scalar = record
end
record.expiresAt = ARGV[2]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'nested local function shadows and then reveals the outer record',
        lua: `${decodedRecord}
do
  local function record()
    return tostring(ARGV[2])
  end
  local scalar = record()
end
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'branch-local value shadows and then reveals the outer record',
        lua: `${decodedRecord}
if ARGV[2] ~= '' then
  local record = {}
  record.expiresAt = ARGV[2]
end
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'nested same-name local function restores the outer function',
        lua: `local function current(key)
  return redis.call('GET', key)
end
do
  local function current(value)
    return tostring(value)
  end
  local scalar = current(ARGV[1])
end
current(KEYS[1])`,
      },
    ]
    for (const { name, lua } of accepted) {
      expect(() => assertEveryRedisKeyComesFromKeys(lua), name).not.toThrow()
    }

    const rejected = [
      {
        name: 'record is replaced by a same-name local function',
        lua: `${decodedRecord}
local function record()
  return tostring(ARGV[2])
end
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], 'done')`,
        message: "member assignment base 'record' is not a proven private local data record",
      },
      {
        name: 'local function is replaced by a same-name local value',
        lua: `local function current()
  return tostring(ARGV[1])
end
local current = ARGV[1]
current()
redis.call('GET', KEYS[1])`,
        message: "unsupported Lua callee 'current' cannot be certified",
      },
      {
        name: 'multi-local declaration replaces a same-name function atomically',
        lua: `local function current()
  return tostring(ARGV[1])
end
local current, current = ARGV[1], ARGV[2]
current()
redis.call('GET', KEYS[1])`,
        message: "unsupported Lua callee 'current' cannot be certified",
      },
      {
        name: 'global function declaration remains unsupported',
        lua: "function current() return redis.call('GET', KEYS[1]) end\ncurrent()",
        message: "global Lua function 'current' cannot be certified",
      },
      {
        name: 'function-member declaration remains unsupported',
        lua: "function object.current() return redis.call('GET', KEYS[1]) end",
        message: "expected Lua '('",
      },
    ]
    for (const { name, lua, message } of rejected) {
      expect(() => assertEveryRedisKeyComesFromKeys(lua), name).toThrow(message)
    }
  })

  it('keeps record-identity invalidation monotone across lexical scope restoration', () => {
    const decodedRecord = `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local record = cjson.decode(raw)
local escaped = nil`
    const rejected = [
      {
        name: 'exact block shadow escape',
        lua: `${decodedRecord}
do
  local record = record or nil
  escaped = record
end
record.expiresAt = ARGV[2]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'exact one-branch shadow escape',
        lua: `${decodedRecord}
if ARGV[2] ~= '' then
  local record = record or nil
  escaped = record
end
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'nested-scope alias escape',
        lua: `${decodedRecord}
do
  local first = record or nil
  do
    local second = first or nil
    escaped = second
  end
end
record.expiresAt = ARGV[2]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'multiple same-name shadows cannot restore privacy',
        lua: `${decodedRecord}
do
  local record = record or nil
  do
    local record = record or nil
    escaped = record
  end
end
record.expiresAt = ARGV[2]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'early return does not roll back invalidation',
        lua: `${decodedRecord}
do
  local record = record or nil
  return record
end
record.expiresAt = ARGV[2]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'loop-body escape survives zero-iteration join and scope restoration',
        lua: `${decodedRecord}
for i = 1, 2 do
  local record = record or nil
  escaped = record
end
record.expiresAt = ARGV[2]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
      {
        name: 'both branch shadows invalidate the shared outer identity',
        lua: `${decodedRecord}
if ARGV[2] ~= '' then
  local record = record or nil
  escaped = record
else
  local record = record or nil
  escaped = record
end
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`,
      },
    ]
    for (const { name, lua } of rejected) {
      expect(() => assertEveryRedisKeyComesFromKeys(lua), name).toThrow(
        "member assignment base 'record' is not a proven private local data record",
      )
    }

    expect(() =>
      assertEveryRedisKeyComesFromKeys(`${decodedRecord}
do
  local record = {}
  record.expiresAt = ARGV[2]
end
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`),
    ).not.toThrow()
  })

  it('resolves record captures by lexical declaration identity', () => {
    const decodedRecord = `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
local record = cjson.decode(raw)`
    const outerWrite = `
record.expiresAt = ARGV[3]
redis.call('SET', KEYS[1], cjson.encode(record))`
    const accepted = [
      {
        name: 'fresh same-name record inside a local function',
        lua: `${decodedRecord}
local function encodeFresh()
  local record = {}
  record.expiresAt = ARGV[2]
  return cjson.encode(record)
end
local encoded = encodeFresh()
${outerWrite}`,
      },
      {
        name: 'fresh same-name record inside a nested local function',
        lua: `${decodedRecord}
local function encodeFresh()
  local function nested()
    local record = {}
    record.expiresAt = ARGV[2]
    return cjson.encode(record)
  end
  return nested()
end
local encoded = encodeFresh()
${outerWrite}`,
      },
      {
        name: 'function parameter shadows the outer record',
        lua: `${decodedRecord}
local function inspect(record)
  return tostring(record)
end
local scalar = inspect(ARGV[2])
${outerWrite}`,
      },
      {
        name: 'loop iterator shadows only inside the function loop body',
        lua: `${decodedRecord}
local function inspect()
  for record = 1, 2 do
    local scalar = tostring(record)
  end
  return 'done'
end
local scalar = inspect()
${outerWrite}`,
      },
      {
        name: 'same-name local function binds itself before its recursive body',
        lua: `${decodedRecord}
local function scope()
  local function record(value)
    if value == '' then return record end
    return value
  end
end
${outerWrite}`,
      },
      {
        name: 'block branch and loop declarations stay in their lexical scopes',
        lua: `${decodedRecord}
local function shadows()
  do
    local record = {}
    record.expiresAt = ARGV[2]
  end
  if ARGV[2] ~= '' then
    local record = {}
    record.expiresAt = ARGV[2]
  else
    local record = {}
    record.expiresAt = ARGV[2]
  end
  for record = 1, 2 do
    local scalar = tostring(record)
  end
end
${outerWrite}`,
      },
      {
        name: 'member field names are not identifier captures',
        lua: `${decodedRecord}
local function makePayload()
  local payload = { value = ARGV[2] }
  return tostring(payload.record)
end
local encoded = makePayload()
${outerWrite}`,
      },
    ]
    for (const { name, lua } of accepted) {
      expect(() => assertEveryRedisKeyComesFromKeys(lua), name).not.toThrow()
    }

    const rejected = [
      {
        name: 'direct outer record capture',
        body: 'local value = record.expiresAt',
      },
      {
        name: 'use before a later same-name local declaration',
        body: `local value = record.expiresAt
local record = {}`,
      },
      {
        name: 'local initializer resolves its own name in the outer scope',
        body: 'local record = record or nil',
      },
      {
        name: 'nested closure propagates a genuine outer capture',
        body: `local function nested()
  return record.expiresAt
end`,
      },
      {
        name: 'a branch-local shadow does not cover a later outer use',
        body: `if ARGV[2] ~= '' then
  local record = {}
  record.expiresAt = ARGV[2]
end
return record.expiresAt`,
      },
      {
        name: 'a block-local shadow does not cover a later outer use',
        body: `do
  local record = {}
  record.expiresAt = ARGV[2]
end
return record.expiresAt`,
      },
      {
        name: 'a loop iterator does not cover a later outer use',
        body: `for record = 1, 2 do
  local scalar = tostring(record)
end
return record.expiresAt`,
      },
      {
        name: 'a for-loop initializer resolves before the iterator declaration',
        body: `for record = tonumber(record.expiresAt), 2 do
  local scalar = tostring(record)
end`,
      },
      {
        name: 'deeply nested closures propagate a genuine free reference',
        body: `local function middle()
  local function nested()
    return record.expiresAt
  end
end`,
      },
    ]
    for (const { name, body } of rejected) {
      const lua = `${decodedRecord}
local function inspect()
${body}
end
${outerWrite}`
      expect(() => assertEveryRedisKeyComesFromKeys(lua), name).toThrow(
        "member assignment base 'record' is not a proven private local data record",
      )
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
      assertEveryRedisKeyComesFromKeys(
        "if true then local function read(key) return redis.call('GET', key) end read(KEYS[1]) end",
      ),
    ).not.toThrow()
    expect(() => assertEveryRedisKeyComesFromKeys("function read() redis.call('GET', KEYS[1]) end\nread()")).toThrow(
      "global Lua function 'read' cannot be certified",
    )
    expect(() => assertEveryRedisKeyComesFromKeys("repeat redis.call('GET', KEYS[1]) until true")).toThrow(
      "unsupported Lua control syntax 'repeat'",
    )
  })
})
