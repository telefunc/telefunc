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

type Provenance = 'KEYS' | 'ARGV' | 'unknown'

type LuaToken = {
  kind: 'identifier' | 'number' | 'string' | 'symbol' | 'newline'
  value: string
}

type LuaStatement =
  | { kind: 'simple'; tokens: readonly LuaToken[] }
  | { kind: 'function'; name: string; parameters: readonly string[]; body: readonly LuaStatement[] }
  | {
      kind: 'if'
      branches: readonly { condition: readonly LuaToken[]; body: readonly LuaStatement[] }[]
      elseBody?: readonly LuaStatement[]
    }
  | { kind: 'loop'; header: readonly LuaToken[]; body: readonly LuaStatement[] }
  | { kind: 'do'; body: readonly LuaStatement[] }

const OPEN = new Map([
  ['(', ')'],
  ['[', ']'],
  ['{', '}'],
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
      if (token.value === 'local' && this.#peek(1)?.value === 'function') {
        this.#index++
        statements.push(this.#parseFunction())
      } else if (token.value === 'function') {
        statements.push(this.#parseFunction())
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

  #parseFunction(): LuaStatement {
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
    return { kind: 'function', name, parameters, body }
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
    this.#index++
    const header = this.#takeUntil('do')
    this.#expect('do')
    const body = this.#parseBlock(new Set(['end']))
    this.#expect('end')
    return { kind: 'loop', header, body }
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

function mergeAliases(target: Map<string, Provenance>, outcomes: readonly ReadonlyMap<string, Provenance>[]): void {
  const names = new Set(outcomes.flatMap((outcome) => [...outcome.keys()]))
  for (const name of names) {
    const sources = new Set(outcomes.map((outcome) => outcome.get(name) ?? 'unknown'))
    target.set(name, sources.size === 1 ? ([...sources][0] as Provenance) : 'unknown')
  }
}

function assertEveryRedisKeyComesFromKeys(lua: string): void {
  const functions = new Map<string, LuaFunction>()
  let redisCallCount = 0

  function visitCalls(tokens: readonly LuaToken[], aliases: Map<string, Provenance>, stack: readonly string[]): void {
    for (let index = 0; index < tokens.length; index++) {
      if (tokens[index]?.kind !== 'identifier') continue
      let callee = tokens[index]?.value as string
      let open = index + 1
      while (tokens[open]?.value === '.' && tokens[open + 1]?.kind === 'identifier') {
        callee += `.${tokens[open + 1]?.value as string}`
        open += 2
      }
      if (tokens[open]?.value !== '(') continue
      const close = findCallClose(tokens, open)
      const args = splitTokens(tokens.slice(open + 1, close), ',')
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
            localAliases.set(fn.parameters[parameter] as string, sourceOf(args[parameter] ?? [], aliases))
          }
          analyzeBlock(fn.body, localAliases, [...stack, callee])
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
    const assignment = tokens.findIndex((token) => token.value === '=')
    if (assignment < 0) {
      visitCalls(tokens, aliases, stack)
      return []
    }
    const isLocal = tokens[0]?.value === 'local'
    const left = tokens.slice(isLocal ? 1 : 0, assignment)
    const names = splitTokens(left, ',').map((part) => (part.length === 1 ? part[0]?.value : undefined))
    if (names.some((name) => name === undefined)) {
      visitCalls(tokens, aliases, stack)
      return []
    }
    const values = splitTokens(tokens.slice(assignment + 1), ',')
    for (const value of values) visitCalls(value, aliases, stack)
    const sources = values.map((value) => sourceOf(value, aliases))
    for (let index = 0; index < names.length; index++) {
      aliases.set(names[index] as string, sources[index] ?? 'unknown')
    }
    return isLocal ? (names as string[]) : []
  }

  function analyzeBlock(
    statements: readonly LuaStatement[],
    aliases: Map<string, Provenance>,
    stack: readonly string[],
    restoreLocals = false,
  ): void {
    const incoming = new Map(aliases)
    const locals = new Set<string>()
    for (const statement of statements) {
      if (statement.kind === 'simple') {
        for (const name of analyzeSimple(statement.tokens, aliases, stack)) locals.add(name)
      } else if (statement.kind === 'function') {
        functions.set(statement.name, {
          parameters: statement.parameters,
          body: statement.body,
        })
      } else if (statement.kind === 'if') {
        const outcomes: Map<string, Provenance>[] = []
        for (const branch of statement.branches) {
          visitCalls(branch.condition, aliases, stack)
          const branchAliases = new Map(aliases)
          analyzeBlock(branch.body, branchAliases, stack, true)
          outcomes.push(branchAliases)
        }
        if (statement.elseBody === undefined) outcomes.push(new Map(aliases))
        else {
          const elseAliases = new Map(aliases)
          analyzeBlock(statement.elseBody, elseAliases, stack, true)
          outcomes.push(elseAliases)
        }
        mergeAliases(aliases, outcomes)
      } else if (statement.kind === 'loop') {
        visitCalls(statement.header, aliases, stack)
        const loopAliases = new Map(aliases)
        const loopBindings: string[] = []
        const bindingEnd = statement.header.findIndex((token) => token.value === '=' || token.value === 'in')
        if (bindingEnd >= 0) {
          for (const part of splitTokens(statement.header.slice(0, bindingEnd), ',')) {
            if (part.length === 1 && part[0]?.kind === 'identifier') {
              loopBindings.push(part[0].value)
              loopAliases.set(part[0].value, 'unknown')
            }
          }
        }
        analyzeBlock(statement.body, loopAliases, stack, true)
        for (const name of loopBindings) {
          const before = aliases.get(name)
          if (before === undefined) loopAliases.delete(name)
          else loopAliases.set(name, before)
        }
        mergeAliases(aliases, [aliases, loopAliases])
      } else {
        const innerAliases = new Map(aliases)
        analyzeBlock(statement.body, innerAliases, stack, true)
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
})
