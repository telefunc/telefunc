import { describe, expect, it } from 'vitest'
import {
  cellKey,
  channelKey,
  directoryIndexKey,
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

function splitTopLevel(value: string): string[] {
  const parts: string[] = []
  let start = 0
  let depth = 0
  let quote: string | null = null
  for (let index = 0; index < value.length; index++) {
    const char = value[index] as string
    if (quote !== null) {
      if (char === quote && value[index - 1] !== '\\') quote = null
    } else if (char === "'" || char === '"') {
      quote = char
    } else if (char === '(' || char === '{' || char === '[') {
      depth++
    } else if (char === ')' || char === '}' || char === ']') {
      depth--
    } else if (char === ',' && depth === 0) {
      parts.push(value.slice(start, index).trim())
      start = index + 1
    }
  }
  parts.push(value.slice(start).trim())
  return parts
}

function sourceOf(expression: string, aliases: ReadonlyMap<string, Provenance>): Provenance {
  if (/\bARGV\s*\[/.test(expression)) return 'ARGV'
  if (/\bKEYS\s*\[/.test(expression)) return 'KEYS'
  const identifier = expression.trim().match(/^[A-Za-z_]\w*$/)?.[0]
  return identifier === undefined ? 'unknown' : (aliases.get(identifier) ?? 'unknown')
}

function assertEveryRedisKeyComesFromKeys(lua: string): void {
  const aliases = new Map<string, Provenance>()
  const assignment = /^\s*(?:local\s+)?([A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)\s*=\s*(.+)$/
  for (const raw of lua.split('\n')) {
    const line = raw.replace(/--.*$/, '')
    const match = line.match(assignment)
    if (match === null) continue
    const names = (match[1] as string).split(',').map((name) => name.trim())
    const values = splitTopLevel(match[2] as string)
    for (let index = 0; index < names.length; index++) {
      aliases.set(names[index] as string, sourceOf(values[index] ?? '', aliases))
    }
  }

  // tf_head's key parameter is safe only because every invocation receives a KEYS-derived alias.
  const headCalls = [...lua.matchAll(/(?<!function\s)\btf_head\(([^)]+)\)/g)]
  if (headCalls.length > 0) {
    const sources = headCalls.map((call) => sourceOf(splitTopLevel(call[1] as string)[0] ?? '', aliases))
    aliases.set('key', sources.every((source) => source === 'KEYS') ? 'KEYS' : 'unknown')
  }

  const singleKeyCommands = new Set([
    'DEL',
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
  const calls = [...lua.matchAll(/redis\.call\(\s*(['"])([A-Z]+)\1(?:\s*,\s*([^,\n)]+))?/g)]
  expect(calls.length).toBeGreaterThan(0)
  for (const call of calls) {
    const command = call[2] as string
    if (command === 'TIME') continue
    expect(singleKeyCommands, `unclassified Redis key signature for ${command}`).toContain(command)
    const keyExpression = call[3] ?? ''
    expect(sourceOf(keyExpression, aliases), `${command} key operand '${keyExpression.trim()}'`).toBe('KEYS')
  }
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
})
