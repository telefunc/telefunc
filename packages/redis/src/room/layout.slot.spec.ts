import { describe, expect, it } from 'vitest'
import {
  CAPTURE_GENERATION_LUA,
  CELLS_CX_LUA,
  channelKey,
  COMMIT_LUA,
  generationInvalidationChannel,
  generationTokensKey,
  gensKey,
  headKey,
  headRevKey,
  HEAD_CX_LUA,
  orderKey,
  retainedKey,
  retainedSizeKey,
  revKey,
  routeCaptureExpiriesKey,
  routeCapturesKey,
  VALIDATE_GENERATION_LUA,
} from './layout.js'

// Redis Cluster's CRC16/XMODEM key-slot algorithm. Keeping it in the test makes the one-room-slot
// claim executable without requiring a Cluster before W4-R.
function slot(key: string): number {
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

function assertLuaKeysOnly(lua: string): void {
  expect(lua).not.toMatch(/redis\.call\(\s*['"][^'"]+['"]\s*,\s*ARGV\[/)
  expect(lua).not.toMatch(/\b[\w_, ]*key[\w_, ]*=\s*ARGV\[/)
}

describe('Redis Room key-slot and Lua key declarations', () => {
  it('keeps every room key in one escaped hash slot', () => {
    const prefix = 'tf:'
    const roomId = 'room} with space'
    const inc = 'generation'
    const keys = [
      headKey(prefix, roomId),
      headRevKey(prefix, roomId),
      gensKey(prefix, roomId),
      generationTokensKey(prefix, roomId),
      routeCapturesKey(prefix, roomId),
      routeCaptureExpiriesKey(prefix, roomId),
      revKey(prefix, roomId, inc),
      orderKey(prefix, roomId, inc, 'semantic'),
      retainedKey(prefix, roomId, inc, 'semantic'),
      retainedSizeKey(prefix, roomId, inc),
      channelKey(prefix, roomId, inc, 'semantic'),
      generationInvalidationChannel(prefix, roomId, inc),
    ]
    expect(new Set(keys.map(slot))).toEqual(new Set([slot(keys[0] as string)]))
    expect(keys.every((key) => key.includes('{room%7D%20with%20space}'))).toBe(true)
  })

  it('requires Lua key access to come from KEYS, not ARGV', () => {
    for (const lua of [HEAD_CX_LUA, CAPTURE_GENERATION_LUA, VALIDATE_GENERATION_LUA, CELLS_CX_LUA, COMMIT_LUA]) {
      assertLuaKeysOnly(lua)
    }
  })
})
