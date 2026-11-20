import { describe, expect, it } from 'vitest'
import { REDIS_COMMANDS, holdsLua } from './commands.js'

/** The commands a script marked as only reading may make: a failure of such a script is told it didn't run. */
const READS = new Set([
  'EXISTS',
  'GET',
  'HEXISTS',
  'HGET',
  'HLEN',
  'HSCAN',
  'SCARD',
  'SSCAN',
  'TIME',
  'TYPE',
  'ZRANGEBYLEX',
  'ZRANGEBYSCORE',
])

describe('a script marked as only reading', () => {
  it.each(Object.values(REDIS_COMMANDS).filter((command) => !command.writes))('$name calls only reads', ({ lua }) => {
    const called = Array.from(lua.matchAll(/redis\.p?call\(([^,)]*)/g), ([, name]) => name)
    expect(called.length).toBeGreaterThan(0)
    expect(called.filter((name) => !READS.has(name.replace(/^'(\w+)'$/, '$1')))).toEqual([])
  })
})

describe("a write script's guard", () => {
  const writing = Object.values(REDIS_COMMANDS).filter((command) => command.writes)

  it.each(writing)('$name checks every key it names by index', ({ lua, holds, numberOfKeys }) => {
    expect(holds).toBeDefined()
    const named = Array.from(lua.matchAll(/KEYS\[(\d+)\]/g), ([, index]) => Number(index))
    expect(holds?.length).toBe(numberOfKeys ?? Math.max(...named))
    expect(named.filter((index) => index > (holds?.length ?? 0))).toEqual([])
    expect(lua).toContain(holdsLua('KEYS', holds ?? []))
  })

  it.each(writing)("$name's refusal is the guard's alone", ({ lua }) => {
    expect(lua.split('TFNOTRUN').length - 1).toBe(1)
    expect(lua).toContain("error({ err = 'TFNOTRUN ' .. word .. ' ' .. detail })")
  })
})
