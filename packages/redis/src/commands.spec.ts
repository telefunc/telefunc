import { describe, expect, it } from 'vitest'
import { REDIS_COMMANDS } from './commands.js'

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
