import { hasFlag } from '@ioredis/commands'
import { describe, expect, it } from 'vitest'
import { REDIS_COMMANDS, holdsLua } from './commands.js'

/** Whether a script marked as only reading may make the call `name`: a failure of such a script is told it didn't run.
 *  The commands ioredis's own table flags `readonly`, and TIME, which reads the clock (flagged `loading, stale, fast`). */
const reads = (name: string) => name === 'TIME' || hasFlag(name.toLowerCase(), 'readonly')

describe('a script marked as only reading', () => {
  it.each(Object.values(REDIS_COMMANDS).filter((command) => !command.writes))('$name calls only reads', ({ lua }) => {
    const called = Array.from(lua.matchAll(/redis\.p?call\(([^,)]*)/g), (match) => match[1] ?? '')
    expect(called.length).toBeGreaterThan(0)
    expect(called.filter((name) => !reads(name.replace(/^'(\w+)'$/, '$1')))).toEqual([])
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
