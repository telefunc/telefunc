import { hasFlag } from '@ioredis/commands'
import { describe, expect, it } from 'vitest'
import { isBug } from './assert.js'
import { REDIS_COMMANDS } from './commands.js'

/** Whether a script marked as only reading may make the call `name` (with `sub`, its first argument): a failure of such
 *  a script is told it didn't run. The commands ioredis's own table flags `readonly`, TIME, which reads the clock
 *  (flagged `loading, stale, fast`), and INFO and CLUSTER INFO, which read the node's state. */
const reads = (name: string, sub: string) =>
  name === 'TIME' ||
  name === 'INFO' ||
  (name === 'CLUSTER' && sub === 'INFO') ||
  hasFlag(name.toLowerCase(), 'readonly')

describe('a script marked as only reading', () => {
  it.each(Object.values(REDIS_COMMANDS).filter((command) => !command.writes))('$name calls only reads', ({ lua }) => {
    const called = Array.from(lua.matchAll(/redis\.p?call\(([^,)]*)(?:, '(\w+)')?/g), ([, name = '', sub = '']) => ({
      name: name.replace(/^'(\w+)'$/, '$1'),
      sub,
    }))
    expect(called.length).toBeGreaterThan(0)
    expect(called.filter(({ name, sub }) => !reads(name, sub))).toEqual([])
  })
})

describe("a write script's checks", () => {
  const writing = Object.values(REDIS_COMMANDS).filter((command) => command.writes)

  it.each(writing)('$name says what each key it names by index holds', ({ lua, holds, numberOfKeys }) => {
    const named = Array.from(lua.matchAll(/KEYS\[(\d+)\]/g), ([, index]) => Number(index))
    expect(holds?.length).toBe(numberOfKeys ?? Math.max(...named))
    expect(named.filter((index) => index > (holds?.length ?? 0))).toEqual([])
  })

  // A TYPE check names each key the script's holds give a type, by index or by the local the script gives it: what a
  // pcall's reply refuses checks the key's type too (`heldReplyLua`). A channel holds no type.
  it.each(writing)('$name checks the type of each key it names by index', ({ lua, holds }) => {
    const aliases = new Map<string, string>()
    for (const [, names = '', values = ''] of lua.matchAll(/local ([\w, ]+?)\s*=\s*((?:KEYS\[\d+\][,\s]*)+)/g)) {
      const indexed = Array.from(values.matchAll(/KEYS\[\d+\]/g), ([key]) => key)
      names.split(',').forEach((name, at) => {
        const key = indexed[at]
        if (key !== undefined) aliases.set(name.trim(), key)
      })
    }
    const typed = new Set(
      Array.from(
        lua.matchAll(/redis\.call\('TYPE', ([^)]+)\)/g),
        ([, key = '']) => aliases.get(key.trim()) ?? key.trim(),
      ),
    )
    const unchecked = (holds ?? []).flatMap((hold, at) =>
      hold === 'channel' || typed.has(`KEYS[${at + 1}]`) ? [] : [`KEYS[${at + 1}] (${hold})`],
    )
    expect(unchecked).toEqual([])
  })

  it.each(writing)('$name refuses only with TFNOTRUN and its word at the start of the reply', ({ lua }) => {
    const refused = Array.from(lua.matchAll(/error\(\{ err = 'TFNOTRUN (WRONGTYPE|ERR) ' \.\. /g))
    expect(lua.split('TFNOTRUN').length - 1).toBe(refused.length)
  })
})

describe('a cells read script reply of another shape', () => {
  const input = { roomId: 'r', inc: 'i', sel: { keys: ['k'] } }
  const told = (reply: unknown) =>
    Promise.resolve()
      .then(() => REDIS_COMMANDS.readCells.parse(reply, input))
      .then(
        () => 'parsed',
        (error: unknown) => (isBug(error) ? 'a bug' : String(error)),
      )
  const bytes = (...items: string[]) => items.map((item) => Buffer.from(item))

  it.each([
    ['a cell without whether it lapsed', bytes('cells', '1', 'k', 'v', 'version')],
    ['a cell without the cell its reference names', [...bytes('cells:beside', '1', 'k', 'v', 'version', '0', 'cell')]],
    ['an item that is no bytes', [...bytes('cells', '1', 'k', 'v', 'version'), 0]],
  ])('with %s is a bug', async (_, reply) => {
    expect(await told(reply)).toBe('a bug')
  })
})
