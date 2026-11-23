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

/** Where each refusal of `lua` starts and ends: `error({ … })` around its TFNOTRUN reply. */
function refusals(lua: string): { start: number; end: number }[] {
  const spans: { start: number; end: number }[] = []
  for (const { index } of lua.matchAll(/error\(\{ err = 'TFNOTRUN /g)) {
    let depth = 0
    let at = index
    let quote = ''
    for (; at < lua.length; at++) {
      const char = lua[at]
      if (quote !== '') {
        if (char === '\\') at++
        else if (char === quote) quote = ''
      } else if (char === "'" || char === '"') quote = char
      else if (char === '(' || char === '{') depth++
      else if (char === ')' || char === '}') {
        depth--
        if (depth === 0) break
      }
    }
    spans.push({ start: index, end: at + 1 })
  }
  return spans
}

/** The string literals of `lua`. */
const literals = (lua: string) => Array.from(lua.matchAll(/'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"/g), ([text]) => text)

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

describe('a call Redis does not refuse', () => {
  // Redis runs a script's whole chunk on every call: each `local function` is defined anew on each.
  it('defines only the functions its script uses outside its checks', () => {
    const defined = Object.fromEntries(
      Object.values(REDIS_COMMANDS).map(({ name, lua }) => [
        name,
        Array.from(lua.matchAll(/^\s*local function (\w+)/gm), ([, defined]) => defined),
      ]),
    )
    const head = ['tf_now', 'tf_live_head']
    const cellWrites = ['tf_write', 'tf_cell_writes_allowed', 'tf_count']
    const frame = ['tf_u64', 'tf_ordering_frame']
    expect(defined).toEqual({
      tfPublish: [],
      tfRoomHeadExchange: ['tf_allowed', ...head, ...cellWrites],
      tfRoomReadHead: head,
      tfRoomReadCells: [...head, 'tf_lapsed', 'tf_cells_reply'],
      tfRoomReadExpiringCells: head,
      tfRoomReadCellPage: [...head, 'tf_lapsed', 'tf_cells_reply'],
      tfRoomLaneStart: [...head, 'tf_authority'],
      tfRoomMemberLaneStarts: [...head, 'tf_authority'],
      tfRoomTrackLanes: [...head, 'tf_authority'],
      tfRoomLaneMarks: ['tf_authority'],
      tfBroadcastStart: ['tf_authority'],
      tfRoomDropIncarnation: ['tf_allowed'],
      tfRoomCellsExchange: ['tf_allowed', ...head, ...cellWrites],
      tfRoomCommit: ['tf_allowed', ...head, ...frame, 'tf_lane_order'],
      tfRoomOperation: [
        'tf_allowed',
        ...head,
        ...frame,
        'tf_lane_order',
        ...cellWrites,
        'tf_retained_delete',
        'tf_commit',
      ],
      tfRoomDirectoryPut: ['tf_allowed', 'tf_now'],
      tfRoomDirectoryDelete: ['tf_allowed'],
      tfRoomDirectoryDue: ['tf_now'],
    })
  })

  it.each(Object.values(REDIS_COMMANDS))("$name builds a refusal's words only in the refusal", ({ lua }) => {
    const spans = refusals(lua)
    let outside = ''
    let last = 0
    for (const { start, end } of spans) {
      outside += lua.slice(last, start)
      last = end
    }
    outside = (outside + lua.slice(last)).replace(/--.*$/gm, '')
    const words = new Set(spans.flatMap(({ start, end }) => literals(lua.slice(start, end))))
    expect(literals(outside).filter((literal) => words.has(literal) || literal.includes('Telefunc'))).toEqual([])
  })
})

describe("the cells exchange's control record", () => {
  const input = { roomId: 'r', inc: 'i', exchange: { expect: [], mutations: [] }, record: { counter: 'c', bytes: 1 } }
  const parsed = (reply: string[]) =>
    Promise.resolve()
      .then(() => REDIS_COMMANDS.cellsExchange.parse(reply, input))
      .then(
        (result) => result,
        (error: unknown) => error,
      )

  it("refused for its size: the error the record's commit throws", async () => {
    const told = await parsed(['record-overflow', '1'])
    expect(told).toMatchObject({
      name: 'ChannelOverflowError',
      message: "Telefunc's Redis backend takes a message of 29360128 bytes at most, and this one has 29360129",
    })
    expect(told).not.toHaveProperty('outcome')
  })

  it('numbered by a counter the exchange leaves absent: a bug', async () => {
    expect(String(await parsed(['record-unnumbered']))).toMatch(
      /\[Bug\].*cells exchange: its control record is numbered by a counter it leaves absent/,
    )
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
