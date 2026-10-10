import { DatabaseSync, type SQLInputValue } from 'node:sqlite'
import { expect, test } from 'vitest'
import type { CellsCx } from '../../../../backend/room/contract.js'
import {
  compareExchangeCells,
  compareExchangeHead,
  directoryList,
  directoryPut,
  initSchema,
  readCells,
} from './storage.js'

/** Cloudflare's SQL storage over node:sqlite, recording the plan of each read. */
function sqlStorage() {
  const db = new DatabaseSync(':memory:')
  const plans: string[] = []
  const sql = {
    exec(query: string, ...bindings: SQLInputValue[]) {
      if (bindings.length === 0 && !query.startsWith('SELECT')) {
        db.exec(query)
        return { toArray: () => [] }
      }
      if (!query.startsWith('SELECT')) {
        db.prepare(query).run(...bindings)
        return { toArray: () => [] }
      }
      const plan = db.prepare(`EXPLAIN QUERY PLAN ${query}`).all(...bindings) as Array<{ detail: string }>
      plans.push(...plan.map((step) => step.detail))
      const rows = db.prepare(query).all(...bindings)
      return { toArray: () => rows }
    },
  } as unknown as SqlStorage
  initSchema(sql)
  return { sql, plans }
}

test("a directory page reads a prefix's rooms as a primary-key range, up to the next code point", () => {
  const { sql, plans } = sqlStorage()
  const rooms = ['game', 'game:', 'game:1', 'game:é', 'game;', 'x\u{10ffff}y', 'y', '\u{d7ff}a', '\u{e000}']
  for (const roomId of rooms) directoryPut(sql, roomId, 'tag')
  const listed = (prefix: string) => directoryList(sql, prefix).entries.map((entry) => entry.roomId)
  expect(listed('game:')).toEqual(['game:', 'game:1', 'game:é'])
  expect(plans).toEqual([expect.stringMatching(/^SEARCH directory USING .*INDEX \w+ \(room_id>\? AND room_id<\?\)$/)])
  expect(listed('x\u{10ffff}')).toEqual(['x\u{10ffff}y'])
  // Surrogates are no code points: the one after U+D7FF is U+E000.
  expect(listed('\u{d7ff}')).toEqual(['\u{d7ff}a'])
  expect(listed('')).toHaveLength(rooms.length)
})

test('a directory lists its rooms in code point order', () => {
  const { sql } = sqlStorage()
  for (const roomId of ['order:a', 'order:\u{1f600}', 'order:é', 'order:\uff01', 'order:B', 'order:f'])
    directoryPut(sql, roomId, 'tag')
  expect(directoryList(sql, 'order:').entries.map((entry) => entry.roomId)).toEqual([
    'order:B',
    'order:a',
    'order:f',
    'order:é',
    'order:\uff01',
    'order:\u{1f600}',
  ])
})

test("a directory pages a prefix's rooms from the cursor it hands back", () => {
  const { sql } = sqlStorage()
  const rooms = Array.from({ length: 150 }, (_, index) => `room:${String(index).padStart(3, '0')}`)
  for (const roomId of ['roo', ...rooms, 'roon']) directoryPut(sql, roomId, 'tag')
  const first = directoryList(sql, 'room:')
  const second = directoryList(sql, 'room:', first.cursor)
  expect([...first.entries, ...second.entries].map((entry) => entry.roomId)).toEqual(rooms)
  expect(second.cursor).toBeUndefined()
})

test("a prefix read selects the incarnation's cells under it as a primary-key range", () => {
  const { sql, plans } = sqlStorage()
  const now = Date.now()
  const config = new TextEncoder().encode('config')
  compareExchangeHead(sql, { form: 'absent' }, { head: { state: 'open', currentInc: 'inc-1', config } }, now)
  const keys = ['m:1', 'identity:a:1', 'identity:a:2', 'identity:ab:3', 'identity;', 'x\u{10ffff}y']
  const mutations = keys.map((key) => ({ key, bytes: new Uint8Array([1]) }))
  expect(compareExchangeCells(sql, 'inc-1', { expect: [], mutations }, now)).toEqual({ committed: new Map() })
  const listed = (prefix: string) => {
    const read = readCells(sql, 'inc-1', { prefix }, now)
    if ('staleInc' in read) throw new Error('cell read fenced unexpectedly')
    return [...read.cells.keys()]
  }
  plans.length = 0
  expect(listed('identity:a:')).toEqual(['identity:a:1', 'identity:a:2'])
  expect(plans.filter((plan) => plan.includes(' cell '))).toEqual([
    expect.stringMatching(/^SEARCH cell USING .*INDEX \w+ \(inc=\? AND key>\? AND key<\?\)$/),
  ])
  expect(listed('identity:')).toEqual(['identity:a:1', 'identity:a:2', 'identity:ab:3'])
  expect(listed('x\u{10ffff}')).toEqual(['x\u{10ffff}y'])
  expect(listed('')).toHaveLength(keys.length)
})

test('an exchange deletes every cell under a cleared prefix before its mutations, only when it commits', () => {
  const { sql } = sqlStorage()
  compareExchangeHead(
    sql,
    { form: 'absent' },
    { head: { state: 'open', currentInc: 'inc', config: new TextEncoder().encode('config') } },
    1_000,
  )
  const exchange = (cx: CellsCx) => compareExchangeCells(sql, 'inc', cx, 1_000)
  const set = (key: string) => ({ key, bytes: new TextEncoder().encode(key) })
  exchange({ expect: [], mutations: [set('p:a'), set('p:b'), set('p:\u{10ffff}'), set('pq'), set('q')] })
  expect(exchange({ expect: [{ key: 'q', version: null }], clear: ['p:'], mutations: [] })).toBe('conflict')
  expect(exchange({ expect: [], clear: ['p:'], mutations: [set('p:b')] })).toEqual({ committed: new Map() })
  const read = readCells(sql, 'inc', { prefix: '' }, 1_000)
  if ('staleInc' in read) throw new Error('the room is gone')
  expect([...read.cells.keys()].sort()).toEqual(['p:b', 'pq', 'q'])
})

test('an exchange commits while the cells it expects are as read, whatever else changed, and never conflicts on a count', () => {
  const { sql } = sqlStorage()
  compareExchangeHead(
    sql,
    { form: 'absent' },
    { head: { state: 'open', currentInc: 'inc', config: new TextEncoder().encode('config') } },
    1_000,
  )
  const exchange = (cx: CellsCx, now: number) => compareExchangeCells(sql, 'inc', cx, now)
  const bytes = (value: string) => new TextEncoder().encode(value)
  expect(
    exchange(
      {
        expect: [{ key: 'a', version: null }],
        mutations: [{ key: 'a', bytes: bytes('a1') }],
        counts: [{ key: 'n', delta: 0, create: true }],
      },
      1_000,
    ),
  ).toEqual({ committed: new Map([['n', { value: 0, changedAt: 1_000 }]]) })
  const read = readCells(sql, 'inc', { keys: ['a'] }, 1_000)
  if ('staleInc' in read) throw new Error('the room is gone')
  const a = read.versions.get('a')!
  // Another writer: a cell this exchange didn't read, and the counter.
  exchange({ expect: [{ key: 'b', version: null }], mutations: [{ key: 'b', bytes: bytes('b1') }] }, 2_000)
  expect(exchange({ expect: [], mutations: [], counts: [{ key: 'n', delta: 1 }] }, 2_000)).toEqual({
    committed: new Map([['n', { value: 1, changedAt: 2_000 }]]),
  })
  expect(
    exchange(
      {
        expect: [{ key: 'a', version: a }],
        mutations: [{ key: 'a', bytes: bytes('a2') }],
        counts: [{ key: 'n', delta: 2 }],
      },
      3_000,
    ),
  ).toEqual({ committed: new Map([['n', { value: 3, changedAt: 3_000 }]]) })
  // `a` was written since that read.
  expect(exchange({ expect: [{ key: 'a', version: a }], mutations: [], counts: [{ key: 'n', delta: 5 }] }, 3_000)).toBe(
    'conflict',
  )
  const counts = [
    { key: 'n', delta: 0 },
    { key: 'missing', delta: 1 },
  ]
  expect(exchange({ expect: [], mutations: [], counts }, 4_000)).toEqual({
    committed: new Map([['n', { value: 3, changedAt: 3_000 }]]),
  })
  const after = readCells(sql, 'inc', { keys: ['a', 'n', 'missing'] }, 4_000)
  if ('staleInc' in after) throw new Error('the room is gone')
  expect([...after.cells].map(([key, value]) => [key, new TextDecoder().decode(value)])).toEqual([
    ['a', 'a2'],
    ['n', JSON.stringify({ value: 3, changedAt: 3_000 })],
  ])
})
