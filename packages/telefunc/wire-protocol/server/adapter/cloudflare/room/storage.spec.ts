import { DatabaseSync, type SQLInputValue } from 'node:sqlite'
import { expect, test } from 'vitest'
import { directoryList, directoryPut, initSchema } from './storage.js'

/** Cloudflare's SQL storage over node:sqlite, recording the plan of each directory page read. */
function directoryStorage() {
  const db = new DatabaseSync(':memory:')
  const plans: string[] = []
  const sql = {
    exec(query: string, ...bindings: SQLInputValue[]) {
      if (bindings.length === 0) {
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
  const { sql, plans } = directoryStorage()
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

test("a directory pages a prefix's rooms from the cursor it hands back", () => {
  const { sql } = directoryStorage()
  const rooms = Array.from({ length: 150 }, (_, index) => `room:${String(index).padStart(3, '0')}`)
  for (const roomId of ['roo', ...rooms, 'roon']) directoryPut(sql, roomId, 'tag')
  const first = directoryList(sql, 'room:')
  const second = directoryList(sql, 'room:', first.cursor)
  expect([...first.entries, ...second.entries].map((entry) => entry.roomId)).toEqual(rooms)
  expect(second.cursor).toBeUndefined()
})
