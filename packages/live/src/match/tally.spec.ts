import { describe, expect, it } from 'vitest'
import type { Alias } from './readShape.js'
import { Tally } from './tally.js'
import type { TallyShape } from './tallyShape.js'
import type { Column } from './values.js'

const userId: Column = {
  name: 'user_id',
  category: 'N',
  type: 'int4',
  sqlType: 'pg_catalog.int4',
  base: ['pg_catalog', 'int4'],
  orderableText: true,
  deterministic: true,
}
const todos = { name: 'todos' } as Alias

describe('Tally', () => {
  // `SELECT user_id FROM todos GROUP BY user_id HAVING count(*) > 1`: user 1 has 5 todos and is in the result. A
  // todo of user 1 is inserted after the read and before its counts query, which counts it: 6. Its change arrives
  // after them (from another server, or late), and can't tell it was counted. Then 5 deletes leave user 1 one todo.
  it('invalidates a group that a change counted already can no longer keep in the result', () => {
    const shape: TallyShape = {
      sources: [{ alias: todos, columns: [userId], groupColumns: 1 }],
      rule: {
        kind: 'groups',
        grouped: true,
        groupHidden: [0],
        outputs: [],
        otherOutputs: false,
        having: [{ aggregate: { kind: 'count' }, operator: '>', operand: 0 }],
      },
    }
    const tally = Tally.of(shape, ['1'], [['1']], [[JSON.stringify(['0', '1']), '6']])!
    const row = { user_id: '1' }
    const invalidated = [tally.changed(todos, undefined, row, true, false)]
    for (let i = 0; i < 5; i++) invalidated.push(tally.changed(todos, row, undefined, true, false))
    expect(invalidated.some(Boolean)).toBe(true)
  })
})
