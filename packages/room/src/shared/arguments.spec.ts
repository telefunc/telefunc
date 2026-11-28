import { runInNewContext } from 'node:vm'
import { describe, expect, it } from 'vitest'
import { isRecord } from './arguments.js'

describe('isRecord', () => {
  it('takes a plain object, from this realm or another, and one with no prototype, and refuses any other object', () => {
    class Point {
      x = 1
    }
    const records = [{ a: 1 }, Object.create(null), runInNewContext('({ a: 1 })')].map(isRecord)
    const others = [[], new Point(), new Map(), new Date(0), null, 'text'].map(isRecord)
    expect({ records, others }).toEqual({
      records: [true, true, true],
      others: [false, false, false, false, false, false],
    })
  })
})
