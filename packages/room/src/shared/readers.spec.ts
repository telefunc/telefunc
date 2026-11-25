import { expect, it } from 'vitest'
import { ctrlEnvelopeOf } from './readers.js'

it('reads two meta events from one parsed value as two events of their own, and leaves the value as it was', () => {
  const fields = { __r: 'p-meta', id: 'member', seq: 7, attrs: { status: 'away' } }
  const first = ctrlEnvelopeOf(fields)
  const second = ctrlEnvelopeOf(fields)
  Object.assign(first, { seq: 8 })
  expect({ first, second, fields, own: first !== fields && second !== fields }).toEqual({
    first: { __r: 'p-meta', id: 'member', seq: 8, attrs: { status: 'away' } },
    second: { __r: 'p-meta', id: 'member', seq: 7, attrs: { status: 'away' } },
    fields: { __r: 'p-meta', id: 'member', seq: 7, attrs: { status: 'away' } },
    own: true,
  })
})
