// The wire format has one job: a row that goes in comes back out as the same VALUE, and anything that
// cannot be trusted degrades to coarse instead of to a wrong row. Both halves are asserted here — a codec
// tested only on the happy path is how a `bigserial` id becomes a string in production.

import { describe, expect, it } from 'vitest'
import { CHANGE_CODEC_VERSION, type ChangeEnvelope, decodeChangePayload, encodeChangePayload } from './changeCodec.js'
import type { TableChange } from '../router/events.js'

const roundTrip = (changes: TableChange[]): TableChange[] => {
  const decoded = decodeChangePayload(
    encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin: 'origin-a', changes }),
  )
  expect(decoded).toBeDefined()
  return (decoded as { changes: TableChange[] }).changes
}

describe('change codec — SQL values survive a serializing transport', () => {
  it('carries the value domain a row is actually made of', () => {
    const row = {
      big: BigInt('100000000000000000000'), // a bigserial id — plain JSON throws on this
      when: new Date('2020-01-02T03:04:05.678Z'),
      bytes: new Uint8Array([1, 2, 250]),
      buf: Buffer.from([9, 8, 7]),
      nothing: null,
      missing: undefined,
      notANumber: Number.NaN,
      infinite: Number.POSITIVE_INFINITY,
      negInfinite: Number.NEGATIVE_INFINITY,
      nested: { deep: [BigInt(1), new Date(0)] },
    }
    const [change] = roundTrip([{ table: 'users', kind: 'insert', new: row }])
    const out = change!.new as typeof row

    expect(out.big).toBe(BigInt('100000000000000000000'))
    expect(out.when).toBeInstanceOf(Date)
    expect(out.when.getTime()).toBe(row.when.getTime())
    expect(out.nothing).toBeNull()
    expect(out.notANumber).toBeNaN()
    expect(out.infinite).toBe(Number.POSITIVE_INFINITY)
    expect(out.negInfinite).toBe(Number.NEGATIVE_INFINITY)
    expect('missing' in out).toBe(true) // an explicit undefined column is not the same as an absent one
    expect(out.nested.deep[0]).toBe(BigInt(1))
    expect(out.nested.deep[1]).toBeInstanceOf(Date)

    // Byte arrays are the one type the underlying serializer drops; both spellings come back as bytes.
    expect(out.bytes).toBeInstanceOf(Uint8Array)
    expect([...out.bytes]).toEqual([1, 2, 250])
    expect(out.buf).toBeInstanceOf(Uint8Array)
    expect([...out.buf]).toEqual([9, 8, 7])
  })

  it('carries a composite retraction key and a coarse marker unchanged', () => {
    const changes = roundTrip([
      { table: 'memberships', kind: 'delete', key: { userId: BigInt(7), teamId: new Date(5) } },
      { table: 'audit', kind: 'coarse' },
    ])
    expect(changes[0]!.key).toEqual({ userId: BigInt(7), teamId: new Date(5) })
    expect(changes[1]).toEqual({ table: 'audit', kind: 'coarse' })
  })

  it('round-trips a coarse-all envelope', () => {
    const payload = encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin: 'origin-a', coarseAll: true })
    expect(decodeChangePayload(payload)).toEqual({ version: CHANGE_CODEC_VERSION, origin: 'origin-a', coarseAll: true })
  })

  it('preserves origin — the publisher drops its own echo by it', () => {
    const decoded = decodeChangePayload(
      encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin: 'db-42', changes: [] }),
    )
    expect(decoded!.origin).toBe('db-42')
  })
})

describe('change codec — anything untrustworthy decodes to nothing (the caller coarsens)', () => {
  const rejected = (payload: string) => expect(decodeChangePayload(payload)).toBeUndefined()

  it('rejects an unknown codec version rather than interpreting it', () => {
    const future = encodeChangePayload({ version: CHANGE_CODEC_VERSION + 1, origin: 'a', coarseAll: true })
    rejected(future)
  })

  it('rejects malformed text', () => {
    rejected('{not json at all')
    rejected('')
  })

  it('rejects a well-formed payload that is not an envelope', () => {
    rejected(JSON.stringify({ hello: 'world' }))
    rejected(JSON.stringify({ version: CHANGE_CODEC_VERSION })) // no origin
    rejected(JSON.stringify({ version: CHANGE_CODEC_VERSION, origin: 'a' })) // neither changes nor coarseAll
    rejected(JSON.stringify(null))
  })

  it('a valid envelope IS accepted — so the rejections above are not vacuous', () => {
    const ok: ChangeEnvelope = { version: CHANGE_CODEC_VERSION, origin: 'a', changes: [] }
    expect(decodeChangePayload(encodeChangePayload(ok))).toBeDefined()
  })
})
