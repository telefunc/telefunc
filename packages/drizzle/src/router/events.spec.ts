// T5.G5 — the change envelope + value codec: every ordinary SQL value round-trips (BigInt, Date,
// Uint8Array, NULL, composite PK) via explicit type tags, and only genuinely unserializable payloads
// (cyclic / function) degrade to a typed failure — never a silent corruption. Ordering / positioning
// is the source's own concern, not the shared shape.

import { describe, expect, it } from 'vitest'
import { type ChangeBatch, deserializeBatch, serializeBatch } from './events.js'

describe('T5.G5 — value-codec fidelity', () => {
  it('round-trips ordinary SQL values: BigInt, Date, Uint8Array, NULL, composite PK', () => {
    const when = new Date('2020-01-02T03:04:05.678Z')
    const big = BigInt('100000000000000000000') // beyond Number.MAX_SAFE_INTEGER
    const input: ChangeBatch = {
      changes: [
        {
          table: 'rows',
          kind: 'insert',
          new: { big, when, bytes: new Uint8Array([1, 2, 255]), nothing: null, a: 1, b: 'x' },
          key: { a: 1, b: 'x' }, // composite PK
        },
      ],
    }
    const encoded = serializeBatch(input)
    expect(encoded.ok).toBe(true)
    const back = deserializeBatch((encoded as { wire: string }).wire)
    const row = back.changes[0]!.new!
    expect(row.big).toBe(big)
    expect(row.when).toBeInstanceOf(Date)
    expect((row.when as Date).getTime()).toBe(when.getTime())
    expect(row.bytes).toBeInstanceOf(Uint8Array)
    expect([...(row.bytes as Uint8Array)]).toEqual([1, 2, 255])
    expect(row.nothing).toBeNull()
    expect(back.changes[0]!.key).toEqual({ a: 1, b: 'x' })
  })

  it('degrades ONLY cyclic / function payloads (typed failure, never a silent corruption)', () => {
    const cyclicRow: Record<string, unknown> = {}
    cyclicRow.self = cyclicRow
    expect(serializeBatch({ changes: [{ table: 't', kind: 'insert', new: cyclicRow }] })).toEqual({
      ok: false,
      reason: 'cyclic',
    })
    expect(serializeBatch({ changes: [{ table: 't', kind: 'insert', new: { f: () => 0 } }] })).toEqual({
      ok: false,
      reason: 'function',
    })
    // A large-but-serializable payload succeeds — there is no size cap in the shared shape.
    expect(serializeBatch({ changes: [{ table: 't', kind: 'insert', new: { s: 'x'.repeat(1000) } }] }).ok).toBe(true)
  })
})
