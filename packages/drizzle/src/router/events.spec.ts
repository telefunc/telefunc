// T5.G5 — the positioned change envelope: an exact configurable size bound, every ordinary SQL
// value round-trips (BigInt, Date, Uint8Array, NULL, composite PK) via explicit type tags, only
// cyclic / function / true-oversize payloads degrade to a coarse resync (never a silent drop),
// and a heartbeat frame causes no apply/notify while obeying the G3/G4 cursor rule.

import { describe, expect, it } from 'vitest'
import type { ApplyOutcome } from '../graph/liveGraph.js'
import { type RoutableGraph, createRouter } from './changeRouter.js'
import {
  type ChangeBatch,
  type TableChange,
  DEFAULT_MAX_BATCH_BYTES,
  deserializeBatch,
  serializeBatch,
} from './events.js'

describe('T5.G5 — event-shape precision', () => {
  it('round-trips ordinary SQL values: BigInt, Date, Uint8Array, NULL, composite PK', () => {
    const when = new Date('2020-01-02T03:04:05.678Z')
    const big = BigInt('100000000000000000000') // beyond Number.MAX_SAFE_INTEGER
    const input: ChangeBatch = {
      sourceId: 's',
      position: 42,
      predecessor: 41,
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
    expect(back.position).toBe(42)
    expect(back.predecessor).toBe(41)
  })

  it('degrades ONLY cyclic / function / true-oversize payloads (typed failure, never a silent drop)', () => {
    const cyclicRow: Record<string, unknown> = {}
    cyclicRow.self = cyclicRow
    const cyclic: ChangeBatch = {
      sourceId: 's',
      position: 1,
      predecessor: null,
      changes: [{ table: 't', kind: 'insert', new: cyclicRow }],
    }
    expect(serializeBatch(cyclic)).toEqual({ ok: false, reason: 'cyclic' })

    const fn: ChangeBatch = {
      sourceId: 's',
      position: 1,
      predecessor: null,
      changes: [{ table: 't', kind: 'insert', new: { f: () => 0 } }],
    }
    expect(serializeBatch(fn)).toEqual({ ok: false, reason: 'function' })

    const large: ChangeBatch = {
      sourceId: 's',
      position: 1,
      predecessor: null,
      changes: [{ table: 't', kind: 'insert', new: { s: 'x'.repeat(1000) } }],
    }
    expect(serializeBatch(large, 10)).toEqual({ ok: false, reason: 'oversize' }) // exact configured bound

    expect(DEFAULT_MAX_BATCH_BYTES).toBe(1_000_000) // exact default bound
    expect(serializeBatch(large).ok).toBe(true) // fits the default bound
  })
})

describe('T5.G5 — heartbeat frames', () => {
  it('cause no apply/notify but obey the cursor rule (liveness only)', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const applyLog: TableChange[][] = []
    const graph: RoutableGraph = {
      tables: ['users'],
      apply: (changes): ApplyOutcome => {
        applyLog.push(changes)
        return { invalidated: true }
      },
      notifyKeys: () => ['I'],
      resync: () => {},
    }
    router.register(graph)

    router.ingest({ sourceId: 's', position: 1, predecessor: null, heartbeat: true, changes: [] })
    expect(applyLog.length).toBe(0) // no apply
    expect(notified).toEqual([]) // no notify
    expect(router.inspect().counters.accepted).toBe(1) // liveness accepted → cursor advanced

    // a data frame linked to the heartbeat's position is accepted (heartbeat set the cursor to 1)
    router.ingest({
      sourceId: 's',
      position: 2,
      predecessor: 1,
      changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }],
    })
    expect(applyLog.length).toBe(1)
  })
})
