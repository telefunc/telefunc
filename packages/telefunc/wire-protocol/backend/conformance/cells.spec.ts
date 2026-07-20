// I3 — compareExchangeCells applies all mutations or none, and success implies the head precondition
// held at apply time. The read side is covered here too: fixed read sets, prefix selection, the revision
// that covers exactly the returned set, and the logical-TTL rule that a silent expiry is never a
// revisioned mutation.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  bytes,
  cellsOf,
  CONTROL,
  enterClosing,
  finalizeClose,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  text,
} from './scenario.js'

for (const harness of installedBackends) {
  describe(`cells — ${harness.name}`, () => {
    let fx: BackendFixture
    let roomId: string
    let inc: string

    beforeEach(async () => {
      fx = await harness.create()
      roomId = nextId('room')
      const opened = await openRoom(fx.backend, roomId)
      inc = opened.inc
    })

    afterEach(async () => {
      await fx.dispose()
    })

    const read = async (sel: { keys: string[] } | { prefix: string }) =>
      cellsOf(await fx.backend.readCells(roomId, inc, sel))

    const write = async (mutations: Parameters<typeof fx.backend.compareExchangeCells>[3], revision?: string) =>
      fx.backend.compareExchangeCells(roomId, inc, revision ?? (await read({ prefix: '' })).revision, mutations)

    it('starts empty with a revision that a CX accepts', async () => {
      const initial = await read({ prefix: '' })
      expect(initial.cells.size).toBe(0)
      expect(await write([{ key: 'member/alice', set: { bytes: bytes('A') } }], initial.revision)).toBe('committed')
    })

    it('applies a mixed mutation batch atomically and changes the revision', async () => {
      expect(
        await write([
          { key: 'member/alice', set: { bytes: bytes('A') } },
          { key: 'member/bob', set: { bytes: bytes('B') } },
          { key: 'marker/seen', set: { bytes: bytes('1') } },
        ]),
      ).toBe('committed')
      const first = await read({ prefix: 'member/' })
      expect([...first.cells.keys()].sort()).toEqual(['member/alice', 'member/bob'])
      expect(text(first.cells.get('member/alice') as Uint8Array)).toBe('A')

      // a delete (set absent) and a set land together
      expect(
        await write([{ key: 'member/alice' }, { key: 'member/carol', set: { bytes: bytes('C') } }], first.revision),
      ).toBe('committed')
      const second = await read({ prefix: 'member/' })
      expect([...second.cells.keys()].sort()).toEqual(['member/bob', 'member/carol'])
      expect(second.revision).not.toBe(first.revision)
    })

    it('selects by explicit key set, omitting absent keys', async () => {
      await write([{ key: 'member/alice', set: { bytes: bytes('A') } }])
      const selected = await read({ keys: ['member/alice', 'member/nobody'] })
      expect([...selected.cells.keys()]).toEqual(['member/alice'])
    })

    it('conflicts on a stale revision and applies NOTHING', async () => {
      // KILLER: dropping the revision compare turns this red.
      const before = await read({ prefix: '' })
      expect(await write([{ key: 'first', set: { bytes: bytes('1') } }], before.revision)).toBe('committed')

      // a second writer still holding the pre-write revision
      expect(await write([{ key: 'second', set: { bytes: bytes('2') } }], before.revision)).toBe('conflict')
      const after = await read({ prefix: '' })
      expect([...after.cells.keys()]).toEqual(['first'])
    })

    it('serializes two writers off one read: the first commits, the second conflicts', async () => {
      const shared = (await read({ prefix: '' })).revision
      const results = await Promise.all([
        write([{ key: 'a', set: { bytes: bytes('1') } }], shared),
        write([{ key: 'b', set: { bytes: bytes('2') } }], shared),
      ])
      expect(results.filter((result) => result === 'committed')).toHaveLength(1)
      expect(results.filter((result) => result === 'conflict')).toHaveLength(1)
    })

    it('is stale-inc for a foreign incarnation', async () => {
      const revision = (await read({ prefix: '' })).revision
      expect(await fx.backend.compareExchangeCells(roomId, 'inc-that-never-was', revision, [])).toBe('stale-inc')
      expect(await fx.backend.readCells(roomId, 'inc-that-never-was', { prefix: '' })).toEqual({ staleInc: true })
    })

    it('refuses cell writes while closing and after closed, but still serves reads while closing', async () => {
      await write([{ key: 'member/alice', set: { bytes: bytes('A') } }])
      const current = await readHeadOrThrow(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, current)

      // reads still work — the closer's tail needs them
      const whileClosing = await read({ prefix: 'member/' })
      expect(text(whileClosing.cells.get('member/alice') as Uint8Array)).toBe('A')
      // writes do not
      expect(await write([{ key: 'member/bob', set: { bytes: bytes('B') } }], whileClosing.revision)).toBe('stale-inc')

      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      expect(await fx.backend.compareExchangeCells(roomId, inc, whileClosing.revision, [])).toBe('stale-inc')
      expect(await fx.backend.readCells(roomId, inc, { prefix: '' })).toEqual({ staleInc: true })
    })

    it('hides a lapsed TTL cell without treating the silent expiry as a revisioned mutation', async () => {
      const before = await read({ prefix: '' })
      expect(
        await write(
          [
            { key: 'member/alice', set: { bytes: bytes('A'), ttlMs: 30_000 } },
            { key: 'member/bob', set: { bytes: bytes('B') } },
          ],
          before.revision,
        ),
      ).toBe('committed')
      const seeded = await read({ prefix: 'member/' })
      expect(seeded.cells.size).toBe(2)

      fx.advanceAuthority(30_001)
      const expired = await read({ prefix: 'member/' })
      expect([...expired.cells.keys()]).toEqual(['member/bob'])
      // the revision is unchanged: a reader's CX built on the pre-expiry revision still applies
      expect(expired.revision).toBe(seeded.revision)
      expect(await write([{ key: 'member/carol', set: { bytes: bytes('C') } }], seeded.revision)).toBe('committed')
    })

    it('accepts an empty mutation batch as a revision bump', async () => {
      const before = await read({ prefix: '' })
      expect(await write([], before.revision)).toBe('committed')
      expect((await read({ prefix: '' })).revision).not.toBe(before.revision)
    })
  })
}
