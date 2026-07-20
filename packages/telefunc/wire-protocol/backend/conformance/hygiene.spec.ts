// Hygiene: the whole close composition end to end (edit 1 — the head-null close through
// closing→closed(currentInc:null)→dropGeneration), the pause/recreate/resume suite that proves a
// resumed janitor step can never touch a freshly recreated incarnation, generation isolation, and
// dispose.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  binaryLane,
  bytes,
  cellsOf,
  CLOSE_LEASE_MS,
  collector,
  CONTROL,
  enterClosing,
  finalizeClose,
  isStale,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  takeoverClose,
  text,
  TOMBSTONE_TTL_MS,
} from './scenario.js'

for (const harness of installedBackends) {
  describe(`hygiene — ${harness.name}`, () => {
    let fx: BackendFixture

    beforeEach(async () => {
      fx = await harness.create()
    })

    afterEach(async () => {
      await fx.dispose()
    })

    // Seed a generation with a cell, a retained lane and an advanced order domain.
    const seedGeneration = async (roomId: string, inc: string, marker: string): Promise<void> => {
      const read = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' }))
      expect(
        await fx.backend.compareExchangeCells(roomId, inc, read.revision, [
          { key: 'member/alice', set: { bytes: bytes(marker) } },
        ]),
      ).toBe('committed')
      accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(marker), { retain: true }))
      accepted(await fx.backend.commitLane(roomId, inc, binaryLane('alice', 'cam'), bytes(marker), { retain: true }))
    }

    it('runs the whole close composition to a head-null tombstone and a dropped generation', async () => {
      const roomId = nextId('room')
      const { inc, head } = await openRoom(fx.backend, roomId)
      await seedGeneration(roomId, inc, 'gen-1')
      await fx.backend.directoryPut(roomId, inc)

      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      expect(closing.state).toBe('closing')
      expect(closing.closeLease?.id).toBe(leaseId)
      expect(closing.closeLease?.until).toBe(fx.authorityNow() + CLOSE_LEASE_MS)

      const event = accepted(
        await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }),
      )
      await event.delivery

      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      expect(tombstone.state).toBe('closed')
      expect(tombstone.currentInc).toBeNull()
      expect(tombstone.closeLease).toBeUndefined() // the lease is present iff the head is closing

      await fx.backend.dropGeneration(roomId, inc)
      await fx.backend.directoryDelete(roomId, inc)
      expect(await fx.backend.listGenerations(roomId)).toEqual([])
      expect(await fx.backend.readRetained(roomId, inc, SEMANTIC)).toBeNull()
      expect((await fx.backend.directoryList(roomId)).entries).toEqual([])
    })

    it('pause / recreate / resume: a resumed dropGeneration never touches the new incarnation', async () => {
      // KILLER: a generation sweep that resolves by room rather than by (room, inc) turns this red.
      const roomId = nextId('room')
      const { inc: oldInc, head } = await openRoom(fx.backend, roomId)
      await seedGeneration(roomId, oldInc, 'gen-1')

      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, oldInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

      // the closer is PAUSED here — finalization happened, dropGeneration(oldInc) has not run yet
      const { inc: newInc } = await openRoom(fx.backend, roomId, { prior: tombstone })
      await seedGeneration(roomId, newInc, 'gen-2')
      expect((await fx.backend.listGenerations(roomId)).sort()).toEqual([oldInc, newInc].sort())

      // the paused step now RESUMES against the old incarnation
      await fx.backend.dropGeneration(roomId, oldInc)

      expect(await fx.backend.listGenerations(roomId)).toEqual([newInc])
      const survivors = cellsOf(await fx.backend.readCells(roomId, newInc, { prefix: '' }))
      expect(text(survivors.cells.get('member/alice') as Uint8Array)).toBe('gen-2')
      const retained = await fx.backend.readRetained(roomId, newInc, SEMANTIC)
      expect(text(retained?.payload ?? bytes(''))).toBe('gen-2')
      expect(accepted(await fx.backend.commitLane(roomId, newInc, SEMANTIC, bytes('still-live'))).seq).toBeGreaterThan(
        0,
      )
    })

    it('is a resumable no-op when the generation is already gone', async () => {
      const roomId = nextId('room')
      const { inc, head } = await openRoom(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

      await fx.backend.dropGeneration(roomId, inc)
      await fx.backend.dropGeneration(roomId, inc) // the janitor may replay its step
      await fx.backend.dropGeneration(nextId('room'), 'never-existed')
      expect(await fx.backend.listGenerations(roomId)).toEqual([])
    })

    it('lists every surviving generation and refuses only the current one', async () => {
      const roomId = nextId('room')
      const { inc: first, head } = await openRoom(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, first, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      const { inc: second } = await openRoom(fx.backend, roomId, { prior: tombstone })

      expect((await fx.backend.listGenerations(roomId)).sort()).toEqual([first, second].sort())
      await expect(fx.backend.dropGeneration(roomId, second)).rejects.toThrow()
      await fx.backend.dropGeneration(roomId, first)
      expect(await fx.backend.listGenerations(roomId)).toEqual([second])
    })

    it('recovers a room abandoned in closing, then recreates it — no path depends on the original closer', async () => {
      const roomId = nextId('room')
      const { inc, head } = await openRoom(fx.backend, roomId)
      await seedGeneration(roomId, inc, 'gen-1')
      // the closer crashes immediately after open→closing
      const { head: closing } = await enterClosing(fx.backend, roomId, head)

      fx.advanceAuthority(CLOSE_LEASE_MS + 1)
      const recovery = await takeoverClose(fx.backend, roomId, closing)
      const fresh = okHead(recovery.result)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: recovery.leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, fresh, recovery.leaseId))
      await fx.backend.dropGeneration(roomId, inc)

      expect(await fx.backend.listGenerations(roomId)).toEqual([])
      const recreated = await openRoom(fx.backend, roomId, { prior: tombstone })
      expect(recreated.head.state).toBe('open')
      expect(accepted(await fx.backend.commitLane(roomId, recreated.inc, SEMANTIC, bytes('alive'))).seq).toBe(1)
    })

    it('isolates generations: cells, retained, order and subscriptions all restart', async () => {
      const roomId = nextId('room')
      const { inc: oldInc, head } = await openRoom(fx.backend, roomId)
      await seedGeneration(roomId, oldInc, 'gen-1')
      const oldOrder = accepted(await fx.backend.commitLane(roomId, oldInc, SEMANTIC, bytes('more')))
      expect(oldOrder.seq).toBeGreaterThan(1)

      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, oldInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      const { inc: newInc } = await openRoom(fx.backend, roomId, { prior: tombstone })

      expect(cellsOf(await fx.backend.readCells(roomId, newInc, { prefix: '' })).cells.size).toBe(0)
      expect(await fx.backend.listRetained(roomId, newInc)).toEqual([])
      expect(accepted(await fx.backend.commitLane(roomId, newInc, SEMANTIC, bytes('first'))).seq).toBe(1)
    })

    it('closes surviving subscriptions when their generation is dropped', async () => {
      const roomId = nextId('room')
      const { inc, head } = await openRoom(fx.backend, roomId)
      const latch = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
      await sub.ready

      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      expect(sub.state()).toBe('ready') // finalization alone does not tear the channel down

      await fx.backend.dropGeneration(roomId, inc)
      expect(sub.state()).toBe('closed')
      await sub.unsubscribe() // still idempotent afterwards
    })

    it('drops a lapsed tombstone through the delete path for a backend without native TTL', async () => {
      const roomId = nextId('room')
      const { inc, head } = await openRoom(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId, TOMBSTONE_TTL_MS))

      const janitor = await fx.backend.compareExchangeHead(roomId, { expect: { rev: tombstone.rev } }, { delete: true })
      expect('ok' in janitor && 'deleted' in janitor).toBe(true)
      expect(await fx.backend.readHead(roomId)).toBeNull()

      // and the id is genuinely fresh again
      const { inc: reborn } = await openRoom(fx.backend, roomId)
      expect(reborn).not.toBe(inc)
    })

    it('refuses to delete a head that is still closing', async () => {
      const roomId = nextId('room')
      const { head } = await openRoom(fx.backend, roomId)
      const { head: closing } = await enterClosing(fx.backend, roomId, head)
      await expect(
        fx.backend.compareExchangeHead(roomId, { expect: { rev: closing.rev } }, { delete: true }),
      ).rejects.toThrow()
      expect((await readHeadOrThrow(fx.backend, roomId)).state).toBe('closing')
    })

    it('rejects every operation after dispose', async () => {
      const roomId = nextId('room')
      const { inc } = await openRoom(fx.backend, roomId)
      await fx.backend.dispose()
      await expect(fx.backend.readHead(roomId)).rejects.toThrow()
      await expect(fx.backend.listGenerations(roomId)).rejects.toThrow()
      expect(
        isStale(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('x')).catch(() => ({ stale: true }))),
      ).toBe(true)
    })
  })
}
