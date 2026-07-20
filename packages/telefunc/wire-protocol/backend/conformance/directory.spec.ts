// I10 — the directory is best-effort: it may lag or lose entries, a delete with a stale tag is a no-op,
// and list staleness is repaired by the caller through readHead. The verbs exist iff
// capabilities.directory, so the whole module is capability-gated.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  bytes,
  CONTROL,
  enterClosing,
  finalizeClose,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
} from './scenario.js'

for (const harness of installedBackends) {
  describe(`directory — ${harness.name}`, () => {
    let fx: BackendFixture

    beforeEach(async () => {
      fx = await harness.create()
    })

    afterEach(async () => {
      await fx.dispose()
    })

    it('puts, lists and tag-guards deletes', async () => {
      if (!fx.backend.capabilities.directory) return
      const prefix = `${nextId('dir')}/`
      const roomId = `${prefix}room`
      await fx.backend.directoryPut(roomId, 'inc-1')
      expect((await fx.backend.directoryList(prefix)).entries).toEqual([{ roomId, incTag: 'inc-1' }])

      // a delete carrying a stale tag is a no-op
      await fx.backend.directoryDelete(roomId, 'inc-0')
      expect((await fx.backend.directoryList(prefix)).entries).toEqual([{ roomId, incTag: 'inc-1' }])

      await fx.backend.directoryDelete(roomId, 'inc-1')
      expect((await fx.backend.directoryList(prefix)).entries).toEqual([])
    })

    it('replaces the tag on a re-put', async () => {
      if (!fx.backend.capabilities.directory) return
      const prefix = `${nextId('dir')}/`
      const roomId = `${prefix}room`
      await fx.backend.directoryPut(roomId, 'inc-1')
      await fx.backend.directoryPut(roomId, 'inc-2')
      expect((await fx.backend.directoryList(prefix)).entries).toEqual([{ roomId, incTag: 'inc-2' }])
      // the superseded tag no longer authorizes a delete
      await fx.backend.directoryDelete(roomId, 'inc-1')
      expect((await fx.backend.directoryList(prefix)).entries).toHaveLength(1)
    })

    it('scopes a listing to its prefix', async () => {
      if (!fx.backend.capabilities.directory) return
      const mine = `${nextId('dir')}/`
      const other = `${nextId('dir')}/`
      await fx.backend.directoryPut(`${mine}a`, 'inc-a')
      await fx.backend.directoryPut(`${other}b`, 'inc-b')
      const listed = await fx.backend.directoryList(mine)
      expect(listed.entries.map((entry) => entry.roomId)).toEqual([`${mine}a`])
    })

    it('pages through a large listing with a cursor and returns every entry once', async () => {
      if (!fx.backend.capabilities.directory) return
      const prefix = `${nextId('dir')}/`
      const total = 250
      for (let n = 0; n < total; n++) {
        await fx.backend.directoryPut(`${prefix}${String(n).padStart(4, '0')}`, `inc-${n}`)
      }

      const seen: string[] = []
      let cursor: string | undefined
      let pages = 0
      do {
        const page = await fx.backend.directoryList(prefix, cursor)
        seen.push(...page.entries.map((entry) => entry.roomId))
        cursor = page.cursor
        pages++
        expect(pages).toBeLessThan(20) // a cursor that never terminates is a failure, not a hang
      } while (cursor !== undefined)

      expect(seen).toHaveLength(total)
      expect(new Set(seen).size).toBe(total)
      expect([...seen]).toEqual([...seen].sort())
    })

    it('may serve a stale entry that the caller repairs through readHead', async () => {
      if (!fx.backend.capabilities.directory) return
      const prefix = `${nextId('dir')}/`
      const roomId = `${prefix}room`
      const { inc, head } = await openRoom(fx.backend, roomId)
      await fx.backend.directoryPut(roomId, inc)

      // the room closes; the projection cleanup has not run yet
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

      const stale = await fx.backend.directoryList(prefix)
      expect(stale.entries).toEqual([{ roomId, incTag: inc }])
      // the caller repairs by consulting the head, which is always consistent
      expect((await readHeadOrThrow(fx.backend, roomId)).currentInc).toBeNull()

      await fx.backend.directoryDelete(roomId, inc)
      expect((await fx.backend.directoryList(prefix)).entries).toEqual([])
    })
  })
}
