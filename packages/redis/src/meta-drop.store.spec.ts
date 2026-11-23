// A replaced meta generation whose drop the store refused, on the oldest and the newest Redis and Valkey Telefunc
// supports: it stays marked, with the flag that a drop is pending, and the meta writes after it take the drop forward.
// A healthy write pays no request for it.
import type { Redis } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc'
import { Room } from '@telefunc/room'
import { getRoomPlane, type CellSelector } from '@telefunc/room/backend'
import { endReached, reached, runs, STORES } from '../test/stores.js'
import { RedisRoomDriver } from './RedisRoomDriver.js'

afterAll(() => endReached())
afterEach(() => vi.restoreAllMocks())

/** The cells of a replaced generation's mark, of the pending flag and of a generation's meta, from
 *  packages/room/src/server/store/cells.ts. */
const META_DROP_CELL_PREFIX = 'meta:drop:'
const META_DROP_PENDING_CELL_KEY = 'meta:drop:pending'
const META_GEN_CELL_KEY = 'meta:gen'
const META_KEY_CELL_PREFIX = 'meta:k:'

const refused = () => new BackendError({ code: 'denied', outcome: 'not-run', message: 'the store refused the read' })
const isMarksRead = (selector: CellSelector) => 'prefix' in selector && selector.prefix === META_DROP_CELL_PREFIX
const outcomeOf = (call: Promise<unknown>) =>
  call.then(
    () => 'resolved',
    (error: unknown) => (error instanceof BackendError ? error.code : error),
  )

const gate = () => {
  let open = () => {}
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  return { open, opened }
}

/** A room whose meta was written once, and what the store holds of its meta: the marks, whether a drop is flagged
 *  pending, and the generations kept. */
async function roomWithMeta(admin: Redis, id: string) {
  await Room.create(id, { emptyTimeout: Infinity, departureTimeout: Infinity })
  await Room.setMeta(id, { earlier: true })
  const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
  if (values === undefined) throw new Error(`room ${id} has no incarnation`)
  return async () => {
    const keys = await admin.hkeys(values)
    const generations = keys
      .filter((key) => key.startsWith(META_KEY_CELL_PREFIX))
      .map((key) => key.slice(META_KEY_CELL_PREFIX.length).split(':')[0])
    return {
      marks: keys.filter((key) => key.startsWith(META_DROP_CELL_PREFIX) && key !== META_DROP_PENDING_CELL_KEY).length,
      pending: keys.includes(META_DROP_PENDING_CELL_KEY),
      generations: new Set(generations).size,
    }
  }
}

/** Each read of room `id`'s cells, once taken, goes to `step` before its answer, with the count of the marks' reads so
 *  far: a read `step` waits in answers what the store held when it was taken. */
function onReads(id: string, step: (selector: CellSelector, marksRead: number) => Promise<void>): void {
  const { readCells } = RedisRoomDriver.prototype
  let marksRead = 0
  vi.spyOn(RedisRoomDriver.prototype, 'readCells').mockImplementation(async function (
    this: RedisRoomDriver,
    roomId,
    inc,
    selector,
  ) {
    const read = await readCells.call(this, roomId, inc, selector)
    if (roomId === id) await step(selector, isMarksRead(selector) ? ++marksRead : marksRead)
    return read
  })
}

const clean = { marks: 0, pending: false, generations: 1 }
const left = { marks: 1, pending: true, generations: 2 }

describe.runIf(runs)('a meta replace whose drop of the old generation the store refused', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a merge after it drops the old generation, its mark and the flag`, async () => {
      const admin = await reached(store)
      const id = `meta-drop-merge-${i}`
      const held = await roomWithMeta(admin, id)
      onReads(id, async (selector) => {
        if (isMarksRead(selector)) throw refused()
      })
      const replaced = await outcomeOf(Room.setMeta(id, { replacement: true }))
      vi.restoreAllMocks()
      const afterRefusal = await held()
      await Room.updateMeta(id, { merged: true })
      const afterMerge = await held()
      const meta = (await Room.get(id)).meta
      admin.disconnect()

      expect({ replaced, afterRefusal, afterMerge, meta }).toEqual({
        replaced: 'denied',
        afterRefusal: left,
        afterMerge: clean,
        meta: { replacement: true, merged: true },
      })
    }, 60_000)

    it(`${store.name}: a replace another replace overtook, both having read no drop pending, drops what the other left`, async () => {
      const admin = await reached(store)
      const id = `meta-drop-overtaken-${i}`
      const held = await roomWithMeta(admin, id)
      const read = gate()
      const release = gate()
      let firstMetaRead = true
      onReads(id, async (selector, marksRead) => {
        if ('keys' in selector && selector.keys.includes(META_GEN_CELL_KEY) && firstMetaRead) {
          firstMetaRead = false
          read.open()
          await release.opened
        }
        if (isMarksRead(selector) && marksRead === 1) throw refused()
      })
      // The overtaken replace read the meta, and waits while the other replace commits and its drop is refused.
      const overtaken = outcomeOf(Room.setMeta(id, { overtaken: true }))
      await read.opened
      const overtaking = await outcomeOf(Room.setMeta(id, { overtaking: true }))
      const beforeOvertaken = await held()
      release.open()
      const outcome = await overtaken
      const after = await held()
      const meta = (await Room.get(id)).meta
      admin.disconnect()

      expect({ overtaking, beforeOvertaken, overtaken: outcome, after, meta }).toEqual({
        overtaking: 'denied',
        beforeOvertaken: left,
        overtaken: 'resolved',
        after: clean,
        meta: { overtaking: true },
      })
    }, 60_000)

    it(`${store.name}: a drop that read the marks before a newer replace marked finishes its own and leaves the newer mark and flag`, async () => {
      const admin = await reached(store)
      const id = `meta-drop-newer-mark-${i}`
      const held = await roomWithMeta(admin, id)
      onReads(id, async (selector) => {
        if (isMarksRead(selector)) throw refused()
      })
      await outcomeOf(Room.setMeta(id, { first: true }))
      vi.restoreAllMocks()
      // The merge's drop read the marks, and waits while a newer replace marks, its own drop refused.
      const read = gate()
      const release = gate()
      let mergeSettled = false
      onReads(id, async (selector, marksRead) => {
        if (!isMarksRead(selector)) return
        if (marksRead === 1 && !mergeSettled) {
          read.open()
          await release.opened
        } else if (marksRead === 2) throw refused()
      })
      const merging = outcomeOf(Room.updateMeta(id, { merged: true })).then((outcome) => {
        mergeSettled = true
        return outcome
      })
      await Promise.race([read.opened, merging])
      const newer = await outcomeOf(Room.setMeta(id, { second: true }))
      release.open()
      const merged = await merging
      vi.restoreAllMocks()
      const afterDrop = await held()
      await Room.updateMeta(id, { again: true })
      const afterNextMerge = await held()
      admin.disconnect()

      expect({ newer, merged, afterDrop, afterNextMerge }).toEqual({
        newer: 'denied',
        merged: 'resolved',
        afterDrop: left,
        afterNextMerge: clean,
      })
    }, 60_000)
  }
})

describe.runIf(runs)('a warm meta write with no drop pending', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a replace makes seven requests, and a merge five`, async () => {
      const admin = await reached(store)
      const id = `meta-drop-count-${i}`
      await roomWithMeta(admin, id)
      await Room.setMeta(id, { warm: true })
      await Room.updateMeta(id, { warm: false })
      const requests = async (write: () => Promise<void>) => {
        await admin.config('RESETSTAT')
        await write()
        const stats = await admin.info('commandstats')
        return Number(/^cmdstat_evalsha:calls=(\d+)/m.exec(stats)?.[1])
      }
      const replace = await requests(() => Room.setMeta(id, { replaced: true }))
      const merge = await requests(() => Room.updateMeta(id, { merged: true }))
      admin.disconnect()

      expect({ replace, merge }).toEqual({ replace: 7, merge: 5 })
    }, 60_000)
  }
})
