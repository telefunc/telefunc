// A replaced meta generation whose drop the store refused stays marked, with the flag that a drop is pending. The meta
// writes after it take the drop forward: a merge that reads the flag, and a replace that another replace overtook, each
// once its own write settled, in a call of its own. On the memory store.
import { afterEach, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import { MemoryRoomDriver } from '../backend/MemoryRoomDriver.js'
import type { CellSelector } from '../backend/contract.js'
import { Room } from '../index.js'
import { META_DROP_CELL_PREFIX, META_DROP_PENDING_CELL_KEY, META_GEN_CELL_KEY } from './store/cells.js'

afterEach(() => vi.restoreAllMocks())

const META_KEY_CELL_PREFIX = 'meta:k:'

const refused = () => new BackendError({ code: 'denied', outcome: 'not-run', message: 'the store refused the read' })

const gate = () => {
  let open = () => {}
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  return { open, opened }
}

/** A room whose meta was written once, and its incarnation. */
async function roomWithMeta(name: string): Promise<{ id: string; inc: string }> {
  const id = `${name}-${crypto.randomUUID()}`
  await Room.create(id, { emptyTimeout: Infinity, departureTimeout: Infinity })
  await Room.setMeta(id, { earlier: true })
  const inc = (await getRoomPlane().direct.reads.readHead(id))?.currentInc
  if (inc == null) throw new Error(`room ${id} has no incarnation`)
  return { id, inc }
}

/** What the store holds of the meta: the marks, whether a drop is flagged pending, and the generations kept. */
async function held(id: string, inc: string): Promise<{ marks: number; pending: boolean; generations: number }> {
  const read = await getRoomPlane().direct.reads.readCells(id, inc, { prefix: 'meta:' })
  if (read.kind === 'stale-incarnation') throw new Error(`room ${id} closed`)
  const keys = [...read.cells.keys()]
  const generations = keys.filter((key) => key.startsWith(META_KEY_CELL_PREFIX)).map((key) => key.split(':')[2])
  return {
    marks: keys.filter((key) => key.startsWith(META_DROP_CELL_PREFIX) && key !== META_DROP_PENDING_CELL_KEY).length,
    pending: keys.includes(META_DROP_PENDING_CELL_KEY),
    generations: new Set(generations).size,
  }
}

const isMarksRead = (selector: CellSelector) => 'prefix' in selector && selector.prefix === META_DROP_CELL_PREFIX

/** Each read of the room's cells, once taken, goes to `step` before its answer, with the count of the marks' reads so
 *  far: a read `step` waits in answers what the store held when it was taken. */
function onReads(id: string, step: (selector: CellSelector, marksRead: number) => Promise<void>): void {
  const { readCells } = MemoryRoomDriver.prototype
  let marksRead = 0
  vi.spyOn(MemoryRoomDriver.prototype, 'readCells').mockImplementation(async function (
    this: MemoryRoomDriver,
    roomId,
    inc,
    selector,
  ) {
    const read = await readCells.call(this, roomId, inc, selector)
    if (roomId === id) await step(selector, isMarksRead(selector) ? ++marksRead : marksRead)
    return read
  })
}

const outcomeOf = (call: Promise<unknown>) =>
  call.then(
    () => 'resolved',
    (error: unknown) => (error instanceof BackendError ? error.code : error),
  )

it('a merge after a replace whose drop the store refused drops the old generation, its mark and the flag', async () => {
  const { id, inc } = await roomWithMeta('drop-merge')
  onReads(id, async (selector) => {
    if (isMarksRead(selector)) throw refused()
  })
  const replaced = await outcomeOf(Room.setMeta(id, { replacement: true }))
  vi.restoreAllMocks()
  const afterRefusal = await held(id, inc)
  await Room.get(id)
  await Room.updateMeta(id, { merged: true })

  expect({ replaced, afterRefusal, afterMerge: await held(id, inc), meta: (await Room.get(id)).meta }).toEqual({
    replaced: 'denied',
    afterRefusal: { marks: 1, pending: true, generations: 2 },
    afterMerge: { marks: 0, pending: false, generations: 1 },
    meta: { replacement: true, merged: true },
  })
})

it('a replace that another replace overtook, both having read no drop pending, drops what the other left', async () => {
  const { id, inc } = await roomWithMeta('drop-overtaken')
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
  // The overtaken replace reads the meta, and waits while the other replace commits and its drop is refused.
  const overtaken = outcomeOf(Room.setMeta(id, { overtaken: true }))
  await read.opened
  const overtaking = await outcomeOf(Room.setMeta(id, { overtaking: true }))
  const beforeOvertaken = await held(id, inc)
  release.open()

  expect({ overtaking, beforeOvertaken, overtaken: await overtaken, after: await held(id, inc) }).toEqual({
    overtaking: 'denied',
    beforeOvertaken: { marks: 1, pending: true, generations: 2 },
    overtaken: 'resolved',
    after: { marks: 0, pending: false, generations: 1 },
  })
  expect((await Room.get(id)).meta).toEqual({ overtaking: true })
})

it('a drop that read the marks before a newer replace marked finishes its own and leaves the newer mark and flag', async () => {
  const { id, inc } = await roomWithMeta('drop-newer-mark')
  onReads(id, async (selector) => {
    if (isMarksRead(selector)) throw refused()
  })
  await outcomeOf(Room.setMeta(id, { first: true }))
  vi.restoreAllMocks()
  // The merge's drop reads the marks, and waits while a newer replace marks, its own drop refused.
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
  // The drop waits with the marks it read, from before the newer replace.
  const newer = await outcomeOf(Room.setMeta(id, { second: true }))
  release.open()
  const merged = await merging
  vi.restoreAllMocks()
  const afterDrop = await held(id, inc)
  await Room.updateMeta(id, { again: true })

  expect({ newer, merged, afterDrop, afterNextMerge: await held(id, inc) }).toEqual({
    newer: 'denied',
    merged: 'resolved',
    afterDrop: { marks: 1, pending: true, generations: 2 },
    afterNextMerge: { marks: 0, pending: false, generations: 1 },
  })
})

it('a merge whose drop of a pending generation is refused resolves, the refusal is reported once, and the next merge drops it', async () => {
  const { id, inc } = await roomWithMeta('drop-refused')
  onReads(id, async (selector) => {
    if (isMarksRead(selector)) throw refused()
  })
  await outcomeOf(Room.setMeta(id, { replacement: true }))
  const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
  const merged = await outcomeOf(Room.updateMeta(id, { merged: true }))
  const refusals = reported.mock.calls.filter(([logged]) =>
    String(logged).includes('the store refused the read'),
  ).length
  vi.restoreAllMocks()
  const afterRefusal = await held(id, inc)
  await Room.updateMeta(id, { again: true })

  expect({ merged, refusals, afterRefusal, afterNextMerge: await held(id, inc) }).toEqual({
    merged: 'resolved',
    refusals: 1,
    afterRefusal: { marks: 1, pending: true, generations: 2 },
    afterNextMerge: { marks: 0, pending: false, generations: 1 },
  })
})
