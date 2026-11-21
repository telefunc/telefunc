// A `Room.setMeta()` that replaces a generation of more than one page: its exchange commits, then its control commit
// and its drop of the old generation run side by side. What the store keeps when one of the two fails first, on the
// memory store.
// - The control commit fails while the drop waits for its second page: the call answers, and the drop hands over no
//   delete after that. The old generation stays marked, for a later replace or the heartbeat to drop.
// - The drop fails first while the control commit is still in flight: the call answers once the commit settled. The
//   update is committed and the old generation stays marked.
import { afterEach, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc/__internal'
import { MemoryRoomDriver } from '../backend/MemoryRoomDriver.js'
import type {
  CellsExchange,
  CellsExchangeResult,
  CommitOptions,
  CommitResult,
  CommitTarget,
} from '../backend/contract.js'
import { getRoomPlane } from '../backend/plane.js'
import { Room } from '../index.js'
import { META_DROP_CELL_PREFIX, roomMetaGenPrefix } from './store/cells.js'

afterEach(() => vi.restoreAllMocks())

const deferred = () => {
  let open = () => {}
  const done = new Promise<void>((resolve) => {
    open = resolve
  })
  return { open, done }
}
const refused = () =>
  new BackendError({
    code: 'denied',
    outcome: 'not-run',
    message: 'the store refused the step',
  })

/** A room whose meta is generation 0, with more keys than a page of cells holds. */
async function roomWithTwoPagesOfMeta(): Promise<{ id: string; inc: string }> {
  const id = `meta-${crypto.randomUUID()}`
  await Room.create(id, { emptyTimeout: Infinity, departureTimeout: Infinity })
  await Room.setMeta(id, Object.fromEntries(Array.from({ length: 1100 }, (_, n) => [`k${n}`, n])))
  const head = await getRoomPlane().direct.reads.readHead(id)
  if (!head?.currentInc) throw new Error(`room ${id} has no open head`)
  return { id, inc: head.currentInc }
}

/** What the store keeps of generation 0: its marker and its key cells. */
async function generationZero(id: string, inc: string): Promise<{ marks: number; cells: number }> {
  const { reads } = getRoomPlane().direct
  const marks = await reads.readCells(id, inc, {
    prefix: META_DROP_CELL_PREFIX,
  })
  const cells = await reads.readCells(id, inc, {
    prefix: roomMetaGenPrefix(0),
  })
  return {
    marks: marks.kind === 'stale-incarnation' ? -1 : marks.cells.size,
    cells: cells.kind === 'stale-incarnation' ? -1 : cells.cells.size,
  }
}

const outcomeOf = (call: Promise<unknown>) =>
  call.then(
    () => 'resolved',
    (error: unknown) => (error instanceof BackendError ? error.outcome : error),
  )

it('the control commit fails while the drop waits for its second page: no delete after the answer, the old generation kept', async () => {
  const { id, inc } = await roomWithTwoPagesOfMeta()
  const secondPage = deferred()
  const release = deferred()
  const { readCellPages, commitLane } = MemoryRoomDriver.prototype
  vi.spyOn(MemoryRoomDriver.prototype, 'readCellPages').mockImplementation(async function* (
    this: MemoryRoomDriver,
    roomId,
    incarnation,
    prefix,
  ) {
    let pages = 0
    for await (const page of readCellPages.call(this, roomId, incarnation, prefix)) {
      if (roomId === id && prefix === roomMetaGenPrefix(0) && pages++ === 1) {
        secondPage.open()
        await release.done
      }
      yield page
    }
  })
  vi.spyOn(MemoryRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: MemoryRoomDriver,
    target: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    if (target.roomId !== id || target.lane.kind !== 'control')
      return await commitLane.call(this, target, payload, opts)
    await secondPage.done
    // The drop's page comes once the commit failed, and the call answered by then where it doesn't wait for it.
    setImmediate(release.open)
    throw refused()
  })

  const replaced = await outcomeOf(Room.setMeta(id, { replacement: true }))
  await release.done
  await new Promise<void>((resolve) => setImmediate(resolve))
  vi.restoreAllMocks()

  expect({ replaced, left: await generationZero(id, inc) }).toEqual({
    replaced: 'may-have-run',
    left: { marks: 1, cells: 1100 },
  })
})

it('the drop fails first while the control commit is in flight: the update committed, the old generation kept', async () => {
  const { id, inc } = await roomWithTwoPagesOfMeta()
  const dropFailed = deferred()
  const { compareExchangeCells, commitLane } = MemoryRoomDriver.prototype
  vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeCells').mockImplementation(async function (
    this: MemoryRoomDriver,
    roomId: string,
    incarnation: string,
    exchange: CellsExchange,
  ): Promise<CellsExchangeResult> {
    const deletes = exchange.mutations.length > 0 && exchange.mutations.every((mutation) => mutation.bytes === null)
    if (roomId === id && deletes) {
      dropFailed.open()
      throw refused()
    }
    return await compareExchangeCells.call(this, roomId, incarnation, exchange)
  })
  vi.spyOn(MemoryRoomDriver.prototype, 'commitLane').mockImplementation(async function (
    this: MemoryRoomDriver,
    target: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    if (target.roomId === id && target.lane.kind === 'control') await dropFailed.done
    return await commitLane.call(this, target, payload, opts)
  })

  const replaced = await outcomeOf(Room.setMeta(id, { replacement: true }))
  vi.restoreAllMocks()

  expect({
    replaced,
    meta: (await Room.get(id)).meta,
    left: await generationZero(id, inc),
  }).toEqual({
    replaced: 'may-have-run',
    meta: { replacement: true },
    left: { marks: 1, cells: 1100 },
  })
})
