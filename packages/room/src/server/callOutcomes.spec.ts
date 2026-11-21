// A public Room call has one account of its writes: a step the store refuses after the call reaped a lapsed lease
// says the call may have run, as that reap wrote. On the memory store, with the later step refused as a store refuses
// it, before its first write.
import { afterEach, describe, expect, it, vi } from 'vitest'
import { BackendError } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import type { OperationOptions, OperationResult, RoomOperation } from '../backend/contract.js'
import { Room } from '../index.js'
import { configFromHead, encodeRoomRecord } from './lanes/lanes.js'
import { newRoomHold } from './lifecycle/hold.js'
import { RoomReplica } from './RoomReplica.js'
import { LEASE_CELL_PREFIX, META_SEQ_CELL_KEY } from './store/cells.js'

afterEach(() => vi.restoreAllMocks())

function refused(): BackendError {
  return new BackendError({ code: 'denied', outcome: 'not-run', message: 'the store refused the step (NOPERM)' })
}

/** An open room written to the store alone, which no view in this process holds, so opening it reads its roster. */
async function storedRoom(): Promise<string> {
  const id = `outcome-${crypto.randomUUID()}`
  const inc = crypto.randomUUID()
  const hold = { emptyTimeout: 60_000, departureTimeout: 60_000 }
  await getRoomPlane().compareExchangeHead(
    id,
    { form: 'absent' },
    {
      head: { currentInc: inc, state: 'open', config: encodeRoomRecord({ meta: {}, inc, hold }) },
      ...newRoomHold(hold),
    },
  )
  return id
}

/** A lease of another server in room `id` that lapsed, hosting nothing: the call's reap claims and removes it. Whether
 *  it is still there. */
async function lapsedLease(id: string): Promise<() => Promise<boolean>> {
  const plane = getRoomPlane()
  const head = await plane.driver.readHead(id)
  if (head === null) throw new Error(`room ${id} has no head`)
  const { inc } = configFromHead(head)
  const key = `${LEASE_CELL_PREFIX}lapsed-${crypto.randomUUID()}`
  const bytes = new TextEncoder().encode(JSON.stringify({ until: 0 }))
  await plane.driver.compareExchangeCells(id, inc, { expect: [], mutations: [{ key, bytes }] })
  return async () => {
    const read = await plane.driver.readCells(id, inc, { keys: [key] })
    return read.kind !== 'stale-incarnation' && read.cells.has(key)
  }
}

/** The store refuses each read of the room's meta. */
function refuseMetaReads(): void {
  const { driver } = getRoomPlane()
  const readCells = driver.readCells.bind(driver)
  vi.spyOn(driver, 'readCells').mockImplementation(async (roomId, inc, selector) => {
    if ('keys' in selector && selector.keys.includes(META_SEQ_CELL_KEY)) throw refused()
    return await readCells(roomId, inc, selector)
  })
}

/** The store refuses each removal of a member. */
function refuseLeaves(): void {
  const plane = getRoomPlane()
  const runOperation = plane.runOperation.bind(plane)
  async function refusingLeaves<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    if (operation.name === 'leave') throw refused()
    return await runOperation(roomId, inc, operation, args, options)
  }
  vi.spyOn(plane, 'runOperation').mockImplementation(refusingLeaves)
}

/** How `call` ends: its error's code and outcome, or `'resolved'`. */
async function outcomeOf(call: Promise<unknown>): Promise<unknown> {
  return await call.then(
    () => 'resolved',
    (error: unknown) => (error instanceof BackendError ? { code: error.code, outcome: error.outcome } : error),
  )
}

describe('a Room call whose later step the store refuses after the call reaped a lapsed lease', () => {
  it('Room.get(): the meta read refused, beside the reap, says it may have run', async () => {
    const id = await storedRoom()
    const leaseStays = await lapsedLease(id)
    refuseMetaReads()

    expect({ got: await outcomeOf(Room.get(id)), leaseStays: await leaseStays() }).toEqual({
      got: { code: 'denied', outcome: 'may-have-run' },
      leaseStays: false,
    })
  })

  it("Room.get() of a room whose view a join seeded meanwhile: the view's head read refused says it may have run", async () => {
    const id = await storedRoom()
    const leaseStays = await lapsedLease(id)
    // A join of another call seeds the view, with no roster, between this call's look for a held view and its own.
    const seeded = RoomReplica.seeded.bind(RoomReplica)
    vi.spyOn(RoomReplica, 'seeded').mockImplementationOnce((roomId, config, stored, seed) => {
      seeded(roomId, config, stored, { count: 0 })
      return seeded(roomId, config, stored, seed)
    })
    // The store refuses the head read once the call's reap removed the lease: the view's catch-up reads it.
    const { driver } = getRoomPlane()
    const readHead = driver.readHead.bind(driver)
    vi.spyOn(driver, 'readHead').mockImplementation(async (roomId) => {
      if (roomId === id && !(await leaseStays())) throw refused()
      return await readHead(roomId)
    })

    expect({ got: await outcomeOf(Room.get(id)), leaseStays: await leaseStays() }).toEqual({
      got: { code: 'denied', outcome: 'may-have-run' },
      leaseStays: false,
    })
  })

  it('Room.getOrCreate() of a room that exists: the meta read refused says it may have run', async () => {
    const id = await storedRoom()
    const leaseStays = await lapsedLease(id)
    refuseMetaReads()

    expect({ got: await outcomeOf(Room.getOrCreate(id)), leaseStays: await leaseStays() }).toEqual({
      got: { code: 'denied', outcome: 'may-have-run' },
      leaseStays: false,
    })
  })

  for (const by of ['id', 'identity'] as const) {
    it(`Room.removeParticipant() by ${by}: the removal refused says it may have run`, async () => {
      const room = await Room.create(`outcome-${crypto.randomUUID()}`)
      const participant = await room.join({ identity: 'u1' })
      const leaseStays = await lapsedLease(room.id)
      refuseLeaves()
      const target = by === 'id' ? { id: participant.id } : { identity: 'u1' }

      expect({
        removed: await outcomeOf(Room.removeParticipant(room.id, target)),
        leaseStays: await leaseStays(),
      }).toEqual({ removed: { code: 'denied', outcome: 'may-have-run' }, leaseStays: false })
    })

    it(`Room.send() by ${by}: the send refused says it may have run`, async () => {
      const room = await Room.create(`outcome-${crypto.randomUUID()}`)
      const participant = await room.join({ identity: 'u1' })
      const leaseStays = await lapsedLease(room.id)
      vi.spyOn(getRoomPlane(), 'commitLane').mockRejectedValueOnce(refused())
      const sending =
        by === 'id' ? Room.send(room.id, { id: participant.id }, 'hi') : Room.send(room.id, { identity: 'u1' }, 'hi')

      expect({ sent: await outcomeOf(sending), leaseStays: await leaseStays() }).toEqual({
        sent: { code: 'denied', outcome: 'may-have-run' },
        leaseStays: false,
      })
    })
  }

  it('a store that refuses only the later step, where no lease lapsed: the call says it did not run', async () => {
    const room = await Room.create(`outcome-${crypto.randomUUID()}`)
    const participant = await room.join({ identity: 'u1' })
    refuseLeaves()

    expect(await outcomeOf(Room.removeParticipant(room.id, { id: participant.id }))).toEqual({
      code: 'denied',
      outcome: 'not-run',
    })
  })
})
