// A `Room.list()` that repairs a room's listing from an earlier read, while the room is created: the list reads the
// create's first listing and the absent head, and waits; the create's head exchange and its second listing run; the
// list's repair goes on. The store is the backend's, on the store `admin` is of (a Cluster's primary serves every slot).
export { listRepair }

import type { Redis } from 'ioredis'
import { vi } from 'vitest'
import { Room } from '@telefunc/room'
import { getRoomPlane } from '@telefunc/room/backend'
import { RedisRoomDriver } from '../src/RedisRoomDriver.js'

const answer = (call: Promise<unknown>) =>
  call.then(
    () => 'resolved',
    (error: unknown) => (error instanceof Error ? error.message : String(error)),
  )

/** What room `id` is, once the interleaving ran: what its create answered, whether the list that repaired it and a list
 *  after it show it, and whether its incarnation's keys stayed. */
async function listRepair(admin: Redis, id: string) {
  const { readHead, directoryPut } = RedisRoomDriver.prototype
  const preListed = Promise.withResolvers<void>()
  const createGoesOn = Promise.withResolvers<void>()
  const listRead = Promise.withResolvers<void>()
  const listGoesOn = Promise.withResolvers<void>()
  let puts = 0
  let listing = false
  vi.spyOn(RedisRoomDriver.prototype, 'directoryPut').mockImplementation(async function (
    this: RedisRoomDriver,
    roomId,
    incTag,
    checkAt,
    minDelayMs,
  ) {
    await directoryPut.call(this, roomId, incTag, checkAt, minDelayMs)
    if (roomId !== id || puts++ > 0) return
    preListed.resolve()
    await createGoesOn.promise
  })
  vi.spyOn(RedisRoomDriver.prototype, 'readHead').mockImplementation(async function (this: RedisRoomDriver, roomId) {
    const head = await readHead.call(this, roomId)
    if (roomId !== id || !listing) return head
    listing = false
    listRead.resolve()
    await listGoesOn.promise
    return head
  })
  const creating = answer(Room.create(id, { meta: { topic: 'new' } }))
  await preListed.promise
  listing = true
  const listed = Room.list().then((rooms) => rooms.some((room) => room.id === id))
  await listRead.promise
  createGoesOn.resolve()
  const created = await creating
  const { reads } = getRoomPlane().direct
  const inc = (await reads.readHead(id))?.currentInc ?? null
  const keys = async () => (inc === null ? 0 : (await admin.keys(`*${inc}*`)).length)
  const keysCreated = await keys()
  listGoesOn.resolve()
  const firstList = await listed
  vi.restoreAllMocks()
  return {
    created,
    firstList,
    laterList: (await Room.list()).some((room) => room.id === id),
    keysKept: keysCreated > 0 && (await keys()) === keysCreated,
  }
}
