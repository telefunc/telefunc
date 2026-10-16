import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MEMBER_CELL_PREFIX } from './cells.js'
import { Room } from './statics.js'
import { ServerRoom } from './room.js'
import { captureHeartbeats, disposeRoomBackend, driver, forceRoomGc, installRoomBackend } from '../../test/room.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
describe("a server's handles to one room", () => {
  for (const handles of [1, 10, 100, 1_000]) {
    it(`read no member list and write once per heartbeat, at ${handles} handles on 10,000 members`, async () => {
      const heartbeats = captureHeartbeats()
      const room = await Room.create(`shared-view-${handles}`)
      const views = [room, ...(await Promise.all(Array.from({ length: handles - 1 }, () => Room.get(room.id))))]
      await Promise.all(views.map((view) => Promise.all(Array.from({ length: 10_000 / handles }, () => view.join()))))
      const reads = vi.spyOn(driver, 'readCellPages')
      const writes = vi.spyOn(driver, 'compareExchangeCells')
      /** No store call for five timer turns in a row. */
      const quiet = async () => {
        for (let still = 0, last = -1; still < 5; ) {
          await new Promise((resolve) => setTimeout(resolve, 20))
          const calls = reads.mock.calls.length + writes.mock.calls.length
          still = calls === last ? still + 1 : 0
          last = calls
        }
      }
      await quiet()
      reads.mockClear()
      writes.mockClear()
      heartbeats.fire()
      await quiet()
      const memberReads = reads.mock.calls.filter(([, , prefix]) => prefix === MEMBER_CELL_PREFIX).length
      const written = writes.mock.calls.flatMap(([, , { mutations }]) => mutations.map(({ key }) => key))
      expect({ memberReads, written }).toEqual({ memberReads: 0, written: [expect.stringMatching(/^session:/)] })
    }, 120_000)
  }
  it('let their view go once no handle holds it, while a member keeps another view, and a Room.get() after that opens a new one', async () => {
    const id = 'shared-view-dropped'
    const dropped = await (async () => {
      const room = (await Room.create(id)) as ServerRoom
      const other = (await Room.get(id)) as ServerRoom
      expect(other._replica).toBe(room._replica)
      return new WeakRef(room._replica)
    })()
    const owning = await (async () => {
      const room = (await Room.create('shared-view-owning')) as ServerRoom
      await room.join()
      return new WeakRef(room._replica)
    })()
    await forceRoomGc()
    expect([dropped.deref(), owning.deref() === undefined]).toEqual([undefined, false])
    expect((await Room.get(id)).count).toBe(0)
  })
})
