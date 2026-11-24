// A call's store counts every write it hands over in the call's account: `CallStore` overrides each write of
// `RoomStore`, and `join`. The record below names the writes; the compiler refuses it once `RoomStore` has a write it
// doesn't name, so a write added to the store fails here until `CallStore` counts it too.
// A cells exchange that counts a cell twice, or counts a cell it also writes, is refused by either store before the
// driver is called: each is a bug of the caller's, thrown as an assertion.
import { expect, it, vi } from 'vitest'
import type { CellsExchange } from './contract.js'
import { MemoryRoomDriver } from './MemoryRoomDriver.js'
import { RoomPlane } from './RoomPlane.js'
import { CallStore, type RoomStore } from './RoomStore.js'

/** The writes of `RoomStore`: its public members but its reads, its size rule, which writes nothing, and `join`. */
type RoomStoreWrite = Exclude<keyof RoomStore, 'reads' | 'assertCommittable' | 'join'>

const WRITES = {
  compareExchangeHead: true,
  compareExchangeCells: true,
  commitLane: true,
  runOperation: true,
  dropIncarnation: true,
  directoryPut: true,
  directoryDelete: true,
} satisfies Record<RoomStoreWrite, true>

it("a call's store overrides every write of the store, and join, so each is counted in its account", () => {
  const overridden = [...Object.keys(WRITES), 'join'].filter((name) => Object.hasOwn(CallStore.prototype, name))
  expect(overridden).toEqual([...Object.keys(WRITES), 'join'])
})

it('refuses a cells exchange that counts a cell twice, or counts a cell it writes, before the driver is called', async () => {
  const driver = new MemoryRoomDriver()
  const exchanged = vi.spyOn(driver, 'compareExchangeCells')
  const plane = new RoomPlane({ driver })
  const malformed: Record<string, CellsExchange> = {
    'counted twice': {
      expect: [],
      mutations: [],
      counts: [
        { key: 'c', delta: 1 },
        { key: 'c', delta: 1 },
      ],
    },
    'counted and written': { expect: [], mutations: [{ key: 'c', bytes: null }], counts: [{ key: 'c', delta: 0 }] },
  }
  const refused = async (exchange: (store: RoomStore) => Promise<unknown>) => {
    const direct = await Promise.resolve()
      .then(() => exchange(plane.direct))
      .then(
        () => 'exchanged',
        (error: unknown) => String(error),
      )
    const call = await plane.call(exchange).then(
      () => 'exchanged',
      (error: unknown) => String(error),
    )
    return { direct, call }
  }
  const told = Object.fromEntries(
    await Promise.all(
      Object.entries(malformed).map(
        async ([shape, exchange]) =>
          [shape, await refused((store) => store.compareExchangeCells('r', 'i', exchange))] as const,
      ),
    ),
  )

  expect({ told, driverCalls: exchanged.mock.calls.length }).toEqual({
    told: {
      'counted twice': {
        direct: expect.stringContaining('cells exchange: c is counted twice'),
        call: expect.stringContaining('cells exchange: c is counted twice'),
      },
      'counted and written': {
        direct: expect.stringContaining('cells exchange: c is both counted and written'),
        call: expect.stringContaining('cells exchange: c is both counted and written'),
      },
    },
    driverCalls: 0,
  })
  plane.dispose()
})
