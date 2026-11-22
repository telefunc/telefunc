// A call's store counts every write it hands over in the call's account: `CallStore` overrides each write of
// `RoomStore`, and `join`. The record below names the writes; the compiler refuses it once `RoomStore` has a write it
// doesn't name, so a write added to the store fails here until `CallStore` counts it too.
// A cells exchange that counts a cell twice, or counts a cell it also writes, is refused by either store before the
// driver is called, and so is one whose control record is numbered by a cell it doesn't count or isn't a length: each is
// a bug of the caller's, thrown as an assertion.
import { expect, it, vi } from 'vitest'
import type { CellsExchange, ControlRecord } from './contract.js'
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

it("refuses a cells exchange whose control record isn't numbered by one of its counts, or isn't a length, before the driver is called", async () => {
  const driver = new MemoryRoomDriver()
  const exchanged = vi.spyOn(driver, 'compareExchangeCells')
  const plane = new RoomPlane({ driver })
  const exchange: CellsExchange = { expect: [], mutations: [], counts: [{ key: 'c', delta: 1, create: true }] }
  const malformed: Record<string, ControlRecord> = {
    uncounted: { counter: 'd', bytes: 10 },
    negative: { counter: 'c', bytes: -1 },
    fractional: { counter: 'c', bytes: 1.5 },
    unsafe: { counter: 'c', bytes: 2 ** 53 },
  }
  const refused = async (record: ControlRecord) => {
    const told = (error: unknown) => (error instanceof Error ? `${error.name}: ${error.message}` : String(error))
    const direct = await Promise.resolve()
      .then(() => plane.direct.compareExchangeCells('r', 'i', exchange, record))
      .then(() => 'exchanged', told)
    const call = await plane
      .call((store) => store.compareExchangeCells('r', 'i', exchange, record))
      .then(() => 'exchanged', told)
    return { direct, call }
  }
  const told: Record<string, unknown> = {}
  for (const [shape, record] of Object.entries(malformed)) told[shape] = await refused(record)

  const bug = (message: string) => {
    const assertion = new RegExp(`^Error: \\[[^\\]]+\\]\\[Bug\\] .*${message}`)
    return { direct: expect.stringMatching(assertion), call: expect.stringMatching(assertion) }
  }
  expect({ told, driverCalls: exchanged.mock.calls.length }).toEqual({
    told: {
      uncounted: bug('cells exchange: its record is numbered by d, uncounted'),
      negative: bug('cells exchange: its record is -1 bytes long'),
      fractional: bug('cells exchange: its record is 1\\.5 bytes long'),
      unsafe: bug('cells exchange: its record is 9007199254740992 bytes long'),
    },
    driverCalls: 0,
  })
  plane.dispose()
})

it("a control record names no lane: it is always the control lane's", () => {
  const record: ControlRecord = { counter: 'c', bytes: 1 }
  // @ts-expect-error A control record has no lane to name.
  const laned: ControlRecord = { counter: 'c', bytes: 1, lane: { kind: 'binary', member: 'm', track: 't' } }
  // @ts-expect-error A control record's length is a number.
  const text: ControlRecord = { counter: 'c', bytes: '1' }
  expect([record, laned, text].map(({ counter }) => counter)).toEqual(['c', 'c', 'c'])
})
