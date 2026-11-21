// A call's store counts every write it hands over in the call's account: `CallStore` overrides each write of
// `RoomStore`, and `join`. The record below names the writes; the compiler refuses it once `RoomStore` has a write it
// doesn't name, so a write added to the store fails here until `CallStore` counts it too.
import { expect, it } from 'vitest'
import { CallStore, type RoomStore } from './RoomStore.js'

/** The writes of `RoomStore`: its public members but its reads and `join`. */
type RoomStoreWrite = Exclude<keyof RoomStore, 'reads' | 'join'>

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
