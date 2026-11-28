// A `Room.list()` that repairs a room's listing from a read made before the room's create exchanged its head, on the
// oldest and the newest Redis and Valkey Telefunc supports (`listRepair()`): the repair's delete, given the listing it
// read, leaves the listing the create wrote again after its exchange, and the room's data with it.
import { afterAll, describe, expect, it } from 'vitest'
import { listRepair } from '../test/listRepair.js'
import { endReached, reached, runs, STORES } from '../test/stores.js'

afterAll(() => endReached())

describe.runIf(runs)("a list's repair from a read made before a create's head exchange", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: leaves the created room listed, with its data`, async () => {
      const admin = await reached(store)
      const seen = await listRepair(admin, `list-repair-${i}`)
      console.log(JSON.stringify({ store: store.name, ...seen }))
      admin.disconnect()

      expect(seen).toEqual({ created: 'resolved', firstList: false, laterList: true, keysKept: true })
    }, 120_000)
  }
})
