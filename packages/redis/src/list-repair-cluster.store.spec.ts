// A `Room.list()` that repairs a room's listing from a read made before the room's create exchanged its head, on a
// Cluster of the oldest and the newest Redis and Valkey Telefunc supports (`listRepair()`): the directory's keys sit in
// a slot of their own, apart from the room's head, and the repair's delete, given the listing it read, leaves the
// listing the create wrote again after its exchange, and the room's data with it.
import { afterAll, describe, expect, it } from 'vitest'
import { clusterBackend } from '../test/cluster.js'
import { listRepair } from '../test/listRepair.js'
import { runs, STORES } from '../test/stores.js'

const backend = clusterBackend()
afterAll(() => backend.end())

describe.runIf(runs)("a list's repair from a read made before a create's head exchange, on a Cluster", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: leaves the created room listed, with its data`, async () => {
      const { primary } = await backend.reached(store)
      // The primary serves every slot, so its keys are the Cluster's.
      const seen = await listRepair(primary.admin, `list-repair-cluster-${i}`)
      console.log(JSON.stringify({ store: store.name, ...seen }))

      expect(seen).toEqual({ created: 'resolved', firstList: false, laterList: true, keysKept: true })
    }, 180_000)
  }
})
