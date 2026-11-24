// The store specs' fixtures on a Cluster of three primaries with a replica each, on the oldest and the newest Redis and
// Valkey Telefunc supports: every slot served, each shard's keys routed to its primary through its node's endpoint, and
// the backend's commands and subscriber each on its role's endpoints.
import { afterAll, describe, expect, it } from 'vitest'
import { Room } from '@telefunc/room'
import { shardedClusterRows, slotsOf } from '../test/cluster.js'
import { carriers, delivered } from '../test/endpoints.js'
import { runs, STORES } from '../test/stores.js'

const rows = shardedClusterRows()
afterAll(() => rows.end())

describe.runIf(runs)("the store specs' fixtures on a Cluster", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a Cluster of three primaries with a replica each routes each shard's keys to its primary`, async () => {
      const at = await rows.reached(store)
      const [shard] = at.shards
      if (shard === undefined) throw new Error('the Cluster has no shard')
      // A key of each shard's slots, written through the app's Cluster, sits on that shard's primary only, which its
      // node's command endpoint carried.
      const routed: { holders: number[]; carriedBy: number[] }[] = []
      for (const s of at.shards.keys()) {
        const [low, high] = slotsOf(s, at.shards.length)
        let [key, slot] = ['', -1]
        for (let n = 0; key === ''; n++) {
          slot = Number(await shard.primary.admin.call('CLUSTER', 'KEYSLOT', `fixtures-${n}`))
          if (low <= slot && slot <= high) key = `fixtures-${n}`
        }
        await at.app.set(key, 'routed')
        // A node answers a key of a slot it doesn't serve with MOVED, but counts any slot's keys it holds.
        const holders = await Promise.all(
          at.shards.map(({ primary }) => primary.admin.call('CLUSTER', 'COUNTKEYSINSLOT', String(slot))),
        )
        routed.push({
          holders: holders.flatMap((held, h) => (held === 1 ? [h] : [])),
          // The key as RESP frames it, so that no longer key holds it.
          carriedBy: carriers(
            at.ends.map(({ command }) => command),
            `\r\n${key}\r\n`,
          ),
        })
      }
      const created = await Room.create(`fixtures-cluster-${i}`).then(() => 'resolved', String)
      const listed = (await Room.list()).some(({ id }) => id === `fixtures-cluster-${i}`)
      const messages = await delivered(`fixtures-cluster-${i}`)
      const subscribes = {
        commands: carriers(
          at.ends.map(({ command }) => command),
          'subscribe',
        ).length,
        subscribers: carriers(
          at.ends.map(({ subscriber }) => subscriber),
          'subscribe',
        ).length,
      }

      expect({
        routed,
        created,
        listed,
        messages,
        subscribed: subscribes.commands === 0 && subscribes.subscribers > 0,
      }).toEqual({
        routed: [
          { holders: [0], carriedBy: [0] },
          { holders: [1], carriedBy: [2] },
          { holders: [2], carriedBy: [4] },
        ],
        created: 'resolved',
        listed: true,
        messages: ['one'],
        subscribed: true,
      })
    }, 240_000)
  }
})
