// The store specs' fixtures on a Sentinel topology, on the oldest and the newest Redis and Valkey Telefunc supports: the
// backend finds the primary through the sentinels' forwarders, and its commands and subscriber each take the primary's
// endpoint of their role.
import { afterAll, describe, expect, it } from 'vitest'
import { Room } from '@telefunc/room'
import { carriers, delivered } from '../test/endpoints.js'
import { sentinelRows } from '../test/sentinel.js'
import { runs, STORES } from '../test/stores.js'

const rows = sentinelRows()
afterAll(() => rows.end())

describe.runIf(runs)("the store specs' fixtures on a Sentinel topology", () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: a Sentinel topology's backend finds the primary through the sentinels`, async () => {
      const at = await rows.reached(store)
      const created = await Room.create(`fixtures-sentinel-${i}`).then(() => 'resolved', String)
      const messages = await delivered(`fixtures-sentinel-${i}`)
      const asked = carriers(at.toSentinels, 'get-master-addr-by-name')

      expect({
        created,
        messages,
        askedASentinel: asked.length > 0,
        // A script goes as EVAL on its first use on a socket, then as EVALSHA.
        scripts: carriers([at.ends.primary.command, at.ends.replica.command], 'eval'),
        subscribes: carriers(
          [at.ends.primary.command, at.ends.primary.subscriber, at.ends.replica.subscriber],
          'subscribe',
        ),
      }).toEqual({
        created: 'resolved',
        messages: ['one'],
        askedASentinel: true,
        scripts: [0],
        subscribes: [1],
      })
    }, 240_000)
  }
})
