// What the backend tells of a Cluster's reply that ended a command, on a Cluster of the oldest and the newest Redis and
// Valkey Telefunc supports, a primary and its replica, through an app client that follows no redirection
// (`maxRedirections: 0`), as a user of `+@all`.
import { setTimeout as sleep } from 'node:timers/promises'
import type { Cluster } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { reaches } from '../test/acl.js'
import { appCluster, startCluster, type Forwarder, type Nodes } from '../test/cluster.js'
import { forwarder, removeStores, runs, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

let app: Cluster | undefined
let via: [Forwarder, Forwarder] | undefined
afterAll(() => {
  app?.disconnect()
  for (const proxy of via ?? []) proxy.close()
  removeStores()
})

/** A new Cluster whose nodes have a user of `+@all`, and the backend installed on a client of that user that follows no
 *  redirection, once it reaches the Cluster. */
async function following(store: Store): Promise<{ nodes: Nodes; where: string }> {
  const proxies = (via ??= [await forwarder(), await forwarder()])
  app?.disconnect()
  removeStores()
  const nodes = await startCluster(store, proxies)
  for (const { admin } of [nodes.primary, nodes.replica]) {
    await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  }
  app = appCluster(proxies, nodes, { maxRedirections: 0 })
  installRedis(app)
  await reaches(`the ${store.name} Cluster`)
  return { nodes, where: proxies.map(({ port }) => `127.0.0.1:${port}`).join(', ') }
}

/** What a publish is told, or `published`. */
async function publishing(channel: string): Promise<unknown> {
  try {
    await Broadcast.publish(channel, 'hello')
    return 'published'
  } catch (error) {
    return error
  }
}

describe.runIf(runs)("a Cluster's reply that ended a command", () => {
  for (const store of STORES) {
    it(`${store.name}: a MOVED after a replica's promotion is told as a redirection`, async () => {
      const { nodes, where } = await following(store)
      // The replica takes the primary's slots over, and the app's client still sends their commands to the old primary.
      await nodes.replica.admin.call('CLUSTER', 'FAILOVER')
      const role = async (node: Nodes['primary']) => String(await node.admin.call('ROLE')).split(',')[0]
      for (
        let tries = 0;
        (await role(nodes.replica)) !== 'master' || (await role(nodes.primary)) !== 'slave';
        tries++
      ) {
        if (tries > 800) throw new Error(`${store.name}: the replica never took over`)
        await sleep(25)
      }
      expect(await publishing('moved')).toMatchObject({
        name: 'BackendError',
        code: 'unavailable',
        outcome: 'not-run',
        message: `The Redis Cluster at ${where} redirected a command of Telefunc's Redis backend (MOVED) more times than ioredis follows, so it didn't run: a later call may succeed once ioredis reaches the node that serves the command's slot`,
      })
    }, 120_000)
  }
})
