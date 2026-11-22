// The ACL rule the Redis page gives Telefunc's own user, read from the page itself, on a Cluster of the oldest and the
// newest Redis and Valkey Telefunc supports, a primary and its replica: both take it as it is written, and a user of it
// makes every call Telefunc makes of the store. They refuse that user only the client info ioredis sends on each
// connection it opens, on the stores that know `CLIENT SETINFO`, and the READONLY it sends on a connection to the
// replica, which the rule leaves out; with the page's optional grant they refuse nothing else.
import type { Cluster } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import {
  CLIENT_INFO_GRANT,
  calls,
  documentedRule,
  EVERY_CALL_RUNS,
  knowsClientInfo,
  reaches,
  refused,
  setUser,
} from '../test/acl.js'
import { appCluster, startCluster, type Forwarder } from '../test/cluster.js'
import { forwarder, removeStores, runs, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

let app: Cluster | undefined
let via: [Forwarder, Forwarder] | undefined
afterAll(() => {
  app?.disconnect()
  for (const proxy of via ?? []) proxy.close()
  removeStores()
})

/** A new Cluster whose nodes have the user of `rule`, the backend installed on a client of that user once it reaches the
 *  Cluster; the nodes' answers to the rule, the calls' outcomes, and what the nodes refused the user from the first
 *  connection on. */
async function asUserOf(store: Store, rule: string[]) {
  const proxies = (via ??= [await forwarder(), await forwarder()])
  app?.disconnect()
  removeStores()
  const nodes = await startCluster(store, proxies)
  const admins = [nodes.primary.admin, nodes.replica.admin]
  const set = await Promise.all(admins.map((admin) => setUser(admin, rule)))
  for (const admin of admins) await admin.call('ACL', 'LOG', 'RESET')
  app = appCluster(proxies, nodes)
  installRedis(app)
  await reaches(`the ${store.name} Cluster`)
  // The rule leaves out `+readonly`. Telefunc reads from primaries only (`scaleReads: 'master'`), and ioredis 5.10.1 and
  // 6.0.0 hand a write to a replica's connection while they hold none to the slot's primary: in READONLY mode, the
  // replica runs the write's script and refuses it, until ioredis reads the slots again (redis/ioredis#2274). Without
  // the grant, the connection stays read-write and the replica answers MOVED, which ioredis follows to the primary.
  // ioredis drops the refusal, which the replica's ACL log records once per connection, as it does READWRITE's after a
  // failover.
  const told = await calls()
  const refusedHere = (await refused(admins)).filter((entry) => entry !== 'command readonly')
  return { rules: set, calls: told, refused: refusedHere }
}

describe.runIf(runs)("the Redis page's ACL rule on a Cluster", () => {
  for (const store of STORES) {
    it(`${store.name}: both nodes take the rule, and a user of it makes every call`, async () => {
      expect(await asUserOf(store, documentedRule())).toEqual({
        rules: ['OK', 'OK'],
        calls: EVERY_CALL_RUNS,
        refused: knowsClientInfo(store) ? ['command client|setinfo'] : [],
      })
    }, 120_000)

    it.runIf(knowsClientInfo(store))(
      `${store.name}: with the optional grant, the nodes refuse the user nothing`,
      async () => {
        expect(await asUserOf(store, [...documentedRule(), CLIENT_INFO_GRANT])).toEqual({
          rules: ['OK', 'OK'],
          calls: EVERY_CALL_RUNS,
          refused: [],
        })
      },
      120_000,
    )
  }
})
