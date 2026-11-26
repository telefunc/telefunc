// The ACL rule the Redis page gives Telefunc's own user, read from the page itself, on a Cluster of the oldest and the
// newest Redis and Valkey Telefunc supports, a primary, its replica and a spare primary: every node takes it as it is
// written, and a user of it makes every call Telefunc makes of the store, also across a slot's move to the spare
// (ASK while it migrates, MOVED once it moved), the replica's promotion and the end of every connection of the user.
// The nodes refuse that user only the client info ioredis sends on each connection it opens, on the stores that know
// `CLIENT SETINFO`, and the READONLY and READWRITE it sends to switch a connection's mode, which the rule leaves out;
// with the page's optional grant they refuse nothing else.
import { setTimeout as sleep } from 'node:timers/promises'
import type { Cluster } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'
import {
  arrived,
  CLIENT_INFO_GRANT,
  calls,
  documentedRule,
  EVERY_CALL_RUNS,
  knowsClientInfo,
  outcomeOf,
  reaches,
  refused,
  setUser,
} from '../test/acl.js'
import { appCluster, listed, startCluster, type Forwarder, type Nodes } from '../test/cluster.js'
import { forwarder, removeStores, runs, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

let app: Cluster | undefined
let via: Forwarder[] | undefined
afterAll(() => {
  app?.disconnect()
  for (const proxy of via ?? []) proxy.close()
})

/** A new Cluster whose nodes have the user of `rule`, the backend installed on a client of that user once it reaches the
 *  Cluster; the nodes' answers to the rule. */
async function asUserOf(store: Store, rule: string[]): Promise<{ nodes: Nodes; rules: string[] }> {
  const proxies = (via ??= [await forwarder(), await forwarder(), await forwarder()])
  app?.disconnect()
  removeStores()
  const nodes = await startCluster(store, proxies)
  const rules = await Promise.all(listed(nodes).map(({ admin }) => setUser(admin, rule)))
  for (const { admin } of listed(nodes)) await admin.call('ACL', 'LOG', 'RESET')
  app = appCluster(proxies, listed(nodes))
  installRedis(app)
  await reaches(`the ${store.name} Cluster`)
  return { nodes, rules }
}

/** What the nodes refused the user from the first connection on. The rule leaves out `+readonly`. Telefunc reads from
 *  primaries only (`scaleReads: 'master'`), and ioredis 5.10.1 and 6.0.0 hand a write to a replica's connection while
 *  they hold none to the slot's primary: in READONLY mode, the replica runs the write's script and refuses it, until
 *  ioredis reads the slots again (redis/ioredis#2274). Without the grant, the connection stays read-write and the
 *  replica answers MOVED, which ioredis follows to the primary. ioredis drops the refusal, which the replica's ACL log
 *  records once per connection, as it does READWRITE's after a failover. */
async function refusedBesidesModes(nodes: Nodes): Promise<string[]> {
  const modes = new Set(['command readonly', 'command readwrite'])
  return (await refused(listed(nodes).map(({ admin }) => admin))).filter((entry) => !modes.has(entry))
}

/** Until every node of `nodes` answers `cluster_state:ok`, and `node` answers `role`. */
async function settled(nodes: Nodes, node: Nodes['primary'], role: string): Promise<void> {
  for (let tries = 0; ; tries++) {
    const states = await Promise.all(listed(nodes).map(({ admin }) => admin.call('CLUSTER', 'INFO')))
    const roleNow = String(await node.admin.call('ROLE')).split(',')[0]
    if (roleNow === role && states.every((info) => String(info).includes('cluster_state:ok'))) return
    if (tries > 800) throw new Error(`the Cluster never settled with ${node.host} a ${role}`)
    await sleep(25)
  }
}

/** The user's connections on every node, by node and id. */
async function userConnections(nodes: Nodes): Promise<string[]> {
  const lists = await Promise.all(listed(nodes).map(({ admin }) => admin.call('CLIENT', 'LIST')))
  return lists.flatMap((list, node) =>
    String(list)
      .split('\n')
      .filter((line) => line.includes(' user=telefunc '))
      .map((line) => `${node}:${/^id=(\d+) /.exec(line)?.[1]}`),
  )
}

/** Moves room `id`'s slot from the primary to the spare: `whileMigrating` runs once every key of the slot is on the
 *  spare and the slot still migrates, so a command on it is asked there (ASK, then ASKING on the spare). */
async function moveRoom(nodes: Nodes, id: string, whileMigrating: () => Promise<unknown>) {
  const { primary, spare } = nodes
  if (spare === undefined) throw new Error('the Cluster has no spare')
  const slot = String(await primary.admin.call('CLUSTER', 'KEYSLOT', `{${id}}`))
  const [primaryId, spareId] = await Promise.all([primary, spare].map(({ admin }) => admin.call('CLUSTER', 'MYID')))
  await spare.admin.call('CLUSTER', 'SETSLOT', slot, 'IMPORTING', String(primaryId))
  await primary.admin.call('CLUSTER', 'SETSLOT', slot, 'MIGRATING', String(spareId))
  const keys = await primary.admin.call('CLUSTER', 'GETKEYSINSLOT', slot, '1000')
  const names = Array.isArray(keys) ? keys.map(String) : []
  if (names.length === 0) throw new Error(`room ${id} has no keys in slot ${slot}`)
  await primary.admin.call('MIGRATE', spare.host, '6379', '', '0', '5000', 'KEYS', ...names)
  const migrating = await whileMigrating()
  for (const { admin } of [primary, spare]) await admin.call('CLUSTER', 'SETSLOT', slot, 'NODE', String(spareId))
  await settled(nodes, spare, 'master')
  return migrating
}

describe.runIf(runs)("the Redis page's ACL rule on a Cluster", () => {
  for (const store of STORES) {
    it(`${store.name}: every node takes the rule, and a user of it makes every call`, async () => {
      const { nodes, rules } = await asUserOf(store, documentedRule())
      expect({ rules, calls: await calls(), refused: await refusedBesidesModes(nodes) }).toEqual({
        rules: ['OK', 'OK', 'OK'],
        calls: EVERY_CALL_RUNS,
        refused: knowsClientInfo(store) ? ['command client|setinfo'] : [],
      })
    }, 120_000)

    it.runIf(knowsClientInfo(store))(
      `${store.name}: with the optional grant, the nodes refuse the user nothing`,
      async () => {
        const { nodes, rules } = await asUserOf(store, [...documentedRule(), CLIENT_INFO_GRANT])
        expect({ rules, calls: await calls(), refused: await refusedBesidesModes(nodes) }).toEqual({
          rules: ['OK', 'OK', 'OK'],
          calls: EVERY_CALL_RUNS,
          refused: [],
        })
      },
      120_000,
    )

    it(`${store.name}: a user of the rule makes every call across a slot's move, a promotion and a connection's end`, async () => {
      const { nodes } = await asUserOf(store, documentedRule())
      const id = 'acl-moving'
      await Room.create(id)
      await Room.setMeta(id, { a: '1' })
      const asked = await moveRoom(nodes, id, () => outcomeOf(() => Room.getParticipants(id)))
      const moved = await outcomeOf(() => Room.updateMeta(id, { b: '2' }))
      // The replica takes the primary's slots over.
      await nodes.replica.admin.call('CLUSTER', 'FAILOVER')
      await settled(nodes, nodes.replica, 'master')
      // Once the backend reaches the promoted Cluster: a call that races the promotion is told what the promotion did
      // to its connection (dropped-call-cluster.store.spec.ts).
      await reaches(`the promoted ${store.name} Cluster`)
      const promoted = await calls('-promoted')
      // A subscription held across the end of every connection, live once its first message arrived.
      const held: unknown[] = []
      const holding = Broadcast.subscribe('acl-held', (message) => held.push(message))
      await holding.opened
      await Broadcast.publish('acl-held', 'before')
      if (!(await arrived(held))) throw new Error('the held subscription got no message')
      const before = await userConnections(nodes)
      let killed = 0
      for (const { admin } of listed(nodes)) killed += Number(await admin.call('CLIENT', 'KILL', 'USER', 'telefunc'))
      await reaches(`the ${store.name} Cluster`)
      const reconnected = await calls('-reconnected')
      // The held subscription subscribes again on its new connection after the call returns: the message is published
      // again until one arrives.
      held.length = 0
      let heldAgain = false
      for (let tries = 0; !heldAgain && tries <= 40; tries++) {
        await Broadcast.publish('acl-held', 'after')
        heldAgain = await arrived(held, 10)
      }
      const after = await userConnections(nodes)
      holding()
      expect({
        asked,
        moved,
        promoted,
        killed: killed > 0,
        kept: before.filter((connection) => after.includes(connection)),
        reconnected,
        heldAgain,
        refused: await refusedBesidesModes(nodes),
      }).toEqual({
        asked: 'ok',
        moved: 'ok',
        promoted: EVERY_CALL_RUNS,
        killed: true,
        kept: [],
        reconnected: EVERY_CALL_RUNS,
        heldAgain: true,
        refused: knowsClientInfo(store) ? ['command client|setinfo'] : [],
      })
    }, 240_000)
  }
})
