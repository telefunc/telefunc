// What `Broadcast.publish()` is told when its connection to a Cluster drops, on a Cluster of the oldest and the newest
// Redis and Valkey Telefunc supports, a primary and its replica, through an app client as the Redis page sets it up, as
// a user of `+@all`: what a call that races a promotion is told when the promotion ends its connection.
// - Dropped before the call's first command: it didn't run, and the store is as before.
// - Dropped while the call's write waits at the primary, which holds every write (`CLIENT PAUSE ... WRITE`): it may have
//   run, as Telefunc can't tell whether the store ran it. Here the store didn't.
import type { Cluster } from 'ioredis'
import { afterAll, afterEach, describe, expect, it, vi } from 'vitest'
import { Broadcast } from 'telefunc'
import { reaches } from '../test/acl.js'
import { appCluster, listed, startCluster, type Forwarder, type Nodes } from '../test/cluster.js'
import { changedKeys, forwarder, removeStores, runs, snapshot, STORES, type Store } from '../test/stores.js'
import { installRedis } from './index.js'
import { RedisConnection } from './RedisConnection.js'
import type { RedisCommand } from './commands.js'

let app: Cluster | undefined
let via: Forwarder[] | undefined
afterAll(() => {
  app?.disconnect()
  for (const proxy of via ?? []) proxy.close()
})
afterEach(() => {
  vi.restoreAllMocks()
})

/** A new Cluster whose nodes have a user of `+@all`, and the backend installed on a client of that user, once it reaches
 *  the Cluster. */
async function reachedCluster(store: Store): Promise<Nodes> {
  const proxies = (via ??= [await forwarder(), await forwarder()])
  app?.disconnect()
  removeStores()
  const nodes = await startCluster(store, proxies)
  for (const { admin } of listed(nodes)) {
    await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
  }
  app = appCluster(proxies, nodes)
  installRedis(app)
  await reaches(`the ${store.name} Cluster`)
  return nodes
}

/** The publish, as a promise whether it is synchronous or not. */
async function publishing(channel: string): Promise<unknown> {
  return Broadcast.publish(channel, 'hello')
}

function told(error: unknown) {
  if (!(error instanceof Error)) return { value: typeof error }
  return {
    name: error.name,
    code: 'code' in error ? String(error.code) : 'none',
    outcome: 'outcome' in error ? String(error.outcome) : 'none',
  }
}

/** Whether the node holds a client's command, as it holds a write while its writes are paused. */
async function holdsACommand(node: Nodes['primary']): Promise<boolean> {
  const info = String(await node.admin.call('INFO', 'clients'))
  return Number(/blocked_clients:(\d+)/.exec(info)?.[1]) > 0
}

/** Runs the backend's commands as they come, but the first one that `drops` picks: before it is sent, `drop` runs. */
function droppingAt(
  drops: (command: { writes: boolean }) => boolean,
  drop: (connection: RedisConnection) => Promise<unknown>,
) {
  const { run } = RedisConnection.prototype
  let armed = true
  vi.spyOn(RedisConnection.prototype, 'run').mockImplementation(async function <Input, Output>(
    this: RedisConnection,
    command: RedisCommand<Input, Output>,
    input: Input,
  ): Promise<Output> {
    if (!armed || !drops(command)) return await run.bind(this)(command, input)
    armed = false
    await drop(this)
    return await run.bind(this)(command, input)
  })
}

describe.runIf(runs)('a publish whose connection to a Cluster drops', () => {
  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: dropped before its first command, it didn't run`, async () => {
      const nodes = await reachedCluster(store)
      const held = await snapshot(nodes.primary.admin)
      droppingAt(
        () => true,
        async (connection) => connection.client.disconnect(true),
      )
      const refused = await publishing(`dropped-first-${i}`).then(() => 'resolved', told)
      vi.restoreAllMocks()
      const changed = changedKeys(held, await snapshot(nodes.primary.admin))

      expect({ refused, changed }).toEqual({
        refused: { name: 'BackendError', code: 'unreachable', outcome: 'not-run' },
        changed: [],
      })
    }, 120_000)

    it(`${store.name}: dropped while its write waits at the primary, it may have run`, async () => {
      const nodes = await reachedCluster(store)
      const held = await snapshot(nodes.primary.admin)
      let heldAtTheStore = false
      // The write is sent while the primary holds every write; every node drops the app's connections meanwhile.
      droppingAt(
        (command) => command.writes,
        async () => {
          await nodes.primary.admin.call('CLIENT', 'PAUSE', '30000', 'WRITE')
        },
      )
      const calling = publishing(`dropped-write-${i}`).then(() => 'resolved', told)
      for (let tries = 0; tries < 2_000 && !heldAtTheStore; tries++) heldAtTheStore = await holdsACommand(nodes.primary)
      for (const { admin } of listed(nodes)) await admin.call('CLIENT', 'KILL', 'USER', 'telefunc')
      await nodes.primary.admin.call('CLIENT', 'UNPAUSE')
      const refused = await calling
      vi.restoreAllMocks()
      const changed = changedKeys(held, await snapshot(nodes.primary.admin))

      expect({ heldAtTheStore, refused, changed }).toEqual({
        heldAtTheStore: true,
        refused: { name: 'BackendError', code: 'unreachable', outcome: 'may-have-run' },
        changed: [],
      })
    }, 120_000)
  }
})
