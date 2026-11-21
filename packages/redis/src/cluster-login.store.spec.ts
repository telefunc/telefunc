// A Cluster whose nodes refuse the backend's login. A refused login stands until an operator changes the password or
// the ACL, so a subscription ends with it once every node refused; a node that refuses beside one that lets the backend
// in, or beside one that is only unreachable for a while, doesn't end it. A Redis Cluster's ACL is each node's own.
//
// The backend is installed once per process, for one deployment, so the app's Cluster starts from the same two
// addresses in every row: a forwarder to each node, which the app's client also reaches the node's own address through.
import { inspect } from 'node:util'
import { setTimeout as sleep } from 'node:timers/promises'
import { Cluster, type Redis } from 'ioredis'
import { afterAll, describe, expect, it } from 'vitest'
import { Broadcast } from 'telefunc'
import { docker, forwarder, removeStores, runs, STORES, startStore, type Store } from '../test/stores.js'
import { installRedis } from './index.js'

const LOGIN_REFUSED =
  "refused the login of Telefunc's Redis backend (WRONGPASS): check the username and password of the client given to installRedis()"

type Node = Awaited<ReturnType<typeof startStore>>
type Forwarder = Awaited<ReturnType<typeof forwarder>>
type Nodes = { primary: Node; replica: Node }

/** A new Cluster of two nodes, each behind its forwarder: `primary` serves every slot, `replica` replicates it. */
async function startCluster(store: Store, via: [Forwarder, Forwarder]): Promise<Nodes> {
  const primary = await startStore(store, '--cluster-enabled', 'yes')
  const replica = await startStore(store, '--cluster-enabled', 'yes')
  via[0].to(primary.host)
  via[1].to(replica.host)
  await primary.admin.call('CLUSTER', 'ADDSLOTSRANGE', '0', '16383')
  await replica.admin.call('CLUSTER', 'MEET', primary.host, '6379')
  const primaryId = await primary.admin.call('CLUSTER', 'MYID')
  if (typeof primaryId !== 'string') throw new Error(`${store.name}: CLUSTER MYID answered ${String(primaryId)}`)
  // The replica learns the primary over the Cluster bus, and the primary learns it is replicated the same way.
  for (let tries = 0; ; tries++) {
    const replicating = await replica.admin.call('CLUSTER', 'REPLICATE', primaryId).then(
      () => true,
      () => false,
    )
    if (replicating) break
    if (tries > 200) throw new Error(`${store.name}: the replica never learned the primary`)
    await sleep(25)
  }
  // A client learns the Cluster's nodes from CLUSTER SLOTS, which lists a replica only once it has replicated.
  for (let tries = 0; ; tries++) {
    const [info, slots] = await Promise.all([
      primary.admin.call('CLUSTER', 'INFO'),
      primary.admin.call('CLUSTER', 'SLOTS'),
    ])
    if (String(info).includes('cluster_state:ok') && nodesServing(slots) === 2) break
    if (tries > 400) throw new Error(`${store.name}: the Cluster never listed its replica`)
    await sleep(25)
  }
  return { primary, replica }
}

/** How many nodes CLUSTER SLOTS lists for the Cluster's one range of slots: its start, its end, then each node. */
function nodesServing(slots: unknown): number {
  const [range] = Array.isArray(slots) ? slots : []
  return Array.isArray(range) ? range.length - 2 : 0
}

/** The node's ACL lets the app's login in, or refuses it from now on; a connection already logged in stays. */
async function login(admin: Redis, accepted: boolean): Promise<void> {
  const password = accepted ? '>pw' : '>another-password'
  await admin.call('ACL', 'SETUSER', 'telefunc', 'on', 'resetpass', password, '~*', '&*', '+@all')
}

/** A new app Cluster of both forwarders, logging in as `telefunc`, installed as the backend's; the last row's ended
 *  first, so the backend's own client is new too. The nodes' own addresses, which CLUSTER SLOTS names, map to their
 *  forwarders. */
function installCluster(via: [Forwarder, Forwarder], { primary, replica }: Nodes): Cluster {
  const app = new Cluster(
    via.map(({ port }) => ({ host: '127.0.0.1', port })),
    {
      lazyConnect: true,
      clusterRetryStrategy: () => 100,
      natMap: {
        [`${primary.host}:6379`]: { host: '127.0.0.1', port: via[0].port },
        [`${replica.host}:6379`]: { host: '127.0.0.1', port: via[1].port },
      },
      redisOptions: { username: 'telefunc', password: 'pw' },
    },
  )
  app.on('error', () => {})
  app.on('node error', () => {})
  installRedis(app)
  return app
}

/** How `promise` ends within `deadline`: `'resolved'`, its error, or still waiting. */
function settled(promise: Promise<unknown>, deadline: number): Promise<unknown> {
  return Promise.race([
    promise.then(
      () => 'resolved',
      (error: unknown) => error,
    ),
    sleep(deadline).then(() => `still waiting after ${deadline} ms`),
  ])
}

/** Publishes `message` until the subscription receives it, for 5 s at most: a publish waits while the subscription on
 *  its key starts, so each gets 1 s. */
async function delivered(key: string, message: string, received: unknown[]): Promise<boolean> {
  const until = Date.now() + 5_000
  while (!received.includes(message) && Date.now() < until) {
    await settled(Promise.resolve(Broadcast.publish(key, message)), 1_000)
    await sleep(200)
  }
  return received.includes(message)
}

describe.runIf(runs)('a Cluster whose nodes refuse the login', () => {
  let via: [Forwarder, Forwarder] | undefined
  let app: Cluster | undefined
  /** The row's Cluster, and the app's client of it, which connects once used. */
  async function cluster(store: Store, primaryLogin: boolean, replicaLogin: boolean): Promise<Nodes> {
    via ??= [await forwarder(), await forwarder()]
    app?.disconnect()
    removeStores()
    const nodes = await startCluster(store, via)
    await login(nodes.primary.admin, primaryLogin)
    await login(nodes.replica.admin, replicaLogin)
    app = installCluster(via, nodes)
    return nodes
  }
  afterAll(() => {
    app?.disconnect()
    for (const proxy of via ?? []) proxy.close()
    removeStores()
  })

  for (const [i, store] of STORES.entries()) {
    it(`${store.name}: where every node refuses it, the subscription ends with 'denied', and so does a publish`, async () => {
      await cluster(store, false, false)

      const subscription = Broadcast.subscribe(`every-${i}`, () => {})
      let ready: unknown
      let published: unknown
      try {
        ready = await settled(subscription.ready, 10_000)
        published = await settled(Promise.resolve(Broadcast.publish(`every-${i}`, 'x')), 10_000)
      } finally {
        subscription()
      }

      const refused = {
        name: 'BackendError',
        code: 'denied',
        outcome: 'not-run',
        message: expect.stringContaining(LOGIN_REFUSED),
      }
      expect({ ready, published }).toMatchObject({ ready: refused, published: refused })
      // Telefunc's own error: nothing of ioredis's, such as its wording of each node's reply.
      expect(inspect(ready, { depth: 8 })).not.toMatch(/invalid username-password pair|lastNodeError|ClusterAllFailed/)
    }, 60_000)

    it(`${store.name}: where one node refuses it beside one that lets it in, the subscription lives on that one`, async () => {
      // A password changed node by node: the Cluster starts with both nodes, whose connections stay once the replica's
      // changes. A node that refuses before the Cluster uses it leaves the client's pool at once.
      const nodes = await cluster(store, true, true)
      const key = `beside-${i}`
      const received: unknown[] = []
      const subscription = Broadcast.subscribe(key, (message) => void received.push(message))
      let ready: unknown
      const drops: Record<string, boolean> = {}
      try {
        ready = await settled(subscription.ready, 10_000)
        drops.before = await delivered(key, 'before', received)
        await login(nodes.replica.admin, false)
        // Each drop moves the subscriber to the next node, so two drops take it to the refusing node whichever it began
        // on.
        for (const drop of ['after drop 1', 'after drop 2']) {
          await Promise.all(
            [nodes.primary, nodes.replica].map(({ admin }) => admin.call('CLIENT', 'KILL', 'TYPE', 'pubsub')),
          )
          drops[drop] = await delivered(key, drop, received)
        }
      } finally {
        subscription()
      }

      expect({ ready, drops }).toEqual({
        ready: 'resolved',
        drops: { before: true, 'after drop 1': true, 'after drop 2': true },
      })
    }, 60_000)

    it(`${store.name}: where one node refuses it beside one that is unreachable for a while, the subscription waits it out`, async () => {
      const nodes = await cluster(store, true, false)
      // The primary's process stops: its connections open, and nothing answers on them.
      docker('pause', nodes.primary.name)
      const key = `unreachable-${i}`
      const received: unknown[] = []
      let paused = true
      const subscription = Broadcast.subscribe(key, (message) => void received.push(message))
      let whileUnreachable: unknown
      let ready: unknown
      let after: unknown
      try {
        // Longer than the subscriber's longest wait between two attempts, 2 s, and than a Cluster's attempt on the
        // stopped node, 1 s.
        whileUnreachable = await settled(subscription.ready, 3_000)
        docker('unpause', nodes.primary.name)
        paused = false
        ready = await settled(subscription.ready, 20_000)
        after = await delivered(key, 'after', received)
      } finally {
        subscription()
        if (paused) docker('unpause', nodes.primary.name)
      }

      expect({ whileUnreachable, ready, after }).toEqual({
        whileUnreachable: 'still waiting after 3000 ms',
        ready: 'resolved',
        after: true,
      })
    }, 60_000)
  }
})
