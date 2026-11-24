// A Cluster for the store specs, each node behind a forwarder, and an app client of it.
export { startCluster, appCluster, clusterBackend, listed, type Forwarder, type Nodes }

import { setTimeout as sleep } from 'node:timers/promises'
import { Cluster, type ClusterOptions } from 'ioredis'
import { installRedis } from '../src/index.js'
import { reaches } from './acl.js'
import { forwarder, removeStores, startStore, type Store } from './stores.js'

type Node = Awaited<ReturnType<typeof startStore>>
type Forwarder = Awaited<ReturnType<typeof forwarder>>
/** `spare` is a primary that serves no slot until a row moves one to it. */
type Nodes = { primary: Node; replica: Node; spare?: Node }

/** The nodes in the order of their forwarders. */
function listed({ primary, replica, spare }: Nodes): Node[] {
  return spare === undefined ? [primary, replica] : [primary, replica, spare]
}

/** A new Cluster, each node behind its forwarder in `via`'s order: `primary` serves every slot, `replica` replicates it,
 *  and with a third forwarder, `spare` is a primary of no slot. */
async function startCluster(store: Store, via: readonly Forwarder[]): Promise<Nodes> {
  const [toPrimary, toReplica, toSpare] = via
  if (toPrimary === undefined || toReplica === undefined) throw new Error('a Cluster takes two forwarders or three')
  const started: Node[] = []
  // Each node joins right after its own start: the first takes every slot, each other one meets every node started
  // before it, so none waits for the Cluster's gossip to learn of another. A master that has no slot to count, its own
  // or a node's it knows, more than 2 s after its start counts itself cut off, and once it has one answers CLUSTERDOWN
  // for 5 more seconds (`clusterUpdateState()`, CLUSTER_WRITABLE_DELAY and the rejoin delay); within those 2 s it
  // doesn't, and a master says ok no sooner than 2 s after its start.
  const join = async (proxy: Forwarder): Promise<Node> => {
    // CLUSTER SLOTS lists a replica only once its replication offset is past 0, which an empty primary's first
    // replication PING makes: these start the replica's sync at once and send that PING every second, where by default
    // the sync waits 5 s and the PING comes 10 s after the primary's start, as long as the wait for the listing below.
    const node = await startStore(
      store,
      ...['--cluster-enabled', 'yes'],
      ...['--repl-diskless-sync-delay', '0', '--repl-ping-replica-period', '1'],
    )
    proxy.to(node.host)
    if (started.length === 0) await node.admin.call('CLUSTER', 'ADDSLOTSRANGE', '0', '16383')
    else await Promise.all(started.map(({ host }) => node.admin.call('CLUSTER', 'MEET', host, '6379')))
    started.push(node)
    return node
  }
  const primary = await join(toPrimary)
  // The spare starts before the replica, so that its 2 s pass while the replica starts and syncs.
  const spare = toSpare === undefined ? undefined : await join(toSpare)
  const replica = await join(toReplica)
  const [primaryId, replicaId] = await Promise.all([primary, replica].map(({ admin }) => admin.call('CLUSTER', 'MYID')))
  if (typeof primaryId !== 'string' || typeof replicaId !== 'string') {
    throw new Error(`${store.name}: CLUSTER MYID answered ${String(primaryId)} and ${String(replicaId)}`)
  }
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
  // A client learns the Cluster's nodes from CLUSTER SLOTS, which lists a replica only once it has replicated, every
  // node must know every other for a slot to move between them, and each must say the Cluster is ok, as a node that
  // doesn't answers a command on a slot it is given with CLUSTERDOWN. A master votes for the replica's failover only
  // once it knows the replica's primary, which CLUSTER REPLICATE doesn't announce: the master learns it from the
  // replica's next ping, and refuses its vote until then ("I don't know its master").
  for (let tries = 0; ; tries++) {
    const [infos, slots] = await Promise.all([
      Promise.all(started.map(({ admin }) => admin.call('CLUSTER', 'INFO'))),
      primary.admin.call('CLUSTER', 'SLOTS'),
    ])
    const known = await Promise.all(started.map(({ admin }) => admin.call('CLUSTER', 'NODES')))
    const everyNode = known.every((list) => String(list).trim().split('\n').length === started.length)
    const replicaKnown = known.every((list) => followsPrimary(String(list), replicaId, primaryId))
    const everyOk = infos.every((info) => String(info).includes('cluster_state:ok'))
    if (everyOk && nodesServing(slots) === 2 && everyNode && replicaKnown) break
    if (tries > 400) throw new Error(`${store.name}: the Cluster never listed its replica`)
    await sleep(25)
  }
  return spare === undefined ? { primary, replica } : { primary, replica, spare }
}

/** Whether a node's CLUSTER NODES lists `replicaId` as a replica of `primaryId`: each line is the node's id, its
 *  address, its flags, then its primary's id. */
function followsPrimary(nodes: string, replicaId: string, primaryId: string): boolean {
  return nodes
    .split('\n')
    .map((line) => line.split(' '))
    .some(([id, , flags, primary]) => id === replicaId && flags?.split(',').includes('slave') && primary === primaryId)
}

/** How many nodes CLUSTER SLOTS lists for the Cluster's one range of slots: its start, its end, then each node. */
function nodesServing(slots: unknown): number {
  const [range] = Array.isArray(slots) ? slots : []
  return Array.isArray(range) ? range.length - 2 : 0
}

/** A new app Cluster of the forwarders, logging in as `telefunc`, which connects once used unless `options` say
 *  otherwise. The nodes' own addresses, which CLUSTER SLOTS names, map to their forwarders. */
function appCluster(via: readonly Forwarder[], nodes: Nodes, options: ClusterOptions = {}): Cluster {
  const natMap: Record<string, { host: string; port: number }> = {}
  for (const [i, node] of listed(nodes).entries()) {
    const proxy = via[i]
    if (proxy === undefined) throw new Error('a node has no forwarder')
    natMap[`${node.host}:6379`] = { host: '127.0.0.1', port: proxy.port }
  }
  const app = new Cluster(
    via.map(({ port }) => ({ host: '127.0.0.1', port })),
    {
      lazyConnect: true,
      clusterRetryStrategy: () => 100,
      natMap,
      redisOptions: { username: 'telefunc', password: 'pw' },
      ...options,
    },
  )
  app.on('error', () => {})
  app.on('node error', () => {})
  return app
}

/** A spec file's backend on a Cluster: `reached()` starts a new Cluster for a row, whose nodes have a user of `+@all`,
 *  and installs the backend on a client of that user once it reaches the Cluster; `end()`, the file's `afterAll`, ends
 *  the client and the forwarders. */
function clusterBackend() {
  let app: Cluster | undefined
  let via: Forwarder[] | undefined
  return {
    async reached(store: Store): Promise<Nodes> {
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
    },
    end(): void {
      app?.disconnect()
      for (const proxy of via ?? []) proxy.close()
    },
  }
}
