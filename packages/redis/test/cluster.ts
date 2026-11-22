// A Cluster for the store specs, each node behind a forwarder, and an app client of it.
export { startCluster, appCluster, listed, type Forwarder, type Nodes }

import { setTimeout as sleep } from 'node:timers/promises'
import { Cluster, type ClusterOptions } from 'ioredis'
import { forwarder, startStore, type Store } from './stores.js'

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
  const started: Node[] = []
  for (const proxy of via) {
    const node = await startStore(store, '--cluster-enabled', 'yes')
    proxy.to(node.host)
    started.push(node)
  }
  const [primary, replica, spare] = started
  if (primary === undefined || replica === undefined) throw new Error('a Cluster takes two forwarders or three')
  await primary.admin.call('CLUSTER', 'ADDSLOTSRANGE', '0', '16383')
  for (const node of started.slice(1)) await node.admin.call('CLUSTER', 'MEET', primary.host, '6379')
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
  // A client learns the Cluster's nodes from CLUSTER SLOTS, which lists a replica only once it has replicated, and
  // every node must know every other for a slot to move between them.
  for (let tries = 0; ; tries++) {
    const [info, slots] = await Promise.all([
      primary.admin.call('CLUSTER', 'INFO'),
      primary.admin.call('CLUSTER', 'SLOTS'),
    ])
    const known = await Promise.all(started.map(({ admin }) => admin.call('CLUSTER', 'NODES')))
    const everyNode = known.every((list) => String(list).trim().split('\n').length === started.length)
    if (String(info).includes('cluster_state:ok') && nodesServing(slots) === 2 && everyNode) break
    if (tries > 400) throw new Error(`${store.name}: the Cluster never listed its replica`)
    await sleep(25)
  }
  return spare === undefined ? { primary, replica } : { primary, replica, spare }
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
