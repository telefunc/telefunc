// A Cluster of two nodes for the store specs, each behind a forwarder, and an app client of it.
export { startCluster, appCluster, type Forwarder, type Nodes }

import { setTimeout as sleep } from 'node:timers/promises'
import { Cluster, type ClusterOptions } from 'ioredis'
import { forwarder, startStore, type Store } from './stores.js'

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

/** A new app Cluster of both forwarders, logging in as `telefunc`, which connects once used unless `options` say
 *  otherwise. The nodes' own addresses, which CLUSTER SLOTS names, map to their forwarders. */
function appCluster(via: [Forwarder, Forwarder], { primary, replica }: Nodes, options: ClusterOptions = {}): Cluster {
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
      ...options,
    },
  )
  app.on('error', () => {})
  app.on('node error', () => {})
  return app
}
