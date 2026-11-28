// A Cluster for the store specs, each node behind a forwarder, and an app client of it.
export {
  startCluster,
  startShardedCluster,
  appCluster,
  clusterBackend,
  shardedClusterRows,
  slotsOf,
  listed,
  type Forwarder,
  type Nodes,
  type Shard,
  type ShardedClusterRow,
}

import { setTimeout as sleep } from 'node:timers/promises'
import { Cluster, type ClusterOptions } from 'ioredis'
import { installRedis } from '../src/index.js'
import { reaches } from './acl.js'
import { endpoint, routeSubscribers, type Endpoint } from './endpoints.js'
import { forwarder, removeStores, startStore, type Store } from './stores.js'

type Node = Awaited<ReturnType<typeof startStore>>
type Forwarder = Awaited<ReturnType<typeof forwarder>>
/** `spare` is a primary that serves no slot until a row moves one to it. */
type Nodes = { primary: Node; replica: Node; spare?: Node }
/** A primary serving its share of the slots, and its replica. */
type Shard = { primary: Node; replica: Node }

/** The nodes in the order of their forwarders. */
function listed({ primary, replica, spare }: Nodes): Node[] {
  return spare === undefined ? [primary, replica] : [primary, replica, spare]
}

/** A new Cluster, each node behind its forwarder in `via`'s order: `primary` serves every slot, `replica` replicates it,
 *  and with a third forwarder, `spare` is a primary of no slot. */
async function startCluster(store: Store, via: readonly Forwarder[]): Promise<Nodes> {
  const [toPrimary, toReplica, toSpare] = via
  if (toPrimary === undefined || toReplica === undefined) throw new Error('a Cluster takes two forwarders or three')
  const {
    shards: [shard],
    spare,
  } = await formCluster(store, [{ primary: toPrimary, replica: toReplica }], toSpare)
  if (shard === undefined) throw new Error(`${store.name}: the Cluster has no shard`)
  return spare === undefined ? shard : { ...shard, spare }
}

/** A new Cluster of three primaries, each serving a third of the slots and replicated by a replica of its own: each
 *  node behind its forwarder in `via`'s order, a shard's primary then its replica. */
async function startShardedCluster(store: Store, via: readonly Forwarder[]): Promise<Shard[]> {
  const pairs = [0, 1, 2].map((i) => {
    const [primary, replica] = [via[2 * i], via[2 * i + 1]]
    if (primary === undefined || replica === undefined) throw new Error('a sharded Cluster takes six forwarders')
    return { primary, replica }
  })
  return (await formCluster(store, pairs)).shards
}

/** The slots the `i`th of `count` shards serves, as its first and its last. */
function slotsOf(i: number, count: number): [number, number] {
  const size = Math.ceil(16384 / count)
  return [i * size, Math.min(16383, (i + 1) * size - 1)]
}

/** A new Cluster of `shards`, each a primary serving its share of the slots and a replica of it, and a primary of no
 *  slot behind `toSpare` if given; each node behind its forwarder. */
async function formCluster(
  store: Store,
  shards: readonly { primary: Forwarder; replica: Forwarder }[],
  toSpare?: Forwarder,
): Promise<{ shards: Shard[]; spare: Node | undefined }> {
  const started: Node[] = []
  // Each node joins right after its own start: a primary takes its slots, and each node meets every node started before
  // it, so none waits for the Cluster's gossip to learn of another. A master that has no slot to count, its own or a
  // node's it knows, more than 2 s after its start counts itself cut off, and once it has one answers CLUSTERDOWN for 5
  // more seconds (`clusterUpdateState()`, CLUSTER_WRITABLE_DELAY and the rejoin delay); within those 2 s it doesn't,
  // and a master says ok no sooner than 2 s after its start.
  const join = async (proxy: Forwarder, slots?: [number, number]): Promise<Node> => {
    // CLUSTER SLOTS lists a replica only once its replication offset is past 0, which an empty primary's first
    // replication PING makes: these start the replica's sync at once and send that PING every second, where by default
    // the sync waits 5 s and the PING comes 10 s after the primary's start, as long as the wait for the listing below.
    const node = await startStore(
      store,
      ...['--cluster-enabled', 'yes'],
      ...['--repl-diskless-sync-delay', '0', '--repl-ping-replica-period', '1'],
    )
    proxy.to(node.host)
    if (slots !== undefined) await node.admin.call('CLUSTER', 'ADDSLOTSRANGE', String(slots[0]), String(slots[1]))
    await Promise.all(started.map(({ host }) => node.admin.call('CLUSTER', 'MEET', host, '6379')))
    started.push(node)
    return node
  }
  const primaries: Node[] = []
  for (const [i, { primary }] of shards.entries()) primaries.push(await join(primary, slotsOf(i, shards.length)))
  // The spare starts before the replicas, so that its 2 s pass while they start and sync.
  const spare = toSpare === undefined ? undefined : await join(toSpare)
  const replicas: Node[] = []
  for (const { replica } of shards) replicas.push(await join(replica))
  const idOf = async ({ admin }: Node) => {
    const id = await admin.call('CLUSTER', 'MYID')
    if (typeof id !== 'string') throw new Error(`${store.name}: CLUSTER MYID answered ${String(id)}`)
    return id
  }
  const formed = await Promise.all(
    primaries.map(async (primary, i) => {
      const replica = replicas[i]
      if (replica === undefined) throw new Error(`${store.name}: shard ${i} has no replica`)
      return { primary, replica, primaryId: await idOf(primary), replicaId: await idOf(replica) }
    }),
  )
  // A replica learns its primary over the Cluster bus, and the primary learns it is replicated the same way.
  for (const { replica, primaryId } of formed) {
    for (let tries = 0; ; tries++) {
      const replicating = await replica.admin.call('CLUSTER', 'REPLICATE', primaryId).then(
        () => true,
        () => false,
      )
      if (replicating) break
      if (tries > 200) throw new Error(`${store.name}: a replica never learned its primary`)
      await sleep(25)
    }
  }
  // A client learns the Cluster's nodes from CLUSTER SLOTS, which lists a replica only once it has replicated, every
  // node must know every other for a slot to move between them, and each must say the Cluster is ok, as a node that
  // doesn't answers a command on a slot it is given with CLUSTERDOWN. A master votes for a replica's failover only
  // once it knows the replica's primary, which CLUSTER REPLICATE doesn't announce: the master learns it from the
  // replica's next ping, and refuses its vote until then ("I don't know its master").
  const [first] = primaries
  if (first === undefined) throw new Error('a Cluster takes a shard')
  for (let tries = 0; ; tries++) {
    const [infos, slots] = await Promise.all([
      Promise.all(started.map(({ admin }) => admin.call('CLUSTER', 'INFO'))),
      first.admin.call('CLUSTER', 'SLOTS'),
    ])
    const known = await Promise.all(started.map(({ admin }) => admin.call('CLUSTER', 'NODES')))
    const everyNode = known.every((list) => String(list).trim().split('\n').length === started.length)
    const replicasKnown = known.every((list) =>
      formed.every(({ replicaId, primaryId }) => followsPrimary(String(list), replicaId, primaryId)),
    )
    const everyOk = infos.every((info) => String(info).includes('cluster_state:ok'))
    if (everyOk && everyRangeReplicated(slots, shards.length) && everyNode && replicasKnown) break
    if (tries > 800) throw new Error(`${store.name}: the Cluster never listed its replicas`)
    await sleep(25)
  }
  return { shards: formed.map(({ primary, replica }) => ({ primary, replica })), spare }
}

/** Whether a node's CLUSTER NODES lists `replicaId` as a replica of `primaryId`: each line is the node's id, its
 *  address, its flags, then its primary's id. */
function followsPrimary(nodes: string, replicaId: string, primaryId: string): boolean {
  return nodes
    .split('\n')
    .map((line) => line.split(' '))
    .some(([id, , flags, primary]) => id === replicaId && flags?.split(',').includes('slave') && primary === primaryId)
}

/** Whether CLUSTER SLOTS lists one range of slots for each of the `shards`, each with two nodes, its primary and its
 *  replica: a range is its start, its end, then each node. */
function everyRangeReplicated(slots: unknown, shards: number): boolean {
  const ranges: unknown[] = Array.isArray(slots) ? slots : []
  return ranges.length === shards && ranges.every((range) => Array.isArray(range) && range.length - 2 === 2)
}

/** A new app Cluster of the forwarders, logging in as `telefunc`, which connects once used unless `options` say
 *  otherwise. The nodes' own addresses, which CLUSTER SLOTS names, map to their forwarders, `nodes` in `via`'s order. */
function appCluster(via: readonly Forwarder[], nodes: readonly Node[], options: ClusterOptions = {}): Cluster {
  const natMap: Record<string, { host: string; port: number }> = {}
  for (const [i, node] of nodes.entries()) {
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
      app = appCluster(proxies, listed(nodes))
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

type ShardedClusterRow = {
  shards: Shard[]
  /** Each node's role endpoints, a shard's primary then its replica. */
  ends: Endpoint[]
  app: Cluster
}

/** A spec file's rows on a Cluster of three primaries with a replica each (`startShardedCluster()`): `reached()` starts a
 *  row's own Cluster, whose nodes have a user of `+@all`, points the file's role endpoints at its nodes, forgetting what
 *  they recorded and held, and installs the backend on an app Cluster of that user, once the backend reaches it. A
 *  node's commands take its node's command endpoint, a subscriber the subscriber endpoint of the node it sits on.
 *  `end()`, the file's `afterAll`, ends the client and the forwarders. `options` are the app Cluster's. */
function shardedClusterRows(options: ClusterOptions = {}) {
  let ends: Endpoint[] | undefined
  let app: Cluster | undefined
  return {
    async reached(store: Store): Promise<ShardedClusterRow> {
      app?.disconnect()
      removeStores()
      if (ends === undefined) {
        const made = await Promise.all([0, 1, 2, 3, 4, 5].map(() => endpoint('')))
        const subscriberOf = new Map(made.map(({ command, subscriber }) => [command.port, subscriber]))
        routeSubscribers((source) => {
          const subscriber = subscriberOf.get(source.port ?? 0)
          if (subscriber === undefined)
            throw new Error(`a subscriber duplicated a node on port ${source.port}, no endpoint's`)
          return { port: subscriber.port }
        })
        ends = made
      }
      const shards = await startShardedCluster(
        store,
        ends.map(({ command }) => command),
      )
      const nodes = shards.flatMap(({ primary, replica }) => [primary, replica])
      for (const [i, { host, admin }] of nodes.entries()) {
        await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
        ends[i]?.to(host)
      }
      app = appCluster(
        ends.map(({ command }) => command),
        nodes,
        options,
      )
      installRedis(app)
      await reaches(`the ${store.name} Cluster`)
      return { shards, ends, app }
    },
    end(): void {
      app?.disconnect()
      for (const node of ends ?? []) node.close()
    },
  }
}
