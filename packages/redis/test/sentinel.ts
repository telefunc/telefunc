// A Sentinel topology for the store specs: a primary, its replica and three sentinels, each behind a forwarder, and a
// row's backend on an app client that finds the primary through the sentinels.
export { MASTER_NAME, startSentinels, sentinelRows, type Sentinels, type SentinelRow }

import { setTimeout as sleep } from 'node:timers/promises'
import { Redis, type RedisOptions } from 'ioredis'
import { installRedis } from '../src/index.js'
import { reaches } from './acl.js'
import { endpoint, routeSubscribers, type Endpoint } from './endpoints.js'
import { forwarder, type Forwarder } from './forwarder.js'
import { removeStores, startServer, startStore, type Store } from './stores.js'

/** The name the sentinels monitor the primary by. */
const MASTER_NAME = 'telefunc'

type Server = Awaited<ReturnType<typeof startStore>>
type Sentinels = { primary: Server; replica: Server; sentinels: Server[] }

/** A new primary, its replica and three sentinels that monitor it with their defaults and a quorum of two, once each
 *  sentinel knows the replica and the two others. */
async function startSentinels(store: Store): Promise<Sentinels> {
  const primary = await startStore(store)
  const replica = await startStore(store, '--replicaof', primary.host, '6379')
  // A sentinel rewrites its config file, so each writes its own before it starts.
  const config = ['port 26379', 'protected-mode no', `sentinel monitor ${MASTER_NAME} ${primary.host} 6379 2`]
  const script = `printf '${config.join('\\n')}\\n' > /tmp/sentinel.conf && exec ${store.server} /tmp/sentinel.conf --sentinel`
  const sentinels: Server[] = []
  for (let i = 0; i < 3; i++) {
    sentinels.push(await startServer(store, { port: 26379, entrypoint: 'sh', command: ['-c', script] }))
  }
  // A sentinel learns the replica from the primary's INFO, and the other sentinels from their hello messages on the
  // primary, each sent every 2 s.
  const knows = async ({ admin }: Server) => {
    const [replicas, others] = await Promise.all([
      admin.call('SENTINEL', 'REPLICAS', MASTER_NAME),
      admin.call('SENTINEL', 'SENTINELS', MASTER_NAME),
    ])
    return Array.isArray(replicas) && replicas.length === 1 && Array.isArray(others) && others.length === 2
  }
  for (let tries = 0; !(await Promise.all(sentinels.map(knows))).every(Boolean); tries++) {
    if (tries > 1200) throw new Error(`${store.name}: the sentinels never knew the replica and each other`)
    await sleep(25)
  }
  for (let tries = 0; !/^master_link_status:up\r?$/m.test(await replica.admin.info('replication')); tries++) {
    if (tries > 400) throw new Error(`${store.name}: the replica never synced`)
    await sleep(25)
  }
  return { primary, replica, sentinels }
}

type SentinelRow = Sentinels & {
  /** The primary's and the replica's role endpoints. */
  ends: { primary: Endpoint; replica: Endpoint }
  /** Each sentinel's forwarder. */
  toSentinels: Forwarder[]
  app: Redis
}

/** A spec file's rows on a Sentinel topology each (`startSentinels()`): `reached()` starts a row's own topology, whose
 *  primary and replica have a user `telefunc` of `+@all`, points the file's endpoints at it, forgetting what they
 *  recorded and held, and installs the backend on an app client of that user which finds the primary through the
 *  sentinels' forwarders, once the backend reaches the primary. A server's command connections take its command
 *  endpoint, a subscriber its subscriber endpoint. `end()`, the file's `afterAll`, ends the client and the forwarders.
 *  `options` are the app client's. */
function sentinelRows(options: RedisOptions = {}) {
  let made: { ends: { primary: Endpoint; replica: Endpoint }; toSentinels: Forwarder[] } | undefined
  let app: Redis | undefined
  return {
    async reached(store: Store): Promise<SentinelRow> {
      app?.disconnect()
      removeStores()
      const topology = await startSentinels(store)
      const { primary, replica, sentinels } = topology
      for (const { admin } of [primary, replica]) {
        await admin.call('ACL', 'SETUSER', 'telefunc', 'on', '>pw', '~*', '&*', '+@all')
      }
      made ??= {
        ends: { primary: await endpoint(''), replica: await endpoint('') },
        toSentinels: [await forwarder(), await forwarder(), await forwarder()],
      }
      const { ends, toSentinels } = made
      ends.primary.to(primary.host)
      ends.replica.to(replica.host)
      // A sentinel names a server, and each other sentinel, by its own address, which maps to that server's endpoint of
      // the connection's role, and to that sentinel's forwarder.
      const sentinelAddresses: Record<string, { host: string; port: number }> = {}
      for (const [i, proxy] of toSentinels.entries()) {
        const sentinel = sentinels[i]
        if (sentinel === undefined) throw new Error('a sentinel forwarder has no sentinel')
        proxy.to(sentinel.host, 26379)
        proxy.clear()
        sentinelAddresses[`${sentinel.host}:26379`] = { host: '127.0.0.1', port: proxy.port }
      }
      const natMap = (role: 'command' | 'subscriber') => ({
        [`${primary.host}:6379`]: { host: '127.0.0.1', port: ends.primary[role].port },
        [`${replica.host}:6379`]: { host: '127.0.0.1', port: ends.replica[role].port },
        ...sentinelAddresses,
      })
      routeSubscribers(() => ({ natMap: natMap('subscriber') }))
      app = new Redis({
        sentinels: toSentinels.map(({ port }) => ({ host: '127.0.0.1', port })),
        name: MASTER_NAME,
        natMap: natMap('command'),
        username: 'telefunc',
        password: 'pw',
        lazyConnect: true,
        ...options,
      })
      app.on('error', () => {})
      installRedis(app)
      await reaches(`the ${store.name} primary`)
      return { ...topology, ends, toSentinels, app }
    },
    end(): void {
      app?.disconnect()
      made?.ends.primary.close()
      made?.ends.replica.close()
      for (const proxy of made?.toSentinels ?? []) proxy.close()
    },
  }
}
