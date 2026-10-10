// ioredis policy: the connection the backend's commands run on, subscriber creation, and calls to defined commands.
export { createCommandClient, createSubscriberSocket, callDefinedCommand, isCluster }
export type { RedisClient, SubscriberSocket }

import { randomUUID } from 'node:crypto'
import { Cluster, type Redis } from 'ioredis'
import { assert } from './assert.js'

type RedisClient = Redis | Cluster

/** What the subscription driver needs of a subscriber connection. */
type SubscriberSocket = {
  connect(): Promise<void>
  subscribe(...channels: string[]): Promise<unknown>
  unsubscribe(...channels: string[]): Promise<unknown>
  disconnect(): void
  on(event: 'messageBuffer', listener: (channel: Buffer, frame: Buffer) => void): unknown
  on(event: 'error', listener: (error: unknown) => void): unknown
  on(event: 'close', listener: () => void): unknown
}

function isCluster(redis: RedisClient): redis is Cluster {
  return redis instanceof Cluster
}

/** A clone of the app's client for the backend's commands: it never resends a command (at-most-once), reads masters
 *  only (Room reads are strongly consistent), and ends when the app's client does. */
function createCommandClient(redis: RedisClient): RedisClient {
  // A Cluster copies redisOptions.keyPrefix to its own options, so this reads either form.
  if (redis.options.keyPrefix)
    throw new Error(
      "installRedis(): ioredis keyPrefix isn't supported (it doesn't apply to Pub/Sub channels). Use installRedis(redis, { prefix }) instead",
    )
  const commands = isCluster(redis)
    ? redis.duplicate([], {
        scaleReads: 'master',
        retryDelayOnFailover: 0,
        // Autopipelining batches a Cluster command by the node of its first argument, which for a script with a
        // variable key count is the count, and refuses a batch whose keys span masters.
        enableAutoPipelining: false,
        redisOptions: { ...redis.options.redisOptions, maxRetriesPerRequest: 0, reconnectOnError: null },
      })
    : redis.duplicate({ maxRetriesPerRequest: 0, reconnectOnError: null })
  // A connection error reaches the backend as its command's rejection.
  commands.on('error', () => {})
  redis.once('end', () => commands.disconnect())
  return commands
}

/** A fresh, unconnected subscriber that never retries on its own; on a Cluster it sits on a live node. */
async function createSubscriberSocket(redis: RedisClient): Promise<SubscriberSocket> {
  let source: Redis
  if (isCluster(redis)) {
    // A Cluster fills its node pool just before it is ready, and a lazyConnect one connects only on a command.
    if (redis.status === 'wait') await redis.connect()
    else if (redis.status === 'connecting') await clusterReady(redis)
    // Every Cluster node, replica or master, delivers every PUBLISH to its subscribers. A failed node stays pooled until
    // the Cluster uses it, and a promoted replica keeps its label until a refresh, so each reconnect starts after the last
    // node used.
    const nodes = redis.nodes('all').filter((candidate) => candidate.status !== 'end')
    const last = subscriberNodes.get(redis)
    const node = nodes[last === undefined ? 0 : (nodes.indexOf(last) + 1) % nodes.length]
    if (node === undefined) throw new Error('Redis Cluster has no available nodes')
    subscriberNodes.set(redis, node)
    source = node
  } else source = redis
  return source.duplicate({
    connectionName: `telefunc-subscriber-${randomUUID()}`,
    autoResubscribe: false,
    lazyConnect: true,
    retryStrategy: () => null,
  })
}

// The node each Cluster's subscriber was last duplicated from.
const subscriberNodes = new WeakMap<Cluster, Redis>()

// One wait per connecting Cluster, however often subscribers reopen during it.
const clusterWaits = new WeakMap<Cluster, Promise<void>>()

function clusterReady(cluster: Cluster): Promise<void> {
  let wait = clusterWaits.get(cluster)
  if (wait) return wait
  wait = new Promise((resolve, reject) => {
    const settle = (error?: Error) => {
      clusterWaits.delete(cluster)
      cluster.off('ready', onReady)
      cluster.off('close', onClose)
      if (error) reject(error)
      else resolve()
    }
    const onReady = () => settle()
    // Every failed connect closes, whether the Cluster retries it or ends.
    const onClose = () => settle(new Error('Redis Cluster connection closed'))
    cluster.once('ready', onReady)
    cluster.once('close', onClose)
  })
  clusterWaits.set(cluster, wait)
  return wait
}

/** Invoke a command registered via `defineCommand`: ioredis attaches it as a dynamic method TypeScript can't see. */
function callDefinedCommand(
  redis: RedisClient,
  command: string,
  keysAndArgs: ReadonlyArray<string | Uint8Array>,
): Promise<unknown> {
  const fn = (redis as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[command]
  assert(typeof fn === 'function', `Redis command "${command}" was not registered via defineCommand`)
  // One array argument, which ioredis flattens into the command: a generation's keys can outnumber a call's arguments.
  return fn.call(redis, keysAndArgs)
}
