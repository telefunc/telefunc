// The client library's facts: the command client and subscriber socket the backend builds from an app's client, a
// client's deployment and address, which raw commands write by ioredis's own command table, its wording for a command
// it never wrote. What a store's reply means is redisFailure.ts's.
export {
  createCommandClient,
  createSubscriberSocket,
  subscriberNodeCount,
  nextReady,
  ClusterNodesFailed,
  isCluster,
  rawWrites,
  deploymentOf,
  connectsAlike,
  addressOf,
  isReplyError,
  isUnsent,
}
export type { RedisClient, SubscriberSocket, RawCommand }

import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { hasFlag } from '@ioredis/commands'
import { Cluster, type Redis } from 'ioredis'
import { isObject } from 'telefunc/__internal'
import { getGlobalObject } from './getGlobalObject.js'

type RedisClient = Redis | Cluster

/** The client's commands the backend sends without a script, by their method's name: `getBuffer` is GET, its reply
 *  bytes. */
const RAW_COMMANDS = [
  'get',
  'getBuffer',
  'hget',
  'hkeys',
  'hmget',
  'smembers',
  'sscan',
  'unlink',
  'zrangebylex',
] as const
type RawCommand = (typeof RAW_COMMANDS)[number]

/** The raw commands that may write: those ioredis's own table of Redis's commands, which it routes by, doesn't flag
 *  `readonly`. Built once; the table throws here for a name it lacks. */
const WRITING: ReadonlySet<RawCommand> = new Set(
  RAW_COMMANDS.filter((name) => !hasFlag(name.replace(/Buffer$/, ''), 'readonly')),
)

/** Whether the raw command `name` may write, by ioredis's table: no flag of a raw command is written by hand. */
function rawWrites(name: RawCommand): boolean {
  return WRITING.has(name)
}

/** What `RedisSubscriptions` needs of its connection. */
type SubscriberSocket = {
  connect(): Promise<void>
  subscribe(...channels: string[]): Promise<unknown>
  unsubscribe(...channels: string[]): Promise<unknown>
  ping(): Promise<unknown>
  disconnect(): void
  on(event: 'messageBuffer', listener: (channel: Buffer, frame: Buffer) => void): unknown
  on(event: 'error', listener: (error: unknown) => void): unknown
  on(event: 'close', listener: () => void): unknown
}

function isCluster(redis: RedisClient): redis is Cluster {
  return redis instanceof Cluster
}

/** What a client connects to: a Cluster's startup nodes, or a server's address, database and Sentinel name. */
function deploymentOf(redis: RedisClient): string {
  if (isCluster(redis)) return JSON.stringify(startupNodesOf(redis))
  const { path, host, port, db, sentinels, name } = redis.options
  return JSON.stringify({ path, host, port, db, sentinels, name })
}

/** A client's options past its deployment that decide how it connects: its credentials, its TLS, and which server of a
 *  Sentinel group it takes. */
const CONNECTION_OPTIONS = [
  'username',
  'password',
  'tls',
  'role',
  'preferredSlaves',
  'natMap',
  'sentinelUsername',
  'sentinelPassword',
  'sentinelTLS',
  'enableTLSForSentinelMode',
] as const
/** A Cluster's own options that decide how it reaches its nodes; its nodes' connections take its `redisOptions`. */
const CLUSTER_CONNECTION_OPTIONS = ['natMap', 'dnsLookup', 'useSRVRecords'] as const

/** Whether two clients of one deployment connect alike, compared where they are, so no secret is copied or shown. */
function connectsAlike(a: RedisClient, b: RedisClient): boolean {
  if (isCluster(a) || isCluster(b)) {
    if (!isCluster(a) || !isCluster(b)) return false
    return (
      CLUSTER_CONNECTION_OPTIONS.every((name) => isDeepStrictEqual(a.options[name], b.options[name])) &&
      CONNECTION_OPTIONS.every((name) =>
        isDeepStrictEqual(a.options.redisOptions?.[name], b.options.redisOptions?.[name]),
      )
    )
  }
  return CONNECTION_OPTIONS.every((name) => isDeepStrictEqual(a.options[name], b.options[name]))
}

/** A Cluster's startup nodes, as the app gave them: ioredis keeps them private, and its own duplicate() reads the same
 *  field. */
function startupNodesOf(cluster: Cluster): unknown {
  return cluster['startupNodes']
}

/** Where a client connects, as a person reads it: its startup nodes, socket path, Sentinel master or host:port, and never
 *  a credential its URL holds. */
function addressOf(redis: RedisClient): string {
  if (isCluster(redis)) {
    const nodes = startupNodesOf(redis)
    return Array.isArray(nodes) ? nodes.map(nodeAddress).join(', ') : 'a Redis Cluster'
  }
  const { path, host, port, sentinels, name } = redis.options
  if (path) return path
  return sentinels?.length ? `Sentinel master ${name}` : `${host}:${port}`
}

/** A Cluster startup node's host and port as ioredis reads it (an object, a port, a URL or `host:port`, with its
 *  defaults), or its socket path. */
function nodeAddress(node: unknown): string {
  if (typeof node === 'number' || (typeof node === 'string' && /^\d+$/.test(node))) return `127.0.0.1:${node}`
  if (typeof node === 'string') {
    if (node.startsWith('/') && !node.startsWith('//')) return node
    try {
      const url = new URL(node.includes('//') ? node : `//${node}`, 'redis://localhost')
      return `${url.hostname || '127.0.0.1'}:${url.port || 6379}`
    } catch {
      return 'a startup node'
    }
  }
  if (!isObject(node)) return 'a startup node'
  const host = typeof node.host === 'string' ? node.host : '127.0.0.1'
  return `${host}:${typeof node.port === 'number' || typeof node.port === 'string' ? node.port : 6379}`
}

/** ioredis's rejection of a command it didn't write, as the backend's command client keeps no offline queue. */
const UNSENT = /^(?:Stream|Cluster) isn't (?:writeable|ready) and enableOfflineQueue options is false$/

/** ioredis's rejection of a command it never wrote. */
function isUnsent(error: unknown): boolean {
  return error instanceof Error && UNSENT.test(error.message)
}

/** An error Redis replied with, as ioredis gives it: by its name, as an app may load another copy of the errors' class. */
function isReplyError(error: unknown): error is Error {
  return error instanceof Error && error.name === 'ReplyError'
}

/** A clone of the app's client for the backend's commands: it never resends a command (at-most-once), never holds one
 *  it can't write yet (`nextReady()` waits for it to be ready instead), and reads masters only (Room reads are strongly
 *  consistent). */
function createCommandClient(redis: RedisClient): RedisClient {
  // A Cluster copies redisOptions.keyPrefix to its own options, so this reads either form.
  if (redis.options.keyPrefix) {
    throw new Error(
      "installRedis(): ioredis keyPrefix isn't supported (it doesn't apply to Pub/Sub channels). Use " +
        'installRedis(redis, { prefix }) instead',
    )
  }
  // ioredis's Sentinel connector takes the server whose role is `role`, the primary unless the app sets 'slave'.
  if (!isCluster(redis) && redis.options.sentinels?.length && redis.options.role === 'slave') {
    throw new Error(
      "installRedis(): the client's Sentinel role is 'slave', so it reaches a replica, and Telefunc writes to the " +
        'primary: leave out `role`',
    )
  }
  const commands = isCluster(redis)
    ? redis.duplicate([], {
        scaleReads: 'master',
        retryDelayOnFailover: 0,
        enableOfflineQueue: false,
        // Autopipelining batches a Cluster command by the node of its first argument, which for a script with a
        // variable key count is the count, and refuses a batch whose keys span masters.
        enableAutoPipelining: false,
        redisOptions: { ...redis.options.redisOptions, maxRetriesPerRequest: 0, reconnectOnError: null },
      })
    : redis.duplicate({ maxRetriesPerRequest: 0, reconnectOnError: null, enableOfflineQueue: false })
  // A connection error reaches the backend as its command's rejection.
  commands.on('error', () => {})
  return commands
}

/** A fresh, unconnected subscriber that never retries on its own; on a Cluster it sits on a live node. */
async function createSubscriberSocket(redis: RedisClient): Promise<SubscriberSocket> {
  let source: Redis
  if (isCluster(redis)) {
    // A Cluster fills its node pool just before it is ready.
    if (redis.status !== 'ready') await nextReady(redis)
    // Every Cluster node, replica or master, delivers every PUBLISH to its subscribers. A failed node stays pooled
    // until the Cluster uses it, and a promoted replica keeps its label until a refresh, so each reconnect starts after
    // the last node used.
    const nodes = subscriberNodesOf(redis)
    const last = subscriberNodes.get(redis)
    const node = nodes[last === undefined ? 0 : (nodes.indexOf(last) + 1) % nodes.length]
    if (node === undefined) throw new Error('Redis Cluster has no available nodes')
    subscriberNodes.set(redis, node)
    source = node
  } else {
    source = redis
  }
  return source.duplicate({
    connectionName: `telefunc-subscriber-${randomUUID()}`,
    autoResubscribe: false,
    lazyConnect: true,
    retryStrategy: () => null,
  })
}

/** The nodes a Cluster's subscriber may sit on, in its pool's order: a failed node stays pooled until the Cluster uses
 *  it. */
function subscriberNodesOf(redis: Cluster): Redis[] {
  return redis.nodes('all').filter((candidate) => candidate.status !== 'end')
}

/** How many nodes the subscriber tries in turn before a refusal of its login stands: each node of a Cluster, as each
 *  reconnect starts after the last node used (`createSubscriberSocket()`), and a server's one. */
function subscriberNodeCount(redis: RedisClient): number {
  return isCluster(redis) ? Math.max(subscriberNodesOf(redis).length, 1) : 1
}

const { subscriberNodes, readyWaits } = getGlobalObject('ioredis.ts', () => ({
  /** The node each Cluster's subscriber was last duplicated from. */
  subscriberNodes: new WeakMap<Cluster, Redis>(),
  /** Each client's wait for its next `ready` (`nextReady()`). */
  readyWaits: new WeakMap<RedisClient, Promise<void>>(),
}))

/** A Cluster's connect that closed after its nodes failed, with each node's last error (`nextReady()`): what the nodes
 *  replied tells a refused login from a node that is only unreachable (`redisFailure()`). Only the backend reads it. */
class ClusterNodesFailed extends Error {
  readonly nodeErrors: readonly unknown[]
  constructor(nodeErrors: readonly unknown[]) {
    super('Every node of the Redis Cluster that was tried failed')
    this.nodeErrors = nodeErrors
  }
}

/** The client's next `ready`, one wait for everything that waits meanwhile: the commands made while it reconnects, and
 *  the subscriber sockets a Cluster opens before it is ready. It rejects once the connection closes first, which is when
 *  ioredis would fail a command it held: with the client's last error, or for a Cluster whose nodes failed, with each
 *  node's (`ClusterNodesFailed`), as ioredis's own error keeps only the last node's. Every failed connect of a Cluster
 *  closes, whether it retries or ends. A client made with `lazyConnect` connects here. */
function nextReady(client: RedisClient): Promise<void> {
  const waiting = readyWaits.get(client)
  if (waiting !== undefined) return waiting
  if (client.status === 'end') return Promise.reject(new Error('Connection is closed.'))
  const promise = new Promise<void>((resolve, reject) => {
    let lastError: unknown = new Error('Connection is closed.')
    // By node: what each answered since the Cluster last read its slots, which it does from the first node that answers.
    const nodeErrors = new Map<string, unknown>()
    const onError = (error: unknown) => {
      lastError = error
    }
    const onNodeError = (error: unknown, node: string) => {
      nodeErrors.set(node, error)
    }
    const onRefresh = () => nodeErrors.clear()
    const settle = () => {
      client.off('error', onError)
      client.off('node error', onNodeError)
      client.off('refresh', onRefresh)
      client.off('ready', onReady)
      client.off('close', onClose)
      client.off('end', onClose)
      if (readyWaits.get(client) === promise) readyWaits.delete(client)
    }
    const onReady = () => {
      settle()
      resolve()
    }
    const onClose = () => {
      settle()
      reject(nodeErrors.size === 0 ? lastError : new ClusterNodesFailed([...nodeErrors.values()]))
    }
    client.on('error', onError)
    if (isCluster(client)) {
      client.on('node error', onNodeError)
      client.on('refresh', onRefresh)
    }
    client.once('ready', onReady)
    client.once('close', onClose)
    client.once('end', onClose)
  })
  readyWaits.set(client, promise)
  if (client.status === 'wait') void client.connect().catch(() => {})
  return promise
}
