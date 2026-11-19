// ioredis policy: a client's deployment, the connection the backend's commands run on, subscriber creation, and calls
// to defined commands.
export {
  createCommandClient,
  createSubscriberSocket,
  callDefinedCommand,
  isCluster,
  deploymentOf,
  connectsAlike,
  addressOf,
  isUnreachable,
  isRefusal,
  isReadOnly,
  ReplicaRefusal,
}
export type { RedisClient, SubscriberSocket }

import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { Cluster, type Redis } from 'ioredis'
import { assert } from './assert.js'
import { getGlobalObject } from './getGlobalObject.js'

type RedisClient = Redis | Cluster

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
  // ioredis keeps a Cluster's startup nodes private; its own duplicate() reads the same field.
  if (isCluster(redis)) return JSON.stringify((redis as unknown as { startupNodes: unknown }).startupNodes)
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

/** Where a client connects, as a person reads it: its startup nodes, socket path, Sentinel master or host:port. */
function addressOf(redis: RedisClient): string {
  if (isCluster(redis)) {
    const nodes = (redis as unknown as { startupNodes: Array<{ host?: string; port?: number } | string | number> })
      .startupNodes
    return nodes.map((node) => (typeof node === 'object' ? `${node.host}:${node.port}` : String(node))).join(', ')
  }
  const { path, host, port, sentinels, name } = redis.options
  if (path) return path
  return sentinels?.length ? `Sentinel master ${name}` : `${host}:${port}`
}

/** ioredis' errors for a command it couldn't send: with no retries, an unreachable Redis fails it at once. */
function isUnreachable(error: unknown): boolean {
  if (!(error instanceof Error)) return false
  return (
    error.name === 'MaxRetriesPerRequestError' ||
    error.name === 'ClusterAllFailedError' ||
    error.message === 'Connection is closed.' ||
    error.message === 'None of startup nodes is available'
  )
}

/** Redis's refusal of a command its user's ACL denies or a replica won't run, at top level or inside a script: Redis 7
 *  and later word a script's ACL refusal as `ERR`, and Redis 5 and 6 and Dragonfly wrap a script's refusal in the
 *  script's error. */
const REFUSAL =
  /^(?:NOPERM |READONLY |ERR ACL failure in script: |ERR The user executing the script |ERR Error running script \(call to (?:f_)?[0-9a-f]+\): @user_script:\d+: (?:@user_script: \d+: )?(?:The user executing the script |-READONLY |-NOPERM ))/

/** A replica's refusal of a write, at top level or inside a script. */
const READ_ONLY =
  /^(?:READONLY |ERR Error running script \(call to (?:f_)?[0-9a-f]+\): @user_script:\d+: (?:@user_script: \d+: )?-READONLY )/

/** A write a replica refused, as the backend reports it: it was given, or now reaches, a replica. */
class ReplicaRefusal extends Error {}

/** Redis refused the command, which stands until an operator changes its ACL or role; `BUSY`, `LOADING` and the like
 *  pass. */
function isRefusal(error: unknown): error is Error {
  if (error instanceof ReplicaRefusal) return true
  return error instanceof Error && error.name === 'ReplyError' && REFUSAL.test(error.message)
}

function isReadOnly(error: unknown): error is Error {
  return error instanceof Error && error.name === 'ReplyError' && READ_ONLY.test(error.message)
}

/** A clone of the app's client for the backend's commands: it never resends a command (at-most-once) and reads masters
 *  only (Room reads are strongly consistent). */
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
        // Autopipelining batches a Cluster command by the node of its first argument, which for a script with a
        // variable key count is the count, and refuses a batch whose keys span masters.
        enableAutoPipelining: false,
        redisOptions: { ...redis.options.redisOptions, maxRetriesPerRequest: 0, reconnectOnError: null },
      })
    : redis.duplicate({ maxRetriesPerRequest: 0, reconnectOnError: null })
  // A connection error reaches the backend as its command's rejection.
  commands.on('error', () => {})
  return commands
}

/** A fresh, unconnected subscriber that never retries on its own; on a Cluster it sits on a live node. */
async function createSubscriberSocket(redis: RedisClient): Promise<SubscriberSocket> {
  let source: Redis
  if (isCluster(redis)) {
    // A Cluster fills its node pool just before it is ready, and a lazyConnect one connects only on a command.
    if (redis.status === 'wait') await redis.connect()
    else if (redis.status === 'connecting') await clusterReady(redis)
    // Every Cluster node, replica or master, delivers every PUBLISH to its subscribers. A failed node stays pooled
    // until the Cluster uses it, and a promoted replica keeps its label until a refresh, so each reconnect starts after
    // the last node used.
    const nodes = redis.nodes('all').filter((candidate) => candidate.status !== 'end')
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

const { subscriberNodes, clusterWaits } = getGlobalObject('ioredis.ts', () => ({
  /** The node each Cluster's subscriber was last duplicated from. */
  subscriberNodes: new WeakMap<Cluster, Redis>(),
  /** One wait per connecting Cluster, however often subscribers reopen during it. */
  clusterWaits: new WeakMap<Cluster, Promise<void>>(),
}))

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
  // One array argument, which ioredis flattens into the command: an incarnation's keys can outnumber a call's
  // arguments.
  return fn.call(redis, keysAndArgs)
}
