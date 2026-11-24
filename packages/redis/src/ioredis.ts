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
  redisFailure,
  isRefusal,
  orderRecordRefusal,
  staleStart,
  slotOf,
  formerMaster,
}
export type { RedisClient, SubscriberSocket, FormerMaster }

import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { Cluster, Command, Redis } from 'ioredis'
import { assert, isBug } from './assert.js'
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

/** How Redis 5 and 6 and Dragonfly wrap the refusal of a call a script makes. */
const IN_SCRIPT = String.raw`ERR Error running script \(call to (?:f_)?[0-9a-f]+\): @user_script:\d+: (?:@user_script: \d+: )?`

/** Redis's refusal of a command its user's ACL denies, at top level or inside a script: Redis 7 and later word a
 *  script's ACL refusal as `ERR`. */
const ACL_REFUSAL = new RegExp(
  String.raw`^(?:NOPERM |ERR ACL failure in script: |ERR The user executing the script |${IN_SCRIPT}(?:The user executing the script |-NOPERM ))`,
)

/** A replica's refusal of a write, at top level or inside a script. */
const READ_ONLY = new RegExp(String.raw`^(?:READONLY |${IN_SCRIPT}-READONLY )`)

/** Redis's refusal of a login: Redis 6 and later, and Redis 5's `AUTH`. */
const LOGIN_REFUSAL = /^(?:WRONGPASS |NOAUTH |ERR invalid password)/

/** What to do about a failed command, by Redis's error code. */
const WHAT_TO_DO: Record<string, string> = {
  BUSY: "another client's script keeps Redis busy; end it with SCRIPT KILL",
  LOADING: 'Redis is loading its data; the backend can use it once that ends',
  OOM: 'Redis reached its maxmemory; raise it, with maxmemory-policy noeviction',
  WRONGTYPE: "a key under Telefunc's prefix holds a value Telefunc didn't write; keep that prefix to Telefunc",
}

/** A failure of Redis, or of the connection to it, as the backend reports it: what failed and what to do, with Redis's
 *  error code (the first word of its reply) or the connection's. It holds none of what ioredis' error does: the command,
 *  its arguments (a password, a message's bytes) or a nested error. */
class RedisFailure extends Error {
  /** `NOPERM`, `WRONGPASS`, `ECONNREFUSED` and the like. */
  readonly code: string | undefined
  /** Redis refused it, by its user's ACL or as a replica, or a key holds what Telefunc can't go on from, which stands
   *  until an operator changes the ACL, the role or the key. */
  readonly refused: boolean
  constructor(message: string, code: string | undefined, refused: boolean) {
    super(message)
    this.code = code
    this.refused = refused
  }
}

/** What the backend reports of an error ioredis gave on a connection of `redis`'s: the one place a failure of Redis
 *  becomes Telefunc's. An error the backend's own `assert()` threw passes as it is, and so does one this made. */
function redisFailure(error: unknown, redis: RedisClient): unknown {
  if (error instanceof RedisFailure || isBug(error)) return error
  const where = addressOf(redis)
  if (!(error instanceof Error) || error.name !== 'ReplyError') {
    const code = connectionCode(error)
    return new RedisFailure(
      `Telefunc's Redis backend can't reach Redis at ${where}${code === undefined ? '' : ` (${code})`}`,
      code,
      false,
    )
  }
  const { message } = error
  const code = replyCode(message)
  const tagged = code === undefined ? '' : ` (${code})`
  if (READ_ONLY.test(message)) {
    return new RedisFailure(
      `Telefunc's Redis backend reached a replica at ${where}, which refuses writes${tagged}: give installRedis() a ` +
        'client of the primary',
      code,
      true,
    )
  }
  if (ACL_REFUSAL.test(message)) {
    return new RedisFailure(
      `Redis at ${where} refused Telefunc's Redis backend a command, key or channel by its user's ACL${tagged}: allow ` +
        'that user what the ACL rule in the docs of @telefunc/redis lists',
      code,
      true,
    )
  }
  if (LOGIN_REFUSAL.test(message)) {
    return new RedisFailure(
      `Redis at ${where} refused the login of Telefunc's Redis backend${tagged}: check the username and password of ` +
        'the client given to installRedis()',
      code,
      false,
    )
  }
  const info = /^TFINFO (.+)$/.exec(message)
  if (info !== null) {
    return new RedisFailure(
      `Redis at ${where} refused Telefunc's Redis backend ${info[1]} by its user's ACL, which a subscription's start ` +
        'reads: allow that user +info, and +cluster|info on a Cluster, as the ACL rule in the docs of @telefunc/redis lists',
      'TFINFO',
      true,
    )
  }
  if (message.startsWith('TFSTALE ')) return staleStart(where)
  const order = /^TFORDER (.+?): /.exec(message)
  if (order !== null) return orderRecordRefusal(order[1]!)
  const toDo = code === undefined ? undefined : WHAT_TO_DO[code]
  return new RedisFailure(
    `Redis at ${where} failed a command of Telefunc's Redis backend${tagged}` +
      (toDo === undefined ? ": Redis's log may say why" : `: ${toDo}`),
    code,
    // A key's value stands until an operator changes it, as a refusal does.
    code === 'WRONGTYPE',
  )
}

/** A record at `key` that commands.ts's scripts can't go on from: another client wrote it, or its next count would pass
 *  2^53 - 1. It stands until an operator changes the key, as a refusal does. */
function orderRecordRefusal(key: string): RedisFailure {
  return new RedisFailure(
    `Telefunc's Redis backend can't go on from the order record at ${key}: it isn't one Telefunc writes, or its next ` +
      "count would pass 2^53 - 1; keep Telefunc's prefix to Telefunc",
    'TFORDER',
    true,
  )
}

/** A subscription's start read from a replica, or across two histories: it is read again, once the primary answers. */
function staleStart(where: string): RedisFailure {
  return new RedisFailure(
    `Telefunc's Redis backend read a subscription's start at ${where} from a replica or across a change of Redis's ` +
      'history: it reads it again',
    'TFSTALE',
    false,
  )
}

/** The first word of the reply a script's call got, where it is a code, else of Redis's reply. */
function replyCode(message: string): string | undefined {
  const inScript = new RegExp(`^${IN_SCRIPT}-?`).exec(message)
  const words = [message.split(' ', 1)[0]]
  if (inScript !== null) words.unshift(message.slice(inScript[0].length).split(' ', 1)[0])
  return words.find((word) => word !== undefined && /^[A-Z][A-Z_]*$/.test(word))
}

/** The system's code of a failed connection, such as `ECONNREFUSED`. */
function connectionCode(error: unknown): string | undefined {
  if (!(error instanceof Error) || !('code' in error) || typeof error.code !== 'string') return undefined
  return /^E[A-Z]+$/.test(error.code) ? error.code : undefined
}

/** Redis refused it, by its user's ACL or as a replica, or a key holds what Telefunc can't go on from; `BUSY`,
 *  `LOADING` and the like pass. */
function isRefusal(error: unknown): error is RedisFailure {
  return error instanceof RedisFailure && error.refused
}

/** ioredis's own default. */
const SLOTS_REFRESH_MS = 5_000

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
        // How soon a failover is seen by the subscriptions, which its slot map tells (RedisConnection), whatever the
        // app's client refreshes at.
        slotsRefreshInterval: SLOTS_REFRESH_MS,
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

/** A key's Cluster slot. */
function slotOf(key: string): number {
  return new Command('exists', [key]).getSlot()
}

/** A Cluster node a slot had, over a connection of its own that is READONLY, so that as a replica it answers a start
 *  read from its own data: its role, its history (its replication id) and the one before it (`master_replid2`), which a
 *  partial resync to a new master leaves it. */
type FormerMaster = {
  readonly client: Redis
  readonly master: boolean
  readonly history: string
  readonly previous: string | null
  /** Whether, as master, it serves `key`'s slot: a command on the key that it redirects (`MOVED`, or `ASK` while it
   *  migrates the slot's keys) says it doesn't; null where it answers otherwise. */
  serves(key: string): Promise<boolean | null>
  close(): void
}

/** How long the backend waits on the master a slot had, which a failover may have taken down. */
const FORMER_MASTER_MS = 2_000

/** `node`, a Cluster node as the slot map named it, with `define`'s scripts; null where it can't be reached or answers
 *  otherwise. */
async function formerMaster(
  cluster: Cluster,
  node: string,
  define: (redis: Redis) => void,
): Promise<FormerMaster | null> {
  const at = node.lastIndexOf(':')
  const redis = new Redis({
    ...cluster.options.redisOptions,
    host: node.slice(0, at),
    port: Number(node.slice(at + 1)),
    lazyConnect: true,
    connectTimeout: FORMER_MASTER_MS,
    commandTimeout: FORMER_MASTER_MS,
    maxRetriesPerRequest: 0,
    retryStrategy: () => null,
  })
  redis.on('error', () => {})
  let info: string | undefined
  try {
    await redis.connect()
    const [[infoError, reply], [readOnlyError]] = (await redis.pipeline().info('replication').readonly().exec()) as [
      [Error | null, string],
      [Error | null, unknown],
    ]
    if (infoError === null && readOnlyError === null) info = reply
  } catch {}
  const history = info === undefined ? undefined : /^master_replid:(\w+)/m.exec(info)?.[1]
  if (history === undefined) {
    redis.disconnect()
    return null
  }
  define(redis)
  const previous = /^master_replid2:(\w+)/m.exec(info!)?.[1]
  return {
    client: redis,
    master: /^role:master\r?$/m.test(info!),
    history,
    // A replica that never followed another history has an all-zero one.
    previous: previous === undefined || /^0+$/.test(previous) ? null : previous,
    async serves(key) {
      try {
        await redis.exists(key)
        return true
      } catch (error) {
        return error instanceof Error && /^(?:MOVED|ASK) /.test(error.message) ? false : null
      }
    },
    close: () => redis.disconnect(),
  }
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

/** Invoke a command registered via `defineCommand`: ioredis attaches it as a dynamic method TypeScript can't see. Its
 *  arguments are `parts`, in order. */
function callDefinedCommand(
  redis: RedisClient,
  command: string,
  ...parts: ReadonlyArray<string | Uint8Array>[]
): Promise<unknown> {
  const fn = (redis as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[command]
  assert(typeof fn === 'function', `Redis command "${command}" was not registered via defineCommand`)
  // Each part an array argument, which ioredis flattens into the command: an incarnation's keys can outnumber a call's
  // arguments.
  return fn.apply(redis, parts)
}
