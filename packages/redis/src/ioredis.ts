// The client library's facts: the command client and subscriber socket the backend builds from an app's client, the
// backend's requests as ioredis commands of its own, a client's deployment and address, which raw commands write by
// ioredis's own command table, its wording for a command it never wrote, timed out, or redirected until it gave up. What
// a store's reply means is redisFailure.ts's.
export {
  StoreRequest,
  sendRequest,
  scriptOf,
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
  isTimedOut,
  commandTimeoutOf,
  lastClusterReply,
  slotOf,
  formerMaster,
}
export type { RedisClient, SubscriberSocket, StoreScript, FormerMaster }

import { createHash, randomUUID } from 'node:crypto'
import { Writable } from 'node:stream'
import { isDeepStrictEqual } from 'node:util'
import { hasFlag } from '@ioredis/commands'
import { Cluster, Command, Redis } from 'ioredis'
import { isObject } from 'telefunc/__internal'
import { assert } from './assert.js'
import { twoOf } from './replies.js'
import { getGlobalObject } from './getGlobalObject.js'

type RedisClient = Redis | Cluster

/** Whether the raw command `name` may write: ioredis's own table of Redis's commands, which it routes by, doesn't flag it
 *  `readonly`. The table throws for a name it lacks; no flag of a raw command is written by hand. */
function rawWrites(name: string): boolean {
  return !hasFlag(name, 'readonly')
}

/** A script as the backend sends it: its source and its SHA1, by which a socket that ran it once runs it again. */
type StoreScript = { lua: string; sha: string }

/** Each script's SHA1, computed once. */
const scripts = new Map<string, StoreScript>()

function scriptOf(lua: string): StoreScript {
  let script = scripts.get(lua)
  if (script === undefined) {
    script = { lua, sha: createHash('sha1').update(lua).digest('hex') }
    scripts.set(lua, script)
  }
  return script
}

/** The scripts each socket ran, by SHA1: a socket's first request of a script sends its source (EVAL), later ones its
 *  SHA1 (EVALSHA), as Redis keeps a script it ran until it restarts or SCRIPT FLUSH. */
const socketScripts = new WeakMap<object, Set<string>>()

/** How much of a request the backend hands its socket at once: the socket's progress then shows as each slice is taken,
 *  and a large argument goes as slices of itself, never copied whole into one frame. */
const SLICE_BYTES = 1024 * 1024

/** One request of the backend, a raw command or a script, with its arguments (a script's: its key count, its keys, then
 *  its arguments), as an ioredis command of its own, sent with `sendRequest()`. ioredis serializes it onto the socket it
 *  writes it to (`toWritable()`): a script as EVAL or EVALSHA by what that socket ran, a request over a slice slice by
 *  slice. `bytes` keeps the reply's bulk strings as bytes. */
class StoreRequest extends Command {
  private readonly script: StoreScript | undefined
  /** The socket it was last written to. */
  private socket: object | undefined

  constructor(sent: { command: string } | { script: StoreScript }, args: (string | Buffer)[], bytes: boolean) {
    const script = 'script' in sent ? sent.script : undefined
    const name = 'command' in sent ? sent.command : 'evalsha'
    super(name, script === undefined ? args : [script.sha, ...args], { replyEncoding: bytes ? null : 'utf8' })
    this.script = script
  }

  /** Whether `error` is Redis's answer that the socket the request was written to no longer has its script (NOSCRIPT),
   *  which that socket then forgets: the request ran nothing, and a resend goes as EVAL. */
  lostScript(error: unknown): boolean {
    if (this.script === undefined || !isReplyError(error) || !error.message.startsWith('NOSCRIPT ')) return false
    if (this.socket !== undefined) socketScripts.get(this.socket)?.delete(this.script.sha)
    return true
  }

  override toWritable(socket: object): string | Buffer {
    this.socket = socket
    if (this.script !== undefined) {
      let ran = socketScripts.get(socket)
      if (ran === undefined) socketScripts.set(socket, (ran = new Set()))
      const again = ran.has(this.script.sha)
      ran.add(this.script.sha)
      this.name = again ? 'evalsha' : 'eval'
      this.args[0] = again ? this.script.sha : this.script.lua
    }
    const bytes = frameBytes(this.name, this.args)
    // A socket that isn't a Node stream gets the frame whole.
    if (bytes <= SLICE_BYTES || !(socket instanceof Writable)) return super.toWritable(socket)
    const slices = slicesOf(framePieces(this.name, this.args), bytes)
    const last = slices.pop()
    assert(last !== undefined)
    // ioredis writes what this returns right after, so the frame reaches the socket whole and in order.
    for (const slice of slices) socket.write(slice)
    return last
  }
}

/** The length in bytes of the RESP frame of a command `name` with `args`, as ioredis writes it: each argument is
 *  `$<length>\r\n<argument>\r\n`. */
function frameBytes(name: string, args: readonly unknown[]): number {
  let bytes = Buffer.byteLength(frameHead(name, args))
  for (const arg of args) {
    const length = Buffer.byteLength(argument(arg))
    bytes += String(length).length + length + 5
  }
  return bytes
}

/** The RESP frame of a command `name` with `args`, as the pieces ioredis joins. */
function framePieces(name: string, args: readonly unknown[]): (string | Buffer)[] {
  const pieces: (string | Buffer)[] = [frameHead(name, args)]
  for (const arg of args) pieces.push(`$${Buffer.byteLength(argument(arg))}\r\n`, argument(arg), '\r\n')
  return pieces
}

const frameHead = (name: string, args: readonly unknown[]) =>
  `*${args.length + 1}\r\n$${Buffer.byteLength(name)}\r\n${name}\r\n`

/** An argument as ioredis keeps it once it made the command: a string or bytes. */
function argument(arg: unknown): string | Buffer {
  assert(typeof arg === 'string' || Buffer.isBuffer(arg))
  return arg
}

/** The frame's pieces as slices of at most `SLICE_BYTES`: a piece longer than a slice is cut into views of itself. */
function slicesOf(pieces: readonly (string | Buffer)[], bytes: number): Buffer[] {
  const slices: Buffer[] = []
  let pending: Buffer[] = []
  let pendingBytes = 0
  const flush = () => {
    const [only] = pending
    slices.push(pending.length === 1 && only !== undefined ? only : Buffer.concat(pending, pendingBytes))
    pending = []
    pendingBytes = 0
  }
  for (const piece of pieces) {
    let left = typeof piece === 'string' ? Buffer.from(piece) : piece
    while (left.length > 0) {
      const taken = left.subarray(0, SLICE_BYTES - pendingBytes)
      pending.push(taken)
      pendingBytes += taken.length
      left = left.subarray(taken.length)
      if (pendingBytes === SLICE_BYTES) flush()
    }
  }
  if (pendingBytes > 0) flush()
  assert(slices.reduce((sum, slice) => sum + slice.length, 0) === bytes)
  return slices
}

/** Sends `request` on `client`: its reply, as its `bytes` says. */
function sendRequest(client: RedisClient, request: StoreRequest): Promise<unknown> {
  void client.sendCommand(request)
  return request.promise
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

/** ioredis's rejection of a command whose reply didn't come within its client's `commandTimeout`. */
function isTimedOut(error: unknown): boolean {
  return error instanceof Error && error.message === 'Command timed out'
}

/** How long a command of `redis` waits for its reply, a Cluster's by its nodes' options; undefined: without end. */
function commandTimeoutOf(redis: RedisClient): number | undefined {
  return isCluster(redis) ? redis.options.redisOptions?.commandTimeout : redis.options.commandTimeout
}

/** ioredis's rejection of a Cluster command once its redirections ran out, holding the reply that ended it. ioredis
 *  counts the app's `maxRedirections` down on every reply that fails a try, before it reads the reply, so at 0 the
 *  first reply ends the command, whatever it is (ioredis 5.10.1, `Cluster.handleError()`). */
const GAVE_UP = /^Too many Cluster redirections\. Last error: ReplyError: /

/** The reply that ended a Cluster command ioredis gave up once its redirections ran out, as Redis worded it. Every try
 *  before it was redirected, so it didn't run: ioredis tries a command again only on MOVED, ASK, TRYAGAIN and
 *  CLUSTERDOWN, each answered before the command runs, as the backend's command client never retries a closed
 *  connection (`retryDelayOnFailover: 0`, `createCommandClient()`). So the last reply tells what the command did. */
function lastClusterReply(error: unknown): string | undefined {
  if (!(error instanceof Error)) return undefined
  const gaveUp = GAVE_UP.exec(error.message)
  return gaveUp === null ? undefined : error.message.slice(gaveUp[0].length)
}

/** An error Redis replied with, as ioredis gives it: by its name, as an app may load another copy of the errors' class. */
function isReplyError(error: unknown): error is Error {
  return error instanceof Error && error.name === 'ReplyError'
}

/** ioredis's own default. */
const SLOTS_REFRESH_MS = 5_000

/** How long a command of the backend waits for its reply where the app's client sets no `commandTimeout`. */
const COMMAND_TIMEOUT_MS = 15_000

/** A clone of the app's client for the backend's commands: it never resends a command (at-most-once), never holds one
 *  it can't write yet (`nextReady()` waits for it to be ready instead), fails one whose reply doesn't come in time, and
 *  reads masters only (Room reads are strongly consistent). */
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
  const commandTimeout = commandTimeoutOf(redis) ?? COMMAND_TIMEOUT_MS
  const commands = isCluster(redis)
    ? redis.duplicate([], {
        scaleReads: 'master',
        // A closed connection fails its command (`lastClusterReply()` counts on it).
        retryDelayOnFailover: 0,
        enableOfflineQueue: false,
        // How soon a failover is seen by the subscriptions, which its slot map tells (RedisConnection), whatever the
        // app's client refreshes at.
        slotsRefreshInterval: SLOTS_REFRESH_MS,
        // Autopipelining batches a Cluster command by the node of its first argument, which for a script with a
        // variable key count is the count, and refuses a batch whose keys span masters.
        enableAutoPipelining: false,
        redisOptions: {
          ...redis.options.redisOptions,
          maxRetriesPerRequest: 0,
          reconnectOnError: null,
          commandTimeout,
        },
      })
    : redis.duplicate({
        maxRetriesPerRequest: 0,
        reconnectOnError: null,
        enableOfflineQueue: false,
        commandTimeout,
        // Autopipelining holds a command until the next tick and writes it in a batch, which alone reaches the socket:
        // each of the backend's commands is written when it is sent, as on its Cluster client.
        enableAutoPipelining: false,
      })
  recordAttempts(commands)
  return commands
}

/** Keeps what the client's connection attempt ended with, from the client's start, for `nextReady()`: a wait that
 *  starts after the attempt failed, or after the client ended, still rejects with it. Its last error, and for a
 *  Cluster each node's since the Cluster last read its slots, which it does from the first node that answers; a `ready`
 *  clears them. A connection error reaches the backend as its command's rejection, not as an event. */
function recordAttempts(client: RedisClient): void {
  const attempt: Attempt = { lastError: new Error('Connection is closed.'), nodeErrors: new Map() }
  attempts.set(client, attempt)
  client.on('error', (error: unknown) => {
    attempt.lastError = error
  })
  if (isCluster(client)) {
    client.on('node error', (error: unknown, node: string) => void attempt.nodeErrors.set(node, error))
    client.on('refresh', () => attempt.nodeErrors.clear())
  }
  client.on('ready', () => {
    attempt.lastError = new Error('Connection is closed.')
    attempt.nodeErrors.clear()
  })
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

/** `node`, a Cluster node as the slot map named it; null where it can't be reached or answers otherwise. */
async function formerMaster(cluster: Cluster, node: string): Promise<FormerMaster | null> {
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
  const info = await readOnlyReplication(redis)
  const history = info === null ? undefined : /^master_replid:(\w+)/m.exec(info)?.[1]
  if (info === null || history === undefined) {
    redis.disconnect()
    return null
  }
  const previous = /^master_replid2:(\w+)/m.exec(info)?.[1]
  return {
    client: redis,
    master: /^role:master\r?$/m.test(info),
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

/** What `redis` says of its replication, once its connection is READONLY; null where it can't be reached or answers
 *  otherwise. */
async function readOnlyReplication(redis: Redis): Promise<string | null> {
  try {
    await redis.connect()
    const replies = await redis.pipeline().info('replication').readonly().exec()
    assert(replies !== null)
    const [[infoError, info], [readOnlyError]] = twoOf(replies)
    return infoError === null && readOnlyError === null && typeof info === 'string' ? info : null
  } catch {
    return null
  }
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
    // Its unanswered pings find a silent connection (`RedisSubscriptions.ping()`), which a command deadline would end.
    commandTimeout: undefined,
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

/** What a client's connection attempt ended with, until its next `ready` (`recordAttempts()`). */
type Attempt = {
  lastError: unknown
  /** By node: what each answered since the Cluster last read its slots. */
  nodeErrors: Map<string, unknown>
}

/** This file's state on `globalThis`. */
function createGlobalObject() {
  return {
    /** The node each Cluster's subscriber was last duplicated from. */
    subscriberNodes: new WeakMap<Cluster, Redis>(),
    /** Each client's wait for its next `ready` (`nextReady()`). */
    readyWaits: new WeakMap<RedisClient, Promise<void>>(),
    /** What each backend client's connection attempt ended with (`recordAttempts()`). */
    attempts: new WeakMap<RedisClient, Attempt>(),
  }
}
declare module './getGlobalObject.js' {
  interface GlobalObjects {
    'ioredis.ts': ReturnType<typeof createGlobalObject>
  }
}
const { subscriberNodes, readyWaits, attempts } = getGlobalObject('ioredis.ts', createGlobalObject)

/** A Cluster's connect that closed after its nodes failed, with each node's last error (`endedWith()`): what the nodes
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
 *  ioredis would fail a command it held, or at once if the client ended: with what its attempt ended with
 *  (`endedWith()`). Every failed connect of a Cluster closes, whether it retries or ends. A client made with
 *  `lazyConnect` connects here. */
function nextReady(client: RedisClient): Promise<void> {
  const waiting = readyWaits.get(client)
  if (waiting !== undefined) return waiting
  if (client.status === 'end') return Promise.reject(endedWith(client))
  const promise = new Promise<void>((resolve, reject) => {
    const settle = () => {
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
      reject(endedWith(client))
    }
    client.once('ready', onReady)
    client.once('close', onClose)
    client.once('end', onClose)
  })
  readyWaits.set(client, promise)
  if (client.status === 'wait') void client.connect().catch(() => {})
  return promise
}

/** What the client's last connection attempt ended with (`recordAttempts()`): for a Cluster whose nodes failed, each
 *  node's error (`ClusterNodesFailed`), as ioredis's own error keeps only the last node's; else its last error. */
function endedWith(client: RedisClient): unknown {
  const attempt = attempts.get(client)
  if (attempt === undefined) return new Error('Connection is closed.')
  return attempt.nodeErrors.size === 0 ? attempt.lastError : new ClusterNodesFailed([...attempt.nodeErrors.values()])
}
