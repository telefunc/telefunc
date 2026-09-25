// ioredis policy: the client options Room requires, subscriber creation, and calls to defined commands.
export { assertAtMostOnceClient, createSubscriberSocket, defineCommand, callDefinedCommand, isCluster }
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

/** Rejects clients that could resend a command (at-most-once) or read a replica (Room reads are strongly consistent). */
function assertAtMostOnceClient(redis: RedisClient): void {
  if (isCluster(redis) && redis.options.scaleReads !== 'master') {
    throw new Error("RedisBackend: ioredis Cluster scaleReads must be 'master' for consistent Room reads")
  }
  const retries = isCluster(redis)
    ? redis.options.retryDelayOnFailover !== 0 ||
      redis.options.redisOptions?.maxRetriesPerRequest !== 0 ||
      redis.options.redisOptions?.reconnectOnError != null
    : redis.options.maxRetriesPerRequest !== 0 || redis.options.reconnectOnError != null
  if (retries)
    throw new Error(
      'RedisBackend: at-most-once requires maxRetriesPerRequest: 0 (standalone Redis), or retryDelayOnFailover: 0 and redisOptions.maxRetriesPerRequest: 0 (Cluster); reconnectOnError must be unset',
    )
  // A Cluster copies redisOptions.keyPrefix to its own options, so this reads either form.
  if (redis.options.keyPrefix)
    throw new Error(
      "RedisBackend: ioredis keyPrefix isn't supported (it doesn't apply to Pub/Sub channels). Use installRedis(redis, { prefix }) instead",
    )
}

/** A fresh, unconnected subscriber that never retries on its own; on a Cluster it sits on a live master. */
async function createSubscriberSocket(redis: RedisClient): Promise<SubscriberSocket> {
  let source: Redis
  if (isCluster(redis)) {
    // A lazyConnect Cluster has no nodes until it connects, and only a command would connect it.
    if (redis.status === 'wait') await redis.connect()
    const master = redis.nodes('master').find((candidate) => candidate.status !== 'end')
    if (master === undefined) throw new Error('RedisBackend: Cluster has no available masters')
    source = master
  } else source = redis
  return source.duplicate({
    connectionName: `telefunc-subscriber-${randomUUID()}`,
    autoResubscribe: false,
    lazyConnect: true,
    maxRetriesPerRequest: 1,
    retryStrategy: () => null,
  })
}

function defineCommand(redis: RedisClient, name: string, lua: string, numberOfKeys: number | null): void {
  if (numberOfKeys === null) redis.defineCommand(name, { lua })
  else redis.defineCommand(name, { numberOfKeys, lua })
}

/** Invoke a command registered via `defineCommand`: ioredis attaches it as a dynamic method TypeScript can't see. */
function callDefinedCommand(
  redis: RedisClient,
  command: string,
  keysAndArgs: ReadonlyArray<string | Uint8Array>,
): Promise<unknown> {
  const fn = (redis as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[command]
  assert(typeof fn === 'function', `Redis command "${command}" was not registered via defineCommand`)
  return fn.apply(redis, keysAndArgs as unknown[])
}
