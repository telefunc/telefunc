import { Cluster, Redis } from 'ioredis'
import { expect, onTestFinished, test, vi } from 'vitest'
import { RedisBackend } from './backend.js'
import { callDefinedCommand, createSubscriberSocket } from './ioredis.js'
import { REDIS_COMMANDS } from './commands.js'

test('requires never-resend clients', () => {
  const nodes = [{ host: '127.0.0.1', port: 6379 }]
  const message =
    'RedisBackend: at-most-once requires maxRetriesPerRequest: 0 (standalone Redis), or retryDelayOnFailover: 0 and redisOptions.maxRetriesPerRequest: 0 (Cluster); reconnectOnError must be unset'
  const defaults = [new Redis('redis://127.0.0.1:6379'), new Cluster(nodes)]
  const safe = [
    new Redis('redis://127.0.0.1:6379', { maxRetriesPerRequest: 0 }),
    new Cluster(nodes, { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0 } }),
  ]
  onTestFinished(() => [...defaults, ...safe].forEach((redis) => redis.disconnect()))
  for (const redis of defaults) expect(() => new RedisBackend({ redis })).toThrow(message)
  for (const redis of safe) expect(() => new RedisBackend({ redis })).not.toThrow()
})

test("rejects an ioredis keyPrefix, which Pub/Sub channel names don't get", () => {
  const nodes = [{ host: '127.0.0.1', port: 6379 }]
  const prefixed = [
    new Redis('redis://127.0.0.1:6379', { maxRetriesPerRequest: 0, keyPrefix: 'app:' }),
    new Cluster(nodes, { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0, keyPrefix: 'app:' } }),
    new Cluster(nodes, { retryDelayOnFailover: 0, keyPrefix: 'app:', redisOptions: { maxRetriesPerRequest: 0 } }),
  ]
  onTestFinished(() => prefixed.forEach((redis) => redis.disconnect()))
  for (const redis of prefixed) expect(() => new RedisBackend({ redis })).toThrow('keyPrefix')
})

test('rejects a Cluster that autopipelines, which batches a variable-key script by its key count', () => {
  const nodes = [{ host: '127.0.0.1', port: 6379 }]
  const options = { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0 } }
  const cluster = new Cluster(nodes, { ...options, enableAutoPipelining: true })
  const redis = new Redis('redis://127.0.0.1:6379', { maxRetriesPerRequest: 0, enableAutoPipelining: true })
  onTestFinished(() => [cluster, redis].forEach((client) => client.disconnect()))
  expect(() => new RedisBackend({ redis: cluster })).toThrow('enableAutoPipelining')
  expect(() => new RedisBackend({ redis })).not.toThrow()
})

test('duplicates the subscriber from a standalone client or a live Cluster node, connecting a lazyConnect Cluster first', async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
    retryDelayOnFailover: 0,
    redisOptions: { maxRetriesPerRequest: 0 },
  })
  const ended = new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 })
  const live = new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 })
  onTestFinished(() => [cluster, ended, live].forEach((redis) => redis.disconnect()))
  ended.disconnect()
  const connect = vi.spyOn(cluster, 'connect').mockImplementation(async () => {
    cluster.status = 'ready'
  })
  const nodes = vi.spyOn(cluster, 'nodes').mockReturnValue([ended])
  await expect(createSubscriberSocket(cluster)).rejects.toThrow('RedisBackend: Cluster has no available nodes')
  nodes.mockReturnValue([ended, live])
  const sockets = [await createSubscriberSocket(cluster), await createSubscriberSocket(live)]
  sockets.forEach((socket) => socket.disconnect())
  expect(connect).toHaveBeenCalledOnce()
})

test("waits for a connecting Cluster's masters instead of reporting none", async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
    retryDelayOnFailover: 0,
    redisOptions: { maxRetriesPerRequest: 0 },
  })
  const master = new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 })
  onTestFinished(() => [cluster, master].forEach((redis) => redis.disconnect()))
  // A non-lazy Cluster is 'connecting' from its constructor until its node pool fills.
  cluster.status = 'connecting'
  const nodes = vi.spyOn(cluster, 'nodes').mockReturnValue([])
  const opening = createSubscriberSocket(cluster)
  // A subscriber reopened while the Cluster still connects shares the one wait: the app's Cluster gets no more listeners.
  const reopening = createSubscriberSocket(cluster)
  expect([cluster.listenerCount('ready'), cluster.listenerCount('close')]).toEqual([1, 1])
  nodes.mockReturnValue([master])
  cluster.status = 'ready'
  cluster.emit('ready')
  for (const socket of [await opening, await reopening]) socket.disconnect()
  // A failed connect is an outage to report, whether the Cluster retries it (it never emits 'end' then) or gives up.
  cluster.status = 'connecting'
  const failing = createSubscriberSocket(cluster)
  cluster.emit('close')
  await expect(failing).rejects.toThrow('RedisBackend: Cluster connection closed')
})

const publishInput = { route: { key: 'chat', kind: 'text' }, payload: new Uint8Array() } as const
test("reads integer replies as ioredis returns them, numbers or, with a shared client's stringNumbers, strings", () => {
  for (const reply of [
    [7, 1_700_000_000_000, 2],
    ['7', '1700000000000', '2'],
  ])
    expect(REDIS_COMMANDS.publish.parse(reply, publishInput)).toEqual({
      seq: 7,
      timestamp: 1_700_000_000_000,
      receivers: 2,
    })
  for (const reply of [1, '1'])
    expect(REDIS_COMMANDS.validateGeneration.parse(reply, { roomId: 'room', inc: 'inc' })).toBe(true)
})

test('names Pub/Sub channels per database, as Pub/Sub spans every database', async () => {
  const clients = [0, 1].map((db) => new Redis({ lazyConnect: true, maxRetriesPerRequest: 0, db }))
  onTestFinished(() => clients.forEach((redis) => redis.disconnect()))
  const channels = await Promise.all(
    clients.map(async (redis) => {
      const backend = new RedisBackend({ redis })
      const publish = vi
        .spyOn(redis as unknown as Record<string, () => Promise<unknown>>, REDIS_COMMANDS.publish.name)
        .mockResolvedValue([1, 1, 0])
      await backend.publish({ key: 'chat', kind: 'text' }, new Uint8Array())
      return (publish.mock.calls[0] as unknown as [unknown[]])[0][1]
    }),
  )
  expect(channels[0]).not.toBe(channels[1])
})

test('moves the subscriber to the next master on each reconnect, so a failed master it never used does not hold it', async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
    retryDelayOnFailover: 0,
    redisOptions: { maxRetriesPerRequest: 0 },
  })
  const masters = [0, 1].map(() => new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 }))
  onTestFinished(() => [cluster, ...masters].forEach((redis) => redis.disconnect()))
  vi.spyOn(cluster, 'connect').mockImplementation(async () => {
    cluster.status = 'ready'
  })
  vi.spyOn(cluster, 'nodes').mockReturnValue(masters)
  const duplicates = masters.map((master) => vi.spyOn(master, 'duplicate'))
  const sockets = [await createSubscriberSocket(cluster), await createSubscriberSocket(cluster)]
  sockets.forEach((socket) => socket.disconnect())
  expect(duplicates.map((duplicate) => duplicate.mock.calls.length)).toEqual([1, 1])
})

test("takes the subscriber from a replica when the Cluster's pool labels no live master, as after a one-shard failover", async () => {
  const cluster = new Cluster([{ host: '127.0.0.1', port: 6379 }], {
    lazyConnect: true,
    retryDelayOnFailover: 0,
    redisOptions: { maxRetriesPerRequest: 0 },
  })
  const promoted = new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 })
  onTestFinished(() => [cluster, promoted].forEach((redis) => redis.disconnect()))
  vi.spyOn(cluster, 'connect').mockImplementation(async () => {
    cluster.status = 'ready'
  })
  // The pool never refreshed: the promoted replica is still labelled a replica.
  vi.spyOn(cluster, 'nodes').mockImplementation((role) => (role === 'master' ? [] : [promoted]))
  const duplicate = vi.spyOn(promoted, 'duplicate')
  const socket = await createSubscriberSocket(cluster)
  socket.disconnect()
  expect(duplicate).toHaveBeenCalledOnce()
})

test("passes a script more keys than a function call takes arguments, as a long-lived room's close does", async () => {
  const received: unknown[][] = []
  const redis = { dropGeneration: async (...args: unknown[]) => void received.push(args) }
  const keys = Array.from({ length: 500_000 }, (_, index) => `lane:${index}`)
  await callDefinedCommand(redis as never, 'dropGeneration', keys)
  expect((received[0] as [string[]])[0]).toHaveLength(500_000)
})
