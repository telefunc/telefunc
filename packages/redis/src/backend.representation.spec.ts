import { Cluster, Redis } from 'ioredis'
import { expect, onTestFinished, test, vi } from 'vitest'
import { RedisBackend } from './backend.js'
import { createSubscriberSocket } from './ioredis.js'

test('requires explicit never-resend playground clients', () => {
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

test('duplicates the subscriber from a standalone client or a live Cluster master, connecting a lazyConnect Cluster first', async () => {
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
  await expect(createSubscriberSocket(cluster)).rejects.toThrow('RedisBackend: Cluster has no available masters')
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
