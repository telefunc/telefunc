import { Cluster, Redis } from 'ioredis'
import { expect, onTestFinished, test } from 'vitest'
import { RedisRoomBackend } from './src/index.js'

test('keeps the ratified Redis backend representation', () => {
  const backend = new RedisRoomBackend({ redis: new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 }) })
  expect(Object.keys(backend).sort()).toEqual(
    '_disposed,_prefix,_publisher,_receivers,spiVersion,subscriptions'.split(','),
  )
})

test('requires explicit never-resend playground clients', () => {
  const nodes = [{ host: '127.0.0.1', port: 6379 }]
  const message =
    'RedisRoomBackend: at-most-once requires maxRetriesPerRequest: 0 (standalone Redis), or retryDelayOnFailover: 0 and redisOptions.maxRetriesPerRequest: 0 (Cluster); reconnectOnError must be unset'
  const defaults = [new Redis('redis://127.0.0.1:6379'), new Cluster(nodes)]
  const safe = [
    new Redis('redis://127.0.0.1:6379', { maxRetriesPerRequest: 0 }),
    new Cluster(nodes, { retryDelayOnFailover: 0, redisOptions: { maxRetriesPerRequest: 0 } }),
  ]
  onTestFinished(() => [...defaults, ...safe].forEach((redis) => redis.disconnect()))
  for (const redis of defaults) expect(() => new RedisRoomBackend({ redis })).toThrow(message)
  for (const redis of safe) expect(() => new RedisRoomBackend({ redis })).not.toThrow()
})
