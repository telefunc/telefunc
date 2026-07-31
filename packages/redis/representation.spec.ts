import { Redis } from 'ioredis'
import { expect, test } from 'vitest'
import { RedisRoomBackend } from './src/index.js'

test('keeps the ratified Redis backend representation', () => {
  const redis = new Redis({ lazyConnect: true, maxRetriesPerRequest: 0 })
  const backend = new RedisRoomBackend({ redis })
  redis.disconnect()
  expect(Object.keys(backend).sort()).toEqual(
    '_disposed,_prefix,_publisher,_receivers,spiVersion,subscriptions'.split(','),
  )
})
