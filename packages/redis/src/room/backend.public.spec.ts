import { describe, expect, it } from 'vitest'
import type { Redis } from 'ioredis'
import { RedisRoomBackend } from './backend.js'

describe('RedisRoomBackend public construction boundary', () => {
  it.each([-1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY])(
    'rejects invalid retained capacity %p before acquiring a subscriber',
    (maxRetainedPayloadBytes) => {
      let duplicates = 0
      const publisher = {
        duplicate: () => {
          duplicates++
          throw new Error('duplicate must not run for invalid public options')
        },
      } as unknown as Redis

      expect(() => new RedisRoomBackend({ redis: publisher, maxRetainedPayloadBytes })).toThrow(
        'maxRetainedPayloadBytes must be a finite non-negative number',
      )
      expect(duplicates).toBe(0)
    },
  )
})
