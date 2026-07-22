import { describe, expect, it } from 'vitest'
import type { Redis } from 'ioredis'
import { RedisRoomBackend } from './backend.js'
import { REDIS_ROOM_COMMANDS } from './layout.js'

function redisSlot(key: string): number {
  const start = key.indexOf('{')
  const end = start < 0 ? -1 : key.indexOf('}', start + 1)
  const tagged = start >= 0 && end > start + 1 ? key.slice(start + 1, end) : key
  let crc = 0
  for (const byte of new TextEncoder().encode(tagged)) {
    crc ^= byte << 8
    for (let bit = 0; bit < 8; bit++) crc = (crc & 0x8000) === 0 ? crc << 1 : (crc << 1) ^ 0x1021
  }
  return crc & 0x3fff
}

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

  it('keeps the shipped directory-put and cell-CX call-site operands co-slotted', async () => {
    const calls = new Map<string, unknown[]>()
    let subscriberQuits = 0
    let publisherQuits = 0
    const subscriber = {
      status: 'ready',
      on: () => subscriber,
      off: () => subscriber,
      quit: async () => {
        subscriberQuits++
        return 'OK'
      },
      disconnect: () => {},
    }
    const publisher: Record<string, unknown> = {
      duplicate: () => subscriber,
      quit: async () => {
        publisherQuits++
        return 'OK'
      },
      defineCommand: (name: string) => {
        publisher[name] = async (...args: unknown[]) => {
          calls.set(name, args)
          return name === REDIS_ROOM_COMMANDS.cellsCx.name ? 'committed' : 1
        }
      },
    }
    const backend = new RedisRoomBackend({ redis: publisher as unknown as Redis })
    try {
      await backend.directoryPut('room} escape', 'inc')
      await backend.compareExchangeCells('room} escape', 'inc', '0', [
        { key: 'cell} escape', set: { bytes: new Uint8Array([1]) } },
      ])

      const directory = calls.get(REDIS_ROOM_COMMANDS.directoryPut.name) ?? []
      const cells = calls.get(REDIS_ROOM_COMMANDS.cellsCx.name) ?? []
      const cellCount = Number(cells[0])
      for (const keys of [directory.slice(0, 2), cells.slice(1, cellCount + 1)]) {
        expect(keys.length).toBeGreaterThan(0)
        expect(new Set(keys.map((key) => redisSlot(String(key))))).toEqual(new Set([redisSlot(String(keys[0]))]))
      }
    } finally {
      await backend.dispose()
      await backend.dispose()
    }
    expect(subscriberQuits).toBe(1)
    expect(publisherQuits).toBe(0)
  })
})
