import { describe, expect, it } from 'vitest'
import {
  accepted,
  bytes,
  cellsOf,
  enterClosing,
  finalizeClose,
  okHead,
  openRoom,
  SEMANTIC,
} from '../../../telefunc/wire-protocol/backend/conformance/scenario.js'
import { createRedisFixture } from './fixture.js'
import { REDIS_ROOM_COMMANDS } from './layout.js'

const REDIS_URL = process.env.TELEFUNC_TEST_REAL_REDIS

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

describe.skipIf(REDIS_URL === undefined || REDIS_URL === '')('Redis production command operand inventory', () => {
  it('executes all nine shipped commands and keeps each actual KEYS group in one slot', async () => {
    const fx = await createRedisFixture(REDIS_URL as string)
    try {
      const roomId = 'runtime-slot} proof'
      const { inc, head } = await openRoom(fx.backend, roomId)
      const cells = cellsOf(await fx.backend.readCells(roomId, inc, { keys: [] }))
      expect(
        await fx.backend.compareExchangeCells(roomId, inc, cells.revision, [
          { key: 'cell} escape', set: { bytes: bytes('value') } },
        ]),
      ).toBe('committed')

      const subscription = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
      await subscription.ready
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('payload'), { retain: true })).delivery
      await fx.backend.deleteRetained(roomId, inc, SEMANTIC)
      await fx.backend.directoryPut(roomId, inc)
      await fx.backend.directoryDelete(roomId, inc)
      await subscription.unsubscribe()

      const closing = await enterClosing(fx.backend, roomId, head)
      okHead(await finalizeClose(fx.backend, roomId, closing.head, closing.leaseId))
      await fx.backend.dropGeneration(roomId, inc)

      const calls = fx.commandCalls()
      for (const descriptor of Object.values(REDIS_ROOM_COMMANDS)) {
        const matching = calls.filter((call) => call.name === descriptor.name)
        expect(matching.length, `shipped command ${descriptor.name} was not exercised`).toBeGreaterThan(0)
        for (const call of matching) {
          const offset = descriptor.numberOfKeys === null ? 1 : 0
          const count = descriptor.numberOfKeys ?? Number(call.args[0])
          const keys = call.args.slice(offset, offset + count).map(String)
          expect(keys).toHaveLength(count)
          expect(new Set(keys.map(redisSlot)), `${descriptor.name}: ${keys.join(', ')}`).toEqual(
            new Set([redisSlot(keys[0] as string)]),
          )
        }
      }
    } finally {
      await fx.dispose()
    }
  })
})
