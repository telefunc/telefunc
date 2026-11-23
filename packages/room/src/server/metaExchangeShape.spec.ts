// The cells exchange a room meta write hands its store, as the in-memory driver receives it: what the Cloudflare driver
// forwards to the room's authority, an RPC argument the platform bounds. It counts `meta:seq`, creating it only on the
// room's first meta write, and carries nothing of the update's size: a store that limits a control record's size reads
// that from the control record beside the exchange.
import { afterEach, expect, it, vi } from 'vitest'
import { MemoryRoomDriver } from '../backend/MemoryRoomDriver.js'
import type { CellsExchange } from '../backend/contract.js'
import { Room } from '../index.js'

afterEach(() => vi.restoreAllMocks())

it("a room meta write's cells exchange counts meta:seq, creating it only on the first write, and holds nothing else", async () => {
  const id = `meta-shape-${crypto.randomUUID()}`
  await Room.create(id)
  const exchanges: CellsExchange[] = []
  const { compareExchangeCells } = MemoryRoomDriver.prototype
  vi.spyOn(MemoryRoomDriver.prototype, 'compareExchangeCells').mockImplementation(function (
    this: MemoryRoomDriver,
    roomId: string,
    inc: string,
    exchange: CellsExchange,
  ) {
    // A replace also drops the generation it replaced, by exchanges that count nothing.
    if (roomId === id && exchange.counts !== undefined) exchanges.push(exchange)
    return compareExchangeCells.call(this, roomId, inc, exchange)
  })

  await Room.setMeta(id, { a: '1' })
  await Room.setMeta(id, { b: '2' })
  await Room.updateMeta(id, { c: '3' })
  await Room.close(id)

  expect(exchanges.map((exchange) => ({ fields: Object.keys(exchange), counts: exchange.counts }))).toEqual([
    { fields: ['expect', 'mutations', 'counts'], counts: [{ key: 'meta:seq', delta: 1, create: true }] },
    { fields: ['expect', 'mutations', 'counts'], counts: [{ key: 'meta:seq', delta: 1 }] },
    { fields: ['expect', 'mutations', 'counts'], counts: [{ key: 'meta:seq', delta: 1 }] },
  ])
})
