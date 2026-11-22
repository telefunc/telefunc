// A cells exchange whose count completes a record, on the in-memory store, which has no size rule: it writes exactly
// what the same exchange without the record writes, and gives the same counters, for a record of any length.
import { afterEach, expect, it, vi } from 'vitest'
import type { CellCount, CellsExchange, RecordCount } from './contract.js'
import { MemoryRoomDriver } from './MemoryRoomDriver.js'

afterEach(() => {
  vi.useRealTimers()
})

const ROOM = 'record-count'
const INC = 'inc-1'
const NOW = Date.UTC(2026, 10, 22)
const SELECTED = { keys: ['meta:seq', 'k'] }

/** Two exchanges that count `meta:seq` and write `k`, the count completing a record of `bytes` bytes, or none. */
function exchanges(bytes: number | null): CellsExchange[] {
  return [1, 2].map((value) => {
    const count: CellCount | RecordCount =
      bytes === null
        ? { key: 'meta:seq', delta: 1, create: true }
        : { key: 'meta:seq', delta: 1, create: true, completes: { lane: { kind: 'control' }, bytes } }
    return { expect: [], mutations: [{ key: 'k', bytes: new Uint8Array([value]) }], counts: [count] }
  })
}

/** What the in-memory store answered the exchanges and then holds. */
async function memoryAfter(bytes: number | null) {
  vi.useFakeTimers({ toFake: ['Date'], now: NOW })
  const driver = new MemoryRoomDriver()
  const config = new Uint8Array()
  await driver.compareExchangeHead(
    ROOM,
    { form: 'absent' },
    { head: { rev: 'rev-1', currentInc: INC, state: 'open', config } },
  )
  const answers = []
  for (const exchange of exchanges(bytes)) answers.push(await driver.compareExchangeCells(ROOM, INC, exchange))
  return { answers, read: await driver.readCells(ROOM, INC, SELECTED) }
}

it('memory: an exchange whose count completes a record writes what it writes without the record', async () => {
  const without = await memoryAfter(null)

  for (const bytes of [40, 64 * 1024 * 1024]) expect(await memoryAfter(bytes)).toEqual(without)
  expect(without.answers.map((answer) => answer.kind)).toEqual(['committed', 'committed'])
})
