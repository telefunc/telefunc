// A head's revision on the memory store: the store keeps the revision the exchange's caller minted, and a room created
// again under an id after the tombstone before it lapsed has a revision no earlier head under the id had.
import { afterEach, expect, it, vi } from 'vitest'
import { Room } from '../index.js'
import { ROOM_TOMBSTONE_TTL_MS } from '../server/constants.js'
import { MemoryRoomDriver } from './MemoryRoomDriver.js'
import { getRoomPlane } from './plane.js'

afterEach(() => {
  vi.useRealTimers()
})

it("keeps the revision the exchange's caller minted", async () => {
  const driver = new MemoryRoomDriver()
  const roomId = 'head-revision'
  const config = new Uint8Array()
  const x = await driver.compareExchangeHead(
    roomId,
    { form: 'absent' },
    { head: { rev: 'rev-x', currentInc: 'inc-x', state: 'open', config } },
  )
  const closed = await driver.compareExchangeHead(
    roomId,
    { form: 'rev', rev: 'rev-x' },
    { head: { rev: 'rev-x-closed', currentInc: null, state: 'closed', config }, ttlMs: 1_000 },
  )
  const read = await driver.readHead(roomId)

  expect([x, closed].map((result) => (result.kind === 'committed' ? result.head.rev : result.kind))).toEqual([
    'rev-x',
    'rev-x-closed',
  ])
  expect(read?.rev).toBe('rev-x-closed')
})

it('writes the head of a room created again, after its tombstone lapsed, at a revision no earlier head had', async () => {
  const id = `head-revision-${crypto.randomUUID()}`
  const { reads } = getRoomPlane().direct
  const revs: string[] = []
  const readRev = async () => {
    const head = await reads.readHead(id)
    if (head !== null) revs.push(head.rev)
    return head
  }
  await Room.create(id)
  await readRev()
  await Room.close(id)
  await readRev()
  vi.useFakeTimers({ toFake: ['Date'], now: Date.now() })
  vi.setSystemTime(Date.now() + ROOM_TOMBSTONE_TTL_MS + 1_000)
  const lapsed = await reads.readHead(id)
  await Room.create(id)
  const y = await reads.readHead(id)

  expect({ lapsed, earlier: revs.length, repeated: y !== null && revs.includes(y.rev) }).toEqual({
    lapsed: null,
    earlier: 2,
    repeated: false,
  })
  await Room.close(id)
})
