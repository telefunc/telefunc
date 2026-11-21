// A store hands each frame of a lane to every receiver of the lane: one that throws is reported, and the receivers
// after it, and the frames after it, still get theirs.
import { afterEach, expect, it, vi } from 'vitest'
import type { RoomSubscriptionSource } from './contract.js'
import { MemoryRoomDriver } from './MemoryRoomDriver.js'

afterEach(() => {
  vi.restoreAllMocks()
})

it('hands each frame to the receiver after one that throws, and reports each throw', async () => {
  const reports: unknown[] = []
  vi.spyOn(console, 'error').mockImplementation((error) => void reports.push(error))
  const driver = new MemoryRoomDriver()
  const roomId = 'throwing-receiver'
  const inc = 'inc-1'
  await driver.compareExchangeHead(
    roomId,
    { form: 'absent' },
    { head: { currentInc: inc, state: 'open', config: new Uint8Array() } },
  )
  const source: RoomSubscriptionSource = { roomId, inc, lane: { kind: 'semantic' } }
  const thrower: number[] = []
  const healthy: number[] = []
  const attempts = [
    driver.subscribe(source, {
      deliver(_payload, info) {
        thrower.push(info.seq)
        throw new Error('Test: a receiver throws')
      },
    }),
    driver.subscribe(source, { deliver: (_payload, info) => void healthy.push(info.seq) }),
  ]
  await expect.poll(() => attempts.map((attempt) => attempt.state())).toEqual(['ready', 'ready'])

  await driver.commitLane({ roomId, inc, lane: { kind: 'semantic' } }, new Uint8Array([1]))
  await driver.commitLane({ roomId, inc, lane: { kind: 'semantic' } }, new Uint8Array([2]))

  await expect.poll(() => healthy).toEqual([1, 2])
  expect(thrower).toEqual([1, 2])
  expect(reports.map(String)).toEqual(['Error: Test: a receiver throws', 'Error: Test: a receiver throws'])
  await Promise.all(attempts.map((attempt) => attempt.unsubscribe()))
})
