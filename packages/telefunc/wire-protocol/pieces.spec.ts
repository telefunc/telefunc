import { afterEach, expect, test, vi } from 'vitest'
import { PieceSender } from './pieces.js'

afterEach(() => void vi.restoreAllMocks())

test('an acknowledgement within one tick of a coarsened clock leaves the link unmeasured', () => {
  vi.spyOn(performance, 'now').mockReturnValue(1_000)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(new Uint8Array(20_000), 5_000)
  expect(sender.acknowledged(1, 0)).toBe(true)
  transmitted.length = 0
  sender.send(new Uint8Array(600_000), 5_000)
  expect(transmitted.length).toBeGreaterThan(1)
})
