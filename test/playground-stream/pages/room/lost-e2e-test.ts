export { testRoomLostFrames }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

type LostResult = { events: string[]; meta: Record<string, unknown>; count: number }

/** The store drops one of a room's frames: the page's view heals from the next frame, well before the 30 s heartbeat. */
function testRoomLostFrames() {
  testLost(
    'leave',
    'room: a member whose leave the store lost leaves the page’s view soon after the next join',
    (result) => {
      expect(result.events.sort()).deep.equal(['join:Next', 'leave:Stays'])
      expect(result.count).toBe(1)
    },
  )
  testLost('meta', 'room: a room meta write the store lost reaches the page with the next one', ({ meta }) => {
    expect(meta).deep.equal({ lost: 1, next: 2 })
  })
  // The 30 s heartbeat settles a close whose frame was lost.
  testLost(
    'close',
    'room: a close whose frame the store lost closes the page’s view by the next heartbeat',
    ({ events }) => {
      expect(events).contains('close')
    },
    45_000,
  )
}

function testLost(id: string, name: string, validate: (result: LostResult) => void, timeout = 10_000) {
  test(name, async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click(`#test-room-lost-${id}`)
    await autoRetry(async () => validate(await getResult<LostResult>('#room-lost-result')), { timeout })
  })
}
