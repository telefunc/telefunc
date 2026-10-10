export { CX_CONFLICT, retryCompareExchange }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { RoomError } from '../errors.js'
assertIsNotBrowser()

// Room owns conflict recovery: 16 attempts with 1→64 ms jitter, then a contention RoomError.
const ROOM_CX_ATTEMPTS = 16
const CX_CONFLICT: unique symbol = Symbol('telefunc.RoomCxConflict')

async function retryCompareExchange<T>(roomId: string, attempt: () => Promise<T | typeof CX_CONFLICT>): Promise<T> {
  for (let n = 0; n < ROOM_CX_ATTEMPTS; n++) {
    const result = await attempt()
    if (result !== CX_CONFLICT) return result
    const ceiling = Math.min(64, 2 ** n)
    await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * ceiling) + 1))
  }
  throw new RoomError(`Room update contention: ${roomId}`)
}
