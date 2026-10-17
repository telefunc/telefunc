export { CX_CONFLICT, retryCompareExchange }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

// Room owns conflict recovery: retried after 1→64 ms of jitter until it commits. A conflict means another write
// committed, to a cell this one read or to the head.
const CX_CONFLICT: unique symbol = Symbol('telefunc.RoomCxConflict')

async function retryCompareExchange<T>(attempt: () => Promise<T | typeof CX_CONFLICT>): Promise<T> {
  for (let n = 0; ; n++) {
    const result = await attempt()
    if (result !== CX_CONFLICT) return result
    const ceiling = Math.min(64, 2 ** n)
    await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * ceiling) + 1))
  }
}
