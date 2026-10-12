export { Slice }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { nextTurnOf } from '../../flow-control/macrotask-yield.js'
import { ROOM_PLAN_SLICE_MS } from '../constants.js'
assertIsNotBrowser()

/** Work's time on the event loop: `ROOM_PLAN_SLICE_MS` from when it starts, and as much in each later turn it waits for. */
class Slice {
  private _end = performance.now() + ROOM_PLAN_SLICE_MS

  get end(): number {
    return this._end
  }

  get over(): boolean {
    return performance.now() >= this._end
  }

  async next(): Promise<void> {
    await nextTurnOf()
    this._end = performance.now() + ROOM_PLAN_SLICE_MS
  }
}
