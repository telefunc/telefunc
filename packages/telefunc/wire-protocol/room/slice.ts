export { Slice }

import { Fifo } from '../../utils/Fifo.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { nextTurn } from '../flow-control/macrotask-yield.js'
import { ROOM_PLAN_SLICE_MS } from './constants.js'

/** The slices waiting in the process, and how many turns resumed one. */
const waiting = getGlobalObject('wire-protocol/room/slice.ts', () => ({ resumed: 0, slices: new Fifo<() => void>() }))

/** Work's time on the event loop: `ROOM_PLAN_SLICE_MS` from when it starts, and as much in each later turn it waits
 *  for. A turn resumes one waiting slice, the first to wait, so slices waiting together don't run in one turn. */
class Slice {
  private _end = performance.now() + ROOM_PLAN_SLICE_MS

  get end(): number {
    return this._end
  }

  get over(): boolean {
    return performance.now() >= this._end
  }

  async next(): Promise<void> {
    await new Promise<void>((resolve) => {
      waiting.slices.push(resolve)
      tick()
    })
    this._end = performance.now() + ROOM_PLAN_SLICE_MS
  }
}

/** Resumes the first waiting slice in a later turn, unless another tick did since; each wait schedules its own, so a
 *  tick a runtime drops stalls nothing past the next wait. */
function tick(): void {
  const resumed = waiting.resumed
  nextTurn(() => {
    if (waiting.resumed !== resumed) return
    waiting.resumed++
    const resume = waiting.slices.shift()
    if (waiting.slices.length > 0) tick()
    resume?.()
  })
}
