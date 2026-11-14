export { StaleReads }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { assertWarning } from '../../utils/assert.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../constants.js'
import type { LanePlan } from '../lanes/LanePlan.js'
assertIsNotBrowser()

type StaleReadsOptions = {
  roomId: string
  model: Pick<RoomModel, 'closed'>
  lanePlan: Pick<LanePlan, 'observing'>
}

/** A view nothing follows answers as of when it was last current: its `Room.get()`, or the end of the follow that kept
 *  it current. A read more than a heartbeat after that warns, once per view: other servers' changes since never reach it. */
class StaleReads {
  private currentAt = Date.now()
  /** Whether the lanes followed the room at their last change, so a follow's end dates the view. */
  private followed = false
  private warned = false

  private readonly roomId: string
  private readonly model: StaleReadsOptions['model']
  private readonly lanePlan: StaleReadsOptions['lanePlan']

  constructor({ roomId, model, lanePlan }: StaleReadsOptions) {
    this.roomId = roomId
    this.model = model
    this.lanePlan = lanePlan
  }

  /** A `Room.get()` caught the view up, or found it current. */
  caughtUp(): void {
    this.currentAt = Date.now()
  }

  lanesChanged(): void {
    const followed = this.lanePlan.observing
    if (this.followed && !followed) this.currentAt = Date.now()
    this.followed = followed
  }

  /** A handle's read: on a followed view, two flags and no clock. */
  read(): void {
    if (this.warned || this.lanePlan.observing || this.model.closed) return
    const age = Date.now() - this.currentAt
    if (age <= ROOM_HEARTBEAT_INTERVAL_MS) return
    this.warned = true
    assertWarning(
      false,
      `Room '${this.roomId}' was read ${Math.round(age / 1000)} s after its last Room.get() on this server, and nothing ` +
        'here listens to it or has joined it, so the read misses what other servers changed since. Call Room.get() ' +
        'where you read the room, or listen to it to keep it current.',
    )
  }
}
