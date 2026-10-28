export { reportRoomError }

import { isChannelOverflowError } from '../../channel/channelErrors.js'
import { reportBug } from '../../../node/server/__internal.js'
import { isRoomError } from '../errors.js'

/** Room's own background work: a RoomError there is an expected outcome (closed room, departed member). A write a
 *  held-send bound refused is no bug either, as Broadcast's refused publish isn't, but nothing else hears of it. */
function reportRoomError(err: unknown): void {
  if (isRoomError(err)) return
  if (isChannelOverflowError(err)) console.error(err)
  else reportBug(err)
}
