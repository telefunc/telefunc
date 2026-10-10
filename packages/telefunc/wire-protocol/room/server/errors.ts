export { reportRoomError }

import { ChannelOverflowError } from '../../channel-errors.js'
import { reportServerChannelError } from '../../server/channel.js'
import { isRoomError } from '../errors.js'

/** Room's own background work: a RoomError there is an expected outcome (closed room, departed member). A write a
 *  held-send bound refused is no bug either, as Broadcast's refused publish isn't, but nothing else hears of it. */
function reportRoomError(err: unknown): void {
  if (isRoomError(err)) return
  if (err instanceof ChannelOverflowError) console.error(err)
  else reportServerChannelError(err)
}
