export { reportRoomError }
/** Application code (listeners, handlers, after-hooks): whatever escapes it is a bug. */
export { reportServerChannelError as reportCallbackError } from '../../server/channel.js'

import { reportServerChannelError } from '../../server/channel.js'
import { isRoomError } from '../errors.js'

/** Room's own background work: a RoomError there is an expected outcome (closed room, departed member). */
function reportRoomError(err: unknown): void {
  if (!isRoomError(err)) reportServerChannelError(err)
}
