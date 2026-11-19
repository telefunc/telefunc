export { invokeChannelListener }

import { invokeChannelListener as invokeListener } from '../../../wire-protocol/channel/channel.js'
import { withoutContext } from './context.js'

/** Room's: its callbacks deliver other members' messages, so on the server they run with no context, as a
 *  `BroadcastChannel`'s listeners do. */
function invokeChannelListener<Args extends unknown[]>(
  listener: (...args: Args) => unknown,
  args: Args,
  handleError: (error: unknown) => boolean | void,
): boolean | void {
  return withoutContext(() => invokeListener(listener, args, handleError))
}
