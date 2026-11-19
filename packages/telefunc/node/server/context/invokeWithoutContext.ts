// Room's server-side callbacks, which deliver other members' messages, as a `BroadcastChannel`'s listeners do: each runs
// with no context.
export { invokeChannelListener, soleAnswer }

import { invokeChannelListener as invokeListener } from '../../../wire-protocol/channel/channel.js'
import { soleAnswer as answerOf } from '../../../wire-protocol/channel/listenerAnswer.js'
import type { Registration } from '../../../utils/Listeners.js'
import { withoutContext } from './context.js'

function invokeChannelListener<Args extends unknown[]>(
  listener: (...args: Args) => unknown,
  args: Args,
  handleError: (error: unknown) => boolean | void,
): boolean | void {
  return withoutContext(() => invokeListener(listener, args, handleError))
}

/** A private message that asks for a reply: its listeners answer another member. */
function soleAnswer<T, A extends unknown[]>(
  listeners: readonly Registration<(data: T, ...args: A) => unknown>[],
  data: T,
  ...args: A
): Promise<unknown> {
  return withoutContext(() => answerOf(listeners, data, ...args))
}
