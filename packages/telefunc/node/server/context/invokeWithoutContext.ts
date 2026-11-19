// The server-side callbacks that deliver other users' messages, a `BroadcastChannel`'s and Room's: each runs with no
// context.
export { invokeChannelListener, invokeChannelListeners, soleAnswer }

import {
  invokeChannelListener as invokeListener,
  invokeChannelListeners as invokeAll,
} from '../../../wire-protocol/channel/channel.js'
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

/** The walk of a list of them: one context withheld for all of its calls. */
function invokeChannelListeners<L, A>(
  listeners: readonly Registration<L>[],
  call: (listener: L, arg: A) => unknown,
  arg: A,
  handleError: (error: unknown) => boolean | void,
): boolean {
  return listeners.length > 0 && withoutContext(invokeAll<L, A>, listeners, call, arg, handleError)
}

/** A private message that asks for a reply: its listeners answer another member. */
function soleAnswer<T, A extends unknown[]>(
  listeners: readonly Registration<(data: T, ...args: A) => unknown>[],
  data: T,
  ...args: A
): Promise<unknown> {
  return withoutContext(() => answerOf(listeners, data, ...args))
}
