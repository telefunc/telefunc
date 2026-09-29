export { ChannelClosedError, ChannelOverflowError, replayLossError }

import { NetworkError } from '../shared/NetworkError.js'
import { ERROR_REASON, type ReplayLoss } from './shared-ws.js'

/** Thrown synchronously by `send()` when the channel is already closed.
 *  Also used to reject pending ack promises when the channel shuts down. */
class ChannelClosedError extends Error {
  constructor(message = 'Channel is closed') {
    super(message)
    this.name = 'ChannelClosedError'
  }
}

/** Used when a buffered channel send is dropped in order to keep memory usage hard-capped. */
class ChannelOverflowError extends Error {
  constructor(message = 'Channel send buffer overflow') {
    super(message)
    this.name = 'ChannelOverflowError'
  }
}

/** How a channel ends when a reconnect needs what `side`'s replay buffer dropped. */
function replayLossError(side: 'server' | 'client', loss: ReplayLoss): NetworkError {
  const [dropped, setting] =
    loss === ERROR_REASON.EXPIRED
      ? ['for their age', 'reconnectTimeout']
      : ['to stay within its size', `${side}ReplayBuffer, or ${side}ReplayBufferBinary for binary messages and streams`]
  return new NetworkError(
    `Channel closed: a reconnect needed messages the ${side}'s replay buffer had dropped ${dropped}. Raise config.channel.${setting}.`,
    true,
  )
}
