export { ChannelClosedError, ChannelOverflowError, isChannelOverflowError, isExpectedChannelFailure, replayLossError }

import { NetworkError, isNetworkError } from '../../shared/NetworkError.js'

/** Thrown synchronously by `send()` when the channel is already closed.
 *  Also used to reject pending ack promises when the channel shuts down. */
class ChannelClosedError extends Error {
  constructor(message = 'Channel is closed') {
    super(message)
    this.name = 'ChannelClosedError'
  }
}

const channelOverflowErrorBrand = Symbol.for('telefunc.ChannelOverflowError')

/** Used when a buffered channel send is dropped in order to keep memory usage hard-capped. */
class ChannelOverflowError extends Error {
  readonly [channelOverflowErrorBrand] = true as const

  constructor(message = 'Channel send buffer overflow') {
    super(message)
    this.name = 'ChannelOverflowError'
  }
}

function isChannelOverflowError(thing: unknown): thing is ChannelOverflowError {
  return (
    thing instanceof ChannelOverflowError ||
    (typeof thing === 'object' && thing !== null && channelOverflowErrorBrand in thing)
  )
}

function isExpectedChannelFailure(err: unknown): err is ChannelClosedError | ChannelOverflowError | NetworkError {
  return err instanceof ChannelClosedError || isChannelOverflowError(err) || isNetworkError(err)
}

/** How a channel ends when a reconnect needs what `side`'s replay buffer dropped. */
function replayLossError(side: 'server' | 'client'): NetworkError {
  return new NetworkError(
    `Channel closed: a reconnect needed messages the ${side}'s replay buffer had dropped to stay within its size. Raise config.channel.${side}ReplayBuffer, or ${side}ReplayBufferBinary for binary messages and streams.`,
    true,
  )
}
