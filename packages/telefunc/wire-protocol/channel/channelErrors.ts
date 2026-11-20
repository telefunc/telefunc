export { ChannelClosedError, ChannelOverflowError, replayLossError, unconfirmedError }

import { NetworkError } from '../../shared/NetworkError.js'

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

/** How a channel ends, on both ends, when its page hasn't confirmed what the server sent on it for over
 *  `reconnectTimeoutMs` of reconnects. */
function unconfirmedError(reconnectTimeoutMs: number): NetworkError {
  return new NetworkError(
    `Channel closed: the page hasn't confirmed what the server sent on it for over ${reconnectTimeoutMs / 1_000} s of reconnects`,
    true,
  )
}

/** How a channel ends when a reconnect needs what the page's replay buffer dropped. */
function replayLossError(): NetworkError {
  return new NetworkError(
    "Channel closed: a reconnect needed messages the client's replay buffer had dropped to stay within its size. Await your sends.",
    true,
  )
}
