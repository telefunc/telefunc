export {
  ChannelClosedError,
  ChannelOverflowError,
  ChannelCloseUnknownError,
  ChannelPageLeftError,
  replayLossError,
  rejectedFrameError,
}

import { NetworkError } from '../../shared/NetworkError.js'
import { markErrorClass } from '../../shared/markErrorClass.js'

/** Thrown synchronously by `send()` when the channel is already closed.
 *  Also used to reject pending ack promises when the channel shuts down. */
class ChannelClosedError extends Error {
  constructor(message = 'Channel is closed', options?: { cause?: unknown }) {
    super(message, options)
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

/** A channel whose connection was lost during its close, on either end: the verdict that both ends' cleanups finished
 *  within the timeout couldn't cross, so whether they did can't be known. It is a channel `NetworkError`, so code that
 *  handles a lost connection handles it; where a lost connection is why, `cause` is its error. */
class ChannelCloseUnknownError extends NetworkError {
  constructor(options?: { cause?: unknown }) {
    super(
      "Channel closed, but how its close ended can't be known: the connection was lost before both ends' verdict crossed",
      true,
      options,
    )
    this.name = 'ChannelCloseUnknownError'
  }
}

/** How the server's channel ends when its page leaves for good (closed, reloaded or navigated away), during a close or
 *  outside one: the page's side can't finish, and what the server still held for it reaches no one. A channel
 *  `NetworkError`. */
class ChannelPageLeftError extends NetworkError {
  constructor() {
    super('Channel ended: its page left (the tab was closed, reloaded or navigated away)', true)
    this.name = 'ChannelPageLeftError'
  }
}

markErrorClass(ChannelClosedError, 'ChannelClosedError')
markErrorClass(ChannelOverflowError, 'ChannelOverflowError')
markErrorClass(ChannelCloseUnknownError, 'ChannelCloseUnknownError')
markErrorClass(ChannelPageLeftError, 'ChannelPageLeftError')

/** How a channel ends on both ends when the server refused its client's wire for a frame it couldn't read. */
function rejectedFrameError(): ChannelClosedError {
  return new ChannelClosedError(
    'Channel closed: the server refused a frame it could not read, as from a client of another version. Reload the page.',
  )
}

/** How a channel ends when a reconnect needs what the page's replay buffer dropped. */
function replayLossError(): NetworkError {
  return new NetworkError(
    "Channel closed: a reconnect needed messages the client's replay buffer had dropped to stay within its size. Await your sends.",
    true,
  )
}
