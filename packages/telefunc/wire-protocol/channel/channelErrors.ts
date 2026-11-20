export {
  ChannelClosedError,
  ChannelOverflowError,
  ChannelCloseUnknownError,
  ChannelPageLeftError,
  framesRefusedError,
  replayLossError,
  unconfirmedError,
}

import { NetworkError } from '../../shared/NetworkError.js'

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

/** How a channel ends, on both ends, when the server can't take a frame the page sent: `reason` is the cause's sentence
 *  (`FRAME_REFUSAL`). Sending the same frame again fails again, so it is no `NetworkError`. */
function framesRefusedError(reason: string): ChannelClosedError {
  return new ChannelClosedError(`Channel closed: the server refused what the page sent on it: ${reason}`)
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
