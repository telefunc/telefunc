export {
  ChannelClosedError,
  ChannelOverflowError,
  ChannelCloseUnknownError,
  ChannelPageLeftError,
  framesRefusedError,
  isChannelOverflowError,
  isExpectedChannelFailure,
}

import { NetworkError } from '../../shared/NetworkError.js'
import { markErrorClass } from '../../shared/markErrorClass.js'

/** The channel is closed: `send()` on it throws this synchronously, and acknowledged sends still pending reject with it
 *  as it shuts down. A channel the server ends closes with it too: a refused frame, a broadcast subscription that didn't
 *  take effect, a close that timed out. */
class ChannelClosedError extends Error {
  constructor(message = 'Channel is closed', options?: { cause?: unknown }) {
    super(message, options)
    this.name = 'ChannelClosedError'
  }
}

/** Telefunc can't hold more for the channel: a refused `send()`, `{ ack: true }` reply or `publish()` rejects with it and
 *  the channel stays open; an overloaded inbound channel or a broadcast subscriber too far behind closes with it. */
class ChannelOverflowError extends Error {
  constructor(message = 'Channel send buffer overflow') {
    super(message)
    this.name = 'ChannelOverflowError'
  }
}

function isChannelOverflowError(thing: unknown): thing is ChannelOverflowError {
  return thing instanceof ChannelOverflowError
}

function isExpectedChannelFailure(err: unknown): err is ChannelClosedError | ChannelOverflowError | NetworkError {
  return err instanceof ChannelClosedError || err instanceof ChannelOverflowError || err instanceof NetworkError
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

/** How a channel ends, on both ends, when the server can't take a frame the page sent: `reason` is the refusal's
 *  sentence (`END_CAUSE`'s frame causes). Sending the same frame again fails again, so it is no `NetworkError`. */
function framesRefusedError(reason: string): ChannelClosedError {
  return new ChannelClosedError(`Channel closed: the server refused what the page sent on it: ${reason}`)
}
