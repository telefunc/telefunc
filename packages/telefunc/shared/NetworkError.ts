export { NetworkError }

import { markErrorClass } from './markErrorClass.js'

/** Network failure: the connection to the server couldn't be established or was lost.
 *  Thrown for regular telefunction calls — as the `ConnectionError` subclass, with `isChannel: false`
 *  — and for channels, with `isChannel: true`: TTL expired, client failed to reconnect, WebSocket
 *  connection rejected, reconnect timeout exceeded, channel not re-acknowledged after reconnect, a
 *  reconnect needed messages a replay buffer had dropped, what it sent was refused by something in front of the server
 *  again after a reconnect, or the page hadn't confirmed what the server sent on it over the reconnects. Two channel
 *  kinds have their own class: a connection lost during a close (`ChannelCloseUnknownError`) and, on the server, a page
 *  that left (`ChannelPageLeftError`).
 *
 *  Catch it with a single `instanceof NetworkError` and read `isChannel` to tell the two apart. */
class NetworkError extends Error {
  /** `true` if the failure happened on a channel connection, `false` for a regular telefunction call. */
  readonly isChannel: boolean

  constructor(message: string, isChannel: boolean, options?: { cause?: unknown }) {
    super(message, options)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'NetworkError'
    this.isChannel = isChannel
    Error.captureStackTrace?.(this, new.target)
  }
}

markErrorClass(NetworkError, 'NetworkError')
