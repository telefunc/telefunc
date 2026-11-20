export { NetworkError }

const networkErrorBrand = Symbol.for('telefunc.NetworkError')

/** Network failure: the connection to the server couldn't be established or was lost.
 *  Thrown for regular telefunction calls — as the `ConnectionError` subclass, with `isChannel: false`
 *  — and for channels, with `isChannel: true`: TTL expired, client failed to reconnect, WebSocket
 *  connection rejected, reconnect timeout exceeded, channel not re-acknowledged after reconnect, a
 *  reconnect needed messages a replay buffer had dropped, or the page hadn't confirmed what the server sent on it over
 *  the reconnects. Two channel kinds have their own class: a connection lost during a close
 *  (`ChannelCloseUnknownError`) and, on the server, a page that left (`ChannelPageLeftError`).
 *
 *  Catch it with a single `instanceof NetworkError` and read `isChannel` to tell the two apart. */
class NetworkError extends Error {
  readonly [networkErrorBrand] = true as const

  /** `true` if the failure happened on a channel connection, `false` for a regular telefunction call. */
  readonly isChannel: boolean

  constructor(message: string, isChannel: boolean, options?: { cause?: unknown }) {
    super(message, options)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'NetworkError'
    this.isChannel = isChannel
    Error.captureStackTrace?.(this, new.target)
  }

  /** `instanceof NetworkError` reads the mark every copy of Telefunc puts on its network errors, as a Vite dev server
   *  loads one copy beside Node's: an app's `NetworkError` is then another class than the one its channel's error was
   *  made from. A subclass takes only its own kind, by its prototype. */
  static [Symbol.hasInstance](thing: unknown): boolean {
    if (this !== NetworkError) return Function.prototype[Symbol.hasInstance].call(this, thing)
    return typeof thing === 'object' && thing !== null && networkErrorBrand in thing
  }
}
