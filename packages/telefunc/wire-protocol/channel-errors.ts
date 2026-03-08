export { ChannelClosedError, ChannelNetworkError }

/** Thrown synchronously by `send()` when the channel is already closed.
 *  Also used to reject pending ack promises when the channel shuts down. */
class ChannelClosedError extends Error {
  constructor() {
    super('Channel is closed')
    this.name = 'ChannelClosedError'
  }
}

/** Passed to `onClose` and used to reject pending ack promises when the channel closes
 *  due to a network or timeout failure: TTL expired, client failed to reconnect, WebSocket
 *  connection rejected, reconnect timeout exceeded, or channel not re-acknowledged after reconnect. */
class ChannelNetworkError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ChannelNetworkError'
  }
}
