export { TransportSocket }

import type { PieceSocket } from '../../flowControl/pieces.js'
import type { ServerTransport, Wire } from './ChannelMux.js'

type TransportSocketOptions = {
  transport: ServerTransport<unknown>
  wire: Wire
}

/** A server wire's socket, as its piece sender hands it messages. */
class TransportSocket implements PieceSocket {
  private readonly transport: ServerTransport<unknown>
  private readonly wire: Wire

  constructor({ transport, wire }: TransportSocketOptions) {
    this.transport = transport
    this.wire = wire
  }

  send(message: Uint8Array<ArrayBuffer>): void {
    this.transport.sendNow(this.wire, message)
  }

  get bufferedAmount(): number {
    return this.transport.bufferedAmount(this.wire) ?? 0
  }
}
