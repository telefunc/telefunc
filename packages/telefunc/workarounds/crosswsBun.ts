// Workaround: crossws's Bun peer hands out its socket as a Proxy, and Bun's `getBufferedAmount()` refuses a Proxy as
// `this` (it throws): without the socket itself, channels can't bound what waits to be sent. Remove once crossws's Bun
// peer exposes the buffered amount, or Telefunc's own WebSocket adapter replaces crossws.
export { bunSockets }

import type { Peer } from 'crossws'
import { assert } from '../utils/assert.js'

type BunSocket = { data: { peer?: Peer }; getBufferedAmount(): number }

/** Each peer's own Bun socket, taken at its `open`, for its buffered amount. */
function bunSockets() {
  const sockets = new WeakMap<Peer, BunSocket>()
  return {
    opened(socket: BunSocket): void {
      const { peer } = socket.data
      assert(peer)
      sockets.set(peer, socket)
    },
    bufferedAmount: (peer: Peer): number => sockets.get(peer)?.getBufferedAmount() ?? 0,
  }
}
