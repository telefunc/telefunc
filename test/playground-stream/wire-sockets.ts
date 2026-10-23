export { trackWireSocket, cutWireSockets }

import type { Duplex } from 'node:stream'

// The connections carrying the page's WebSocket and SSE requests, so a test can drop them as a failing network would.
const wireSockets: Set<Duplex> = ((globalThis as Record<symbol, unknown>)[
  Symbol.for('playground-stream:wireSockets')
] ??= new Set<Duplex>()) as Set<Duplex>

function trackWireSocket(socket: Duplex) {
  if (wireSockets.has(socket)) return
  wireSockets.add(socket)
  socket.once('close', () => wireSockets.delete(socket))
}

/** Returns how many connections it dropped. */
function cutWireSockets(): number {
  const cut = wireSockets.size
  for (const socket of wireSockets) socket.destroy()
  return cut
}
