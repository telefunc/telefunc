export { trackWireSocket, untrackWireSocket, cutWireSockets }

import type { Duplex } from 'node:stream'

// The connections carrying the page's WebSocket and SSE requests, so a test can drop them as a failing network would.
const wireSockets: Set<Duplex> = ((globalThis as Record<symbol, unknown>)[
  Symbol.for('playground-stream:wireSockets')
] ??= new Set<Duplex>()) as Set<Duplex>

const closeWatched: WeakSet<Duplex> = ((globalThis as Record<symbol, unknown>)[
  Symbol.for('playground-stream:wireSocketsWatched')
] ??= new WeakSet<Duplex>()) as WeakSet<Duplex>

function trackWireSocket(socket: Duplex) {
  wireSockets.add(socket)
  if (closeWatched.has(socket)) return
  closeWatched.add(socket)
  socket.once('close', () => wireSockets.delete(socket))
}

/** A kept-alive connection that carried the page's wire carries a call now: dropping it would make the browser send the
 *  call again. */
function untrackWireSocket(socket: Duplex) {
  wireSockets.delete(socket)
}

/** Returns how many connections it dropped. */
function cutWireSockets(): number {
  const cut = wireSockets.size
  for (const socket of wireSockets) socket.destroy()
  return cut
}
