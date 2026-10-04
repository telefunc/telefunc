export { getTelefuncChannelHooks }

import { defineHooks, type Peer } from 'crossws'
import { enableChannelTransports } from '../../node/server/serverConfig.js'
import { getChannelMux } from './mux.js'
import type { ServerTransport } from './mux.js'

declare module 'crossws' {
  interface PeerContext {
    telefuncSessionId?: string
  }
}

/** `bufferedAmount` reads the socket's: Node's ws and Deno's WebSocket have it, and workerd's WebSocket has none, so a
 *  Durable Object's reads `undefined`. */
function getTelefuncChannelHooks({
  terminate = (peer) => peer.terminate(),
  bufferedAmount = (peer) => peer.websocket.bufferedAmount,
}: {
  terminate?: (peer: Peer) => void
  bufferedAmount?: (peer: Peer) => number | undefined
} = {}) {
  enableChannelTransports(['ws'])
  const mux = getChannelMux()
  const transport: ServerTransport<Peer> = {
    getSessionId: (peer) => peer.context.telefuncSessionId,
    setSessionId: (peer, sessionId) => {
      peer.context.telefuncSessionId = sessionId
    },
    /** Persistent bidirectional WebSocket: no out-of-band POSTs to route, so no need for a
     *  connId-based reverse lookup. */
    getConnId: () => null,
    sendNow: (peer, frame) => {
      peer.send(frame)
    },
    bufferedAmount,
    // Closed at once, as an SSE wire is: a Durable Object peer's terminate() is a close handshake a vanished client
    // never answers, so its close hook would run late, if at all.
    terminateConnection: (peer) => {
      const permanent = mux.readPermanentTermination(peer)
      terminate(peer)
      mux.onConnectionClosed(peer, { permanent })
    },
  }

  return defineHooks({
    open: (peer) => mux.onConnectionOpen(peer, transport),
    message: (peer, message) => mux.onConnectionRawMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
    close: (peer, details) => {
      const isPermanent = mux.readPermanentTermination(peer) || details?.code === 1000 || details?.code === 1001
      mux.onConnectionClosed(peer, { permanent: isPermanent })
    },
    error: (peer) => mux.onConnectionClosed(peer, { permanent: false }),
  })
}
