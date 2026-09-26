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

function getTelefuncChannelHooks() {
  enableChannelTransports(['ws'])
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
    // Closed at once, as an SSE wire is: a Durable Object peer's terminate() is a close handshake a vanished client
    // never answers, so its close hook would never run.
    terminateConnection: (peer) => {
      const mux = getChannelMux()
      const permanent = mux.readPermanentTermination(peer) === true
      peer.terminate()
      mux.onConnectionClosed(peer, { permanent })
    },
  }

  return defineHooks({
    open: (peer) => getChannelMux().onConnectionOpen(peer, transport),
    message: (peer, message) =>
      getChannelMux().onConnectionRawMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
    close: (peer, details) => {
      const mux = getChannelMux()
      const terminatePermanently = mux.readPermanentTermination(peer)
      const isPermanent =
        terminatePermanently === true ||
        (terminatePermanently === null && (details?.code === 1000 || details?.code === 1001))
      mux.onConnectionClosed(peer, { permanent: isPermanent })
    },
    error: (peer) => getChannelMux().onConnectionClosed(peer, { permanent: false }),
  })
}
