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
    terminateConnection: (peer) => peer.terminate(),
  }

  return defineHooks({
    open: (peer) => mux.onConnectionOpen(peer, transport),
    message: (peer, message) => mux.onConnectionRawMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
    close: (peer, details) => {
      const terminatePermanently = mux.consumePermanentTermination(peer)
      const isPermanent =
        terminatePermanently === true ||
        (terminatePermanently === null && (details?.code === 1000 || details?.code === 1001))
      mux.onConnectionClosed(peer, isPermanent)
    },
    error: (peer) => mux.onConnectionClosed(peer, false),
  })
}
