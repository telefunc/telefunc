export { getTelefuncChannelHooks }

import { defineHooks, type Peer } from 'crossws'
import { enableChannelTransports } from '../../node/server/serverConfig.js'
import { getChannelMux } from './substrate.js'
import type { ServerTransport } from './substrate-runtime.js'

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
    /** WebSocket frames always land on the owner instance (the wire is persistent and
     *  bidirectional), so there's no cross-instance routing to support. Returning null
     *  opts the connection out of the substrate's connection-pin machinery. */
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
