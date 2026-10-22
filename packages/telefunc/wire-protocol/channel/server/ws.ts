export { getTelefuncChannelHooks }

import { defineHooks, type Hooks, type Peer } from 'crossws'
import { enableChannelTransports } from '../../../node/server/serverConfig.js'
import { getChannelMux } from './mux.js'
import type { ServerTransport } from './mux.js'

declare module 'crossws' {
  interface PeerContext {
    telefuncSessionId?: string
  }
}

/** `bufferedAmount` reads the socket's: `undefined` on workerd, whose WebSocket has none. `onMessage` runs as each
 *  message arrives, before the mux takes it. */
function getTelefuncChannelHooks({
  upgrade,
  send = (peer, frame) => peer.send(frame),
  terminate = (peer) => peer.terminate(),
  bufferedAmount = (peer) => peer.websocket.bufferedAmount,
  wireRequest = (peer) => peer.request,
  onMessage = () => {},
}: {
  upgrade?: Hooks['upgrade']
  send?: (peer: Peer, frame: Uint8Array<ArrayBuffer>) => void
  terminate?: (peer: Peer) => void
  bufferedAmount?: (peer: Peer) => number | undefined
  wireRequest?: (peer: Peer) => Request
  onMessage?: (peer: Peer) => void
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
    sendNow: send,
    wholeMessages: true,
    bufferedAmount,
    // Closed at once: a Durable Object peer's terminate() is a close handshake a vanished client never answers.
    terminateConnection: (peer) => {
      const permanent = mux.readPermanentTermination(peer)
      terminate(peer)
      mux.onConnectionClosed(peer, { permanent })
    },
  }

  return defineHooks({
    upgrade,
    open: (peer) => mux.onConnectionOpen(peer, transport, wireRequest(peer)),
    message: (peer, message) => {
      onMessage(peer)
      return mux.onConnectionRawMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>)
    },
    close: (peer, details) => {
      const isPermanent = mux.readPermanentTermination(peer) || details?.code === 1000 || details?.code === 1001
      mux.onConnectionClosed(peer, { permanent: isPermanent })
    },
    error: (peer) => mux.onConnectionClosed(peer, { permanent: false }),
  })
}
