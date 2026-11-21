export { getTelefuncChannelHooks }

import { defineHooks, type Hooks, type Peer } from 'crossws'
import { enableChannelTransports } from '../../../node/server/serverConfig.js'
import { WS_CLOSE_PAGE_LEFT, WS_CLOSE_WIRE_VERSION } from '../../constants.js'
import { namesWireVersion } from '../../wire/requestKind.js'
import { getChannelMux } from './ChannelMux.js'
import type { ServerTransport } from './ChannelMux.js'

declare module 'crossws' {
  interface PeerContext {
    telefuncSessionId?: string
  }
}

/** `bufferedAmount` reads the socket's: `undefined` on workerd, whose WebSocket has none. */
function getTelefuncChannelHooks({
  upgrade,
  send = (peer, frame) => peer.send(frame),
  terminate = (peer) => peer.terminate(),
  bufferedAmount = (peer) => peer.websocket.bufferedAmount,
  copiesOnSend,
}: {
  upgrade?: Hooks['upgrade']
  send?: (peer: Peer, frame: Uint8Array<ArrayBuffer>) => void
  terminate?: (peer: Peer) => void
  bufferedAmount?: (peer: Peer) => number | undefined
  copiesOnSend?: boolean
} = {}) {
  enableChannelTransports(['ws'])
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
    copiesOnSend,
    bufferedAmount,
    // Closed at once: a Durable Object peer's terminate() is a close handshake a vanished client never answers.
    terminateWire: (peer) => {
      const mux = getChannelMux()
      const permanent = mux.readPermanentTermination(peer)
      terminate(peer)
      mux.onWireClosed(peer, { permanent })
    },
  }

  return defineHooks({
    upgrade,
    open: (peer) => {
      // A page on another version reloads: none of its frames could be read.
      if (!namesWireVersion(peer.request)) return peer.close(WS_CLOSE_WIRE_VERSION, 'reload the page')
      getChannelMux().onWireOpen(peer, transport)
    },
    message: (peer, message) => getChannelMux().onWireMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
    close: (peer, details) => {
      const mux = getChannelMux()
      const isPermanent = mux.readPermanentTermination(peer) || details?.code === WS_CLOSE_PAGE_LEFT
      mux.onWireClosed(peer, { permanent: isPermanent })
    },
    error: (peer) => getChannelMux().onWireClosed(peer, { permanent: false }),
  })
}
