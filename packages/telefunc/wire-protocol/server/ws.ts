export { getTelefuncChannelHooks }

import { defineHooks, type Peer } from 'crossws'
import { ServerConnection } from './connection.js'
import { enableChannelTransports } from '../../node/server/serverConfig.js'

declare module 'crossws' {
  interface PeerContext {
    telefuncSessionId?: string
  }
}

function getTelefuncChannelHooks() {
  enableChannelTransports(['ws'])
  const connection = new ServerConnection<Peer>({
    getSessionId(peer) {
      return peer.context.telefuncSessionId
    },
    setSessionId(peer, sessionId) {
      peer.context.telefuncSessionId = sessionId
    },
    sendNow(peer, frame) {
      peer.send(frame)
    },
    terminateConnection(peer) {
      peer.terminate()
    },
  })

  return defineHooks({
    open: (peer) => {
      connection.onConnectionOpen(peer)
    },
    message: (peer, message) => {
      return connection.onConnectionRawMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>)
    },
    close: (peer, details) => {
      const terminatePermanently = connection.consumePermanentTermination(peer)
      const isPermanent =
        terminatePermanently === true ||
        (terminatePermanently === null && (details?.code === 1000 || details?.code === 1001))
      connection.onConnectionClosed(peer, isPermanent)
    },
    error: (peer) => {
      connection.onConnectionClosed(peer, false)
    },
  })
}
