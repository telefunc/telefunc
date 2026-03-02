export { getTelefuncChannelHooks }

import { defineHooks } from 'crossws'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry } from './channel.js'
import { getServerConfig } from './serverConfig.js'

declare module 'crossws' {
  interface PeerContext {
    channel: ServerChannel
  }
}

function getTelefuncChannelHooks() {
  return defineHooks({
    upgrade(req) {
      const url = new URL(req.url, 'http://localhost')
      const telefuncUrl = getServerConfig().telefuncUrl
      if (url.pathname !== telefuncUrl) return

      const channelId = url.searchParams.get('channelId')
      if (!channelId) return
      const channel = getChannelRegistry().get(channelId)
      if (!channel) return

      return { context: { channel } }
    },

    open(peer) {
      peer.context.channel.attachPeer(peer)
    },

    message(peer, message) {
      peer.context.channel._onPeerMessage(message.text())
    },

    close(peer) {
      peer.context.channel._onPeerClose()
    },

    error(peer) {
      peer.context.channel._onPeerClose()
    },
  })
}
