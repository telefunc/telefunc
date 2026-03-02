export { pluginWebSocket }

import type { Plugin } from 'vite'
import type { Server } from 'node:http'

/**
 * Vite plugin that automatically registers the Telefunc WebSocket upgrade handler
 * in dev mode. This allows `createChannel()` to work transparently during development
 * without the user needing to call `telefuncWebSocket(server)` manually.
 */
function pluginWebSocket(): Plugin[] {
  return [
    {
      name: 'telefunc:pluginWebSocket',
      configureServer(server) {
        if (server.httpServer) {
          // Dynamic import to avoid loading crossws at build time
          import('../../server/telefuncWebSocket.js').then(({ telefuncWebSocket }) => {
            telefuncWebSocket(server.httpServer as Server)
          })
        }
      },
    },
  ]
}
