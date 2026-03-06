export { telefuncWebSocket }

import crossws from 'crossws/adapters/node'
import { getTelefuncChannelHooks } from '../ws.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import type { Server } from 'node:http'
import type { Http2SecureServer } from 'node:http2'

type HttpServer = Server | Http2SecureServer
const registeredServers = new WeakSet<HttpServer>()

/** Return type of {@link telefuncWebSocket}. */
interface TelefuncAdapter {
  /**
   * Install the WebSocket upgrade handler on a Node.js HTTP server.
   * Idempotent — safe to call multiple times on the same server.
   */
  install(server: HttpServer): void
}

/**
 * Create a Telefunc WebSocket adapter for Node.js.
 *
 * @example
 * ```ts
 * import { telefuncWebSocket } from 'telefunc/websocket/node'
 *
 * // Express
 * const server = app.listen(3000)
 * telefuncWebSocket().install(server)
 *
 * // Hono with Node adapter
 * import { serve } from '@hono/node-server'
 * const server = serve({ fetch: app.fetch })
 * telefuncWebSocket().install(server)
 * ```
 */
function telefuncWebSocket(): TelefuncAdapter {
  const ws = crossws({ hooks: getTelefuncChannelHooks() })

  return {
    install(httpServer: HttpServer): void {
      if (registeredServers.has(httpServer)) return
      registeredServers.add(httpServer)

      httpServer.on('upgrade', (req, socket, head) => {
        const url = new URL(req.url ?? '', 'http://localhost')
        const telefuncUrl = getServerConfig().telefuncUrl
        if (url.pathname !== telefuncUrl) return
        ws.handleUpgrade(req, socket, head)
      })
    },
  }
}
