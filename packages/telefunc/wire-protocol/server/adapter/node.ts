export { telefuncWebSocket }

import crossws from 'crossws/adapters/node'
import { getTelefuncChannelHooks } from '../ws.js'
import type { TelefuncWebSocketOptions } from '../ws.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import type { Server } from 'node:http'
import type { Http2SecureServer } from 'node:http2'

type HttpServer = Server | Http2SecureServer
// Accept either a plain Node.js server or a srvx-style wrapper ({ node: { server } })
type HttpServerOrWrapper = HttpServer | { node?: { server?: HttpServer } }
const registeredServers = new WeakSet<HttpServer>()

/** Return type of {@link telefuncWebSocket}. */
interface TelefuncAdapter {
  /**
   * Install the WebSocket upgrade handler on a Node.js HTTP server.
   * Idempotent — safe to call multiple times on the same server.
   * Accepts a plain `http.Server` or a srvx-style wrapper (e.g. from `@photonjs/hono`).
   */
  install(server: HttpServerOrWrapper): void
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
function telefuncWebSocket(options?: TelefuncWebSocketOptions): TelefuncAdapter {
  const ws = crossws({ hooks: getTelefuncChannelHooks(options) })

  return {
    install(server: HttpServerOrWrapper): void {
      // Unwrap srvx-style wrappers (e.g. from @photonjs/hono or srvx directly)
      const httpServer: HttpServer = (server as any)?.node?.server ?? (server as HttpServer)
      if (typeof (httpServer as any)?.on !== 'function') {
        throw new Error(
          'telefuncWebSocket().install() received an unsupported server object. Pass a Node.js `http.Server` or a srvx-compatible wrapper.',
        )
      }
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
