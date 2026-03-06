export { telefuncWebSocket }

import crossws from 'crossws/adapters/node'
import { getTelefuncChannelHooks } from '../../wire-protocol/server/ws.js'
import { getServerConfig } from './serverConfig.js'
import type { Server } from 'node:http'
import type { Http2SecureServer } from 'node:http2'

type HttpServer = Server | Http2SecureServer
const registeredServers = new WeakSet<HttpServer>()

/**
 * Install the Telefunc WebSocket upgrade handler on a Node.js HTTP server.
 * Required for `createChannel()` to work. Idempotent — safe to call multiple times.
 *
 * @example
 * ```ts
 * import { telefuncWebSocket } from 'telefunc'
 *
 * // Express
 * const server = app.listen(3000)
 * telefuncWebSocket(server)
 *
 * // Hono with Node adapter
 * import { serve } from '@hono/node-server'
 * const server = serve({ fetch: app.fetch })
 * telefuncWebSocket(server)
 * ```
 */
function telefuncWebSocket(httpServer: HttpServer): void {
  if (registeredServers.has(httpServer)) return
  registeredServers.add(httpServer)

  const ws = crossws({ hooks: getTelefuncChannelHooks() })
  httpServer.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url ?? '', 'http://localhost')
    const telefuncUrl = getServerConfig().telefuncUrl
    if (url.pathname !== telefuncUrl) return
    ws.handleUpgrade(req, socket, head)
  })
}
