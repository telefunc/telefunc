export { telefuncWebSocket }

import type { Server } from 'node:http'
import crossws from 'crossws/adapters/node'
import { getTelefuncChannelHooks } from './telefuncChannelHooks.js'
import { getServerConfig } from './serverConfig.js'

const registeredServers = new WeakSet<Server>()

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
function telefuncWebSocket(httpServer: Server): void {
  if (registeredServers.has(httpServer)) return
  registeredServers.add(httpServer)

  const ws = crossws({ hooks: getTelefuncChannelHooks() })
  httpServer.on('upgrade', (req, socket, head) => {
    // Only handle upgrade requests targeting the telefunc URL with a channelId.
    // Let all other upgrades pass through untouched.
    const url = new URL(req.url ?? '', 'http://localhost')
    const telefuncUrl = getServerConfig().telefuncUrl
    if (url.pathname !== telefuncUrl || !url.searchParams.has('channelId')) return
    ws.handleUpgrade(req, socket, head)
  })
}
