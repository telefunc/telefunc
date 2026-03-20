export { telefuncWebSocket }

import crossws from 'crossws/adapters/bun'
import { getTelefuncChannelHooks } from '../ws.js'
import { getServerConfig, enableChannelTransports } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'

type BunWs = ReturnType<typeof crossws>
type BunServer = Parameters<BunWs['handleUpgrade']>[1]

/** Return type of {@link telefuncWebSocket}. */
interface TelefuncAdapter {
  /** Pass this to `Bun.serve({ websocket: ... })`. */
  websocket: BunWs['websocket']
  /**
   * Call in your `fetch` handler. Returns the upgrade response when the
   * request targets the Telefunc WebSocket URL, or `undefined` otherwise.
   */
  handleUpgrade(request: Request, server: BunServer): Response | Promise<Response> | undefined
}

/**
 * Create a Telefunc WebSocket adapter for Bun.
 *
 * @example
 * ```ts
 * import { telefuncWebSocket } from 'telefunc/websocket/bun'
 *
 * const ws = telefuncWebSocket()
 *
 * Bun.serve({
 *   port: 3000,
 *   websocket: ws.websocket,
 *   fetch(request, server) {
 *     const upgraded = ws.handleUpgrade(request, server)
 *     if (upgraded) return upgraded
 *     // ... rest of your fetch handler
 *   },
 * })
 * ```
 */
function telefuncWebSocket(): TelefuncAdapter {
  const ws = crossws({ hooks: getTelefuncChannelHooks() })

  return {
    websocket: ws.websocket,

    handleUpgrade(request: Request, server: BunServer): Response | Promise<Response> | undefined {
      enableChannelTransports([CHANNEL_TRANSPORT.WS])
      const url = new URL(request.url)
      const config = getServerConfig()
      if (url.pathname !== config.telefuncUrl || request.headers.get('upgrade') !== 'websocket') {
        return undefined
      }
      if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }
      return ws.handleUpgrade(request, server) as Response | Promise<Response>
    },
  }
}
