export { telefuncWebSocket }

import crossws from 'crossws/adapters/deno'
import { getTelefuncChannelHooks } from '../ws.js'
import { getServerConfig, enableChannelTransports } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../../constants.js'

type DenoWs = ReturnType<typeof crossws>
type DenoInfo = Parameters<DenoWs['handleUpgrade']>[1]

/** Return type of {@link telefuncWebSocket}. */
interface TelefuncAdapter {
  /**
   * Call in your `Deno.serve` handler. Returns the upgrade response when the
   * request targets the Telefunc WebSocket URL, or `undefined` otherwise.
   */
  handleUpgrade(request: Request, info: DenoInfo): Response | Promise<Response> | undefined
}

/**
 * Create a Telefunc WebSocket adapter for Deno.
 *
 * @example
 * ```ts
 * import { telefuncWebSocket } from 'telefunc/websocket/deno'
 *
 * const ws = telefuncWebSocket()
 *
 * Deno.serve({ port: 3000 }, (request, info) => {
 *   const upgraded = ws.handleUpgrade(request, info)
 *   if (upgraded) return upgraded
 *   // ... rest of your handler
 * })
 * ```
 */
function telefuncWebSocket(): TelefuncAdapter {
  const ws = crossws({ hooks: getTelefuncChannelHooks() })

  return {
    handleUpgrade(request: Request, info: DenoInfo): Response | Promise<Response> | undefined {
      enableChannelTransports([CHANNEL_TRANSPORT.WS])
      const url = new URL(request.url)
      const config = getServerConfig()
      if (url.pathname !== config.telefuncUrl || request.headers.get('upgrade') !== 'websocket') {
        return undefined
      }
      if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        return new Response(null, { status: 400 })
      }
      return ws.handleUpgrade(request, info) as Response | Promise<Response>
    },
  }
}
