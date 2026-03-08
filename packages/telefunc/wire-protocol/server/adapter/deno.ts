export { telefuncWebSocket }

import crossws from 'crossws/adapters/deno'
import { getTelefuncChannelHooks } from '../ws.js'
import type { TelefuncWebSocketOptions } from '../ws.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'

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
function telefuncWebSocket(options?: TelefuncWebSocketOptions): TelefuncAdapter {
  const ws = crossws({ hooks: getTelefuncChannelHooks(options) })

  return {
    handleUpgrade(request: Request, info: DenoInfo): Response | Promise<Response> | undefined {
      const url = new URL(request.url)
      const telefuncUrl = getServerConfig().telefuncUrl
      if (url.pathname !== telefuncUrl || request.headers.get('upgrade') !== 'websocket') {
        return undefined
      }
      return ws.handleUpgrade(request, info) as Response | Promise<Response>
    },
  }
}
