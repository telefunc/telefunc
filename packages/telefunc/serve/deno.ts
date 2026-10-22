export { Telefunc }

import crossws from 'crossws/adapters/deno'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { getTelefuncChannelHooks } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest, toResponse } from './shared.js'

type DenoWs = ReturnType<typeof crossws>
type DenoInfo = Parameters<DenoWs['handleUpgrade']>[1]

type ServeInput = {
  request: Request
  info: DenoInfo
  context?: TelefuncNamespace.Context
}

interface TelefuncServe {
  serve(input: ServeInput): Promise<Response | undefined>
}

interface Telefunc extends TelefuncServe {}
class Telefunc {
  constructor() {
    return telefunc()
  }
}

function telefunc(): TelefuncServe {
  enableChannelTransports([CHANNEL_TRANSPORT.WS])
  // TODO: remove once Telefunc's own WebSocket adapter replaces crossws (https://github.com/h3js/crossws/pull/216, closed unmerged).
  // Deno's WebSocket has no terminate(), which crossws's Deno peer calls: close it instead.
  const hooks = getTelefuncChannelHooks({ terminate: (peer) => peer.close() })
  const ws = crossws({
    hooks: {
      open(peer) {
        // TODO: remove once Telefunc's own WebSocket adapter replaces crossws (https://github.com/h3js/crossws/pull/213, closed unmerged).
        // crossws's Deno adapter reports a close without its code, and the code tells a page that left from a dropped one.
        peer.websocket.addEventListener!('close', (event) =>
          hooks.close(peer, { code: event.code, reason: event.reason }),
        )
        return hooks.open(peer)
      },
      message: hooks.message,
      close() {},
      error: hooks.error,
    },
  })

  return {
    async serve({ request, info, context }: ServeInput): Promise<Response | undefined> {
      const url = new URL(request.url)
      const config = getServerConfig()
      if (url.pathname === config.telefuncUrl && request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, info) as Response | Promise<Response>
      }
      if (!isTelefuncRequest(request)) return undefined

      const httpResponse = await serveTelefunc(context ? { request, context } : { request })
      return toResponse(httpResponse)
    },
  }
}
