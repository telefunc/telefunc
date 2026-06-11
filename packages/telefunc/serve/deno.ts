export { telefunc }

import crossws from 'crossws/adapters/deno'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import type { Telefunc } from '../node/server/context/getContext.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest, toResponse } from './shared.js'

type DenoWs = ReturnType<typeof crossws>
type DenoInfo = Parameters<DenoWs['handleUpgrade']>[1]

type ServeInput = {
  request: Request
  info: DenoInfo
  context?: Telefunc.Context
}

interface TelefuncServe {
  serve(input: ServeInput): Promise<Response | undefined>
}

function telefunc(): TelefuncServe {
  enableChannelTransports([CHANNEL_TRANSPORT.WS])
  const ws = crossws({ hooks: getTelefuncChannelHooks() })

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
