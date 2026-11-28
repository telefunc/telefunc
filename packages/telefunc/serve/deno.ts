export { Telefunc }

import crossws from 'crossws/adapters/deno'
import { serve } from '../node/server/telefunc.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest, toResponse } from './shared.js'
import { denoHooks, terminateDenoPeer } from '../workarounds/crosswsDeno.js'

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
  const { hooks, ...options } = getTelefuncChannelOptions({
    terminate: terminateDenoPeer,
    copiesOnSend: true,
  })
  const ws = crossws({ ...options, hooks: denoHooks(hooks) })

  return {
    async serve({ request, info, context }: ServeInput): Promise<Response | undefined> {
      if (!isTelefuncRequest(request)) return undefined
      const config = getServerConfig()
      if (request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, info) as Response | Promise<Response>
      }

      const httpResponse = await serve(context ? { request, context } : { request })
      return toResponse(httpResponse)
    },
  }
}
