export { Telefunc }

import crossws from 'crossws/adapters/bun'
import { serve } from '../node/server/telefunc.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT, WIRE_MAX_RAW_FRAME_BYTES } from '../wire-protocol/constants.js'
import { isTelefuncRequest, toResponse } from './shared.js'

type BunWs = ReturnType<typeof crossws>
type BunServer = Parameters<BunWs['handleUpgrade']>[1]

type ServeInput = {
  request: Request
  server: BunServer
  context?: TelefuncNamespace.Context
}

interface TelefuncServe {
  websocket: BunWs['websocket']
  serve(input: ServeInput): Promise<Response | undefined>
}

interface Telefunc extends TelefuncServe {}
class Telefunc {
  constructor() {
    return telefunc()
  }
}

function telefunc(): TelefuncServe {
  const ws = crossws(getTelefuncChannelOptions({ copiesOnSend: true }))

  return {
    websocket: {
      ...ws.websocket,
      // Bun drops sends past 16 MiB queued, a gap in a channel; channels bound what waits by `peer.bufferedAmount`.
      backpressureLimit: 0,
      // Bun closes the socket on a message over 16 MiB by default, below the frames telefunc accepts.
      maxPayloadLength: WIRE_MAX_RAW_FRAME_BYTES,
    },
    async serve({ request, server, context }: ServeInput): Promise<Response | undefined> {
      if (!isTelefuncRequest(request)) return undefined
      const config = getServerConfig()
      if (request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, server) as Response | Promise<Response>
      }
      // Bun applies its idle timeout to quiet streams and SSE wires.
      server.timeout(request, 0)
      const httpResponse = await serve(context ? { request, context } : { request })
      return toResponse(httpResponse)
    },
  }
}
