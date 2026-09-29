export { Telefunc }

import crossws from 'crossws/adapters/bun'
import type { Peer } from 'crossws'
import { serve as serveTelefunc } from '../node/server/telefunc.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { getTelefuncChannelHooks } from '../wire-protocol/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest, toResponse } from './shared.js'

type BunWs = ReturnType<typeof crossws>
type BunServer = Parameters<BunWs['handleUpgrade']>[1]
type BunSocket = Parameters<NonNullable<BunWs['websocket']['open']>>[0]

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
  enableChannelTransports([CHANNEL_TRANSPORT.WS])
  // A peer's `websocket` is a Proxy, and Bun's methods refuse one as `this`: the socket itself answers.
  const sockets = new WeakMap<Peer, BunSocket>()
  const ws = crossws({
    hooks: getTelefuncChannelHooks(undefined, (peer) => sockets.get(peer)!.getBufferedAmount()),
  })

  return {
    websocket: {
      ...ws.websocket,
      // Bun drops a send once 16 MiB wait on the socket (its default backpressureLimit), leaving a gap in a channel,
      // and closing there instead would cut a credited stream, whose windows can hold more. 0 is no limit in
      // uWebSockets, as on Node's ws and Deno's WebSocket: channels bound what they send past credit by
      // getBufferedAmount().
      backpressureLimit: 0,
      open(socket: BunSocket) {
        ws.websocket.open!(socket)
        sockets.set(socket.data.peer!, socket)
      },
    },
    async serve({ request, server, context }: ServeInput): Promise<Response | undefined> {
      const url = new URL(request.url)
      const config = getServerConfig()
      if (url.pathname === config.telefuncUrl && request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, server) as Response | Promise<Response>
      }
      if (!isTelefuncRequest(request)) return undefined

      const httpResponse = await serveTelefunc(context ? { request, context } : { request })
      return toResponse(httpResponse)
    },
  }
}
