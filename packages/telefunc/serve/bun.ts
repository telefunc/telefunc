export { Telefunc }

import crossws from 'crossws/adapters/bun'
import type { Peer } from 'crossws'
import { serveRuntimeRequest } from '../node/server/telefunc.js'
import { upgradesTakenHere } from '../node/server/runTelefunc/takesUpgrades.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT, WIRE_MAX_RAW_FRAME_BYTES } from '../wire-protocol/constants.js'
import { getTelefuncRequestUrl, toResponse } from './shared.js'

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
  // A peer's `websocket` is a Proxy, and Bun's methods refuse one as `this`: the socket itself answers.
  const sockets = new WeakMap<Peer, BunSocket>()
  const ws = crossws(
    getTelefuncChannelOptions({
      bufferedAmount: (peer) => sockets.get(peer)?.getBufferedAmount() ?? 0,
      copiesOnSend: true,
    }),
  )

  return {
    websocket: {
      ...ws.websocket,
      // Bun drops sends past 16 MiB queued, a gap in a channel; channels bound what waits by getBufferedAmount().
      backpressureLimit: 0,
      // Bun closes the socket on a message over 16 MiB by default, below the frames telefunc accepts.
      maxPayloadLength: WIRE_MAX_RAW_FRAME_BYTES,
      open(socket: BunSocket) {
        ws.websocket.open!(socket)
        sockets.set(socket.data.peer!, socket)
      },
    },
    async serve({ request, server, context }: ServeInput): Promise<Response | undefined> {
      if (getTelefuncRequestUrl(request) === null) return undefined
      const config = getServerConfig()
      if (request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, server) as Response | Promise<Response>
      }
      // Bun.serve aborts the signal of a request it made once its client disconnects, before the body's read fails. Once
      // the request is answered it leaves the signal be, and fails the read with an `AbortError` ("The connection was
      // closed.").
      const clientLeft = (err: unknown) =>
        request.signal.aborted || (err instanceof DOMException && err.name === 'AbortError')
      const httpResponse = await serveRuntimeRequest(context ? { request, context } : { request }, {
        clientLeft,
        clientGone: request.signal,
        takesUpgrades: upgradesTakenHere,
        // Bun cuts a request quiet for `idleTimeout` (10 s by default): a body that stops short, and also a slow call, a
        // stream or an SSE wire. Once the body is read, telefunc ends its own responses.
        onBodyRead: () => server.timeout(request, 0),
      })
      return toResponse(httpResponse)
    },
  }
}
