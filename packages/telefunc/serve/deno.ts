export { Telefunc }

import crossws from 'crossws/adapters/deno'
import { serveRuntimeRequest } from '../node/server/telefunc.js'
import { hasProp } from '../utils/hasProp.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { getTelefuncRequestUrl, toResponse } from './shared.js'

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
  const { hooks, getNamespace } = getTelefuncChannelOptions({
    // TODO: remove once Telefunc's own WebSocket adapter replaces crossws (https://github.com/h3js/crossws/pull/216, closed unmerged).
    // Deno's WebSocket has no terminate(), which crossws's Deno peer calls: close it instead.
    terminate: (peer) => peer.close(),
    copiesOnSend: true,
  })
  const ws = crossws({
    getNamespace,
    hooks: {
      upgrade: hooks.upgrade,
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
      if (getTelefuncRequestUrl(request) === null) return undefined
      const config = getServerConfig()
      if (request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, info) as Response | Promise<Response>
      }

      // Deno.serve aborts `request.signal` once its client leaves while the response is open, and also, unless Deno runs
      // with `--unstable-no-legacy-abort`, at every response's end: the response ends only once the call's body is over,
      // where the signal stops counting (`runTelefunc`), so that abort is never taken for the client leaving. A read
      // under way may fail only once the response ends.
      const httpResponse = await serveRuntimeRequest(
        context ? { request, context } : { request },
        clientLeft,
        request.signal,
      )
      return toResponse(httpResponse)
    },
  }
}

/** Deno.serve fails the read of a request it made with its own `BadResource` (the client closed) or `Http` (the client
 *  reset) error. */
function clientLeft(err: unknown): boolean {
  return isDenoConnectionError(err, globalThis)
}

/** `global` is `globalThis`, whose `Deno` isn't typed here. */
function isDenoConnectionError(err: unknown, global: object): boolean {
  if (!hasProp(global, 'Deno', 'object') || !hasProp(global.Deno, 'errors', 'object')) return false
  const { errors } = global.Deno
  if (!hasProp(errors, 'BadResource', 'function') || !hasProp(errors, 'Http', 'function')) return false
  return err instanceof errors.BadResource || err instanceof errors.Http
}
