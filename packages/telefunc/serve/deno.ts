export { Telefunc }

import crossws from 'crossws/adapters/deno'
import { serveRuntimeRequest } from '../node/server/telefunc.js'
import { upgradesTakenHere } from '../node/server/runTelefunc/takesUpgrades.js'
import { hasProp } from '../utils/hasProp.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { preloadServerProductionEntry } from '../node/server/runTelefunc/loadTelefuncFilesUsingVite.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { getTelefuncRequestUrl, toResponse } from './shared.js'
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
      await preloadServerProductionEntry()
      if (getTelefuncRequestUrl(request) === null) return undefined
      const config = getServerConfig()
      if (request.headers.get('upgrade') === 'websocket') {
        if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) return new Response(null, { status: 400 })
        return ws.handleUpgrade(request, info) as Response | Promise<Response>
      }

      // Deno.serve aborts `request.signal` once its client leaves while the response is open, and, in its legacy abort
      // mode, at every response's end: workarounds/denoUploadResponseHold.ts holds a call's response until its upload
      // is over.
      const httpResponse = await serveRuntimeRequest(context ? { request, context } : { request }, {
        clientLeft: (err) => isDenoConnectionError(err, globalThis),
        clientGone: request.signal,
        takesUpgrades: upgradesTakenHere,
      })
      return toResponse(httpResponse)
    },
  }
}

/** Deno.serve fails the read of a request it made with its own `BadResource` (the client closed) or `Http` (the client
 *  reset) error. `global` is `globalThis`, whose `Deno` isn't typed here. */
function isDenoConnectionError(err: unknown, global: object): boolean {
  if (!hasProp(global, 'Deno', 'object') || !hasProp(global.Deno, 'errors', 'object')) return false
  const { errors } = global.Deno
  if (!hasProp(errors, 'BadResource', 'function') || !hasProp(errors, 'Http', 'function')) return false
  return err instanceof errors.BadResource || err instanceof errors.Http
}
