export { telefunc }

import { serve as serveTelefunc } from '../node/server/telefunc.js'
import type { Telefunc } from '../node/server/getContext.js'
import { telefuncWebSocket } from '../wire-protocol/server/adapter/bun.js'
import { isTelefuncRequest, toResponse } from './shared.js'

type BunAdapter = ReturnType<typeof telefuncWebSocket>
type BunServer = Parameters<BunAdapter['handleUpgrade']>[1]

type ServeInput = {
  request: Request
  server: BunServer
  context?: Telefunc.Context
}

interface TelefuncServe {
  websocket: BunAdapter['websocket']
  serve(input: ServeInput): Promise<Response | undefined>
}

function telefunc(): TelefuncServe {
  const adapter = telefuncWebSocket()

  return {
    websocket: adapter.websocket,
    async serve({ request, server, context }: ServeInput): Promise<Response | undefined> {
      const upgraded = adapter.handleUpgrade(request, server)
      if (upgraded) return upgraded
      if (!isTelefuncRequest(request)) return undefined

      const httpResponse = await serveTelefunc(context ? { request, context } : { request })
      return toResponse(httpResponse)
    },
  }
}
