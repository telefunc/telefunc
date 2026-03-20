export { telefunc }

import { serve as serveTelefunc } from '../node/server/telefunc.js'
import type { Telefunc } from '../node/server/getContext.js'
import { telefuncWebSocket } from '../wire-protocol/server/adapter/deno.js'
import { isTelefuncRequest, toResponse } from './shared.js'

type DenoAdapter = ReturnType<typeof telefuncWebSocket>
type DenoInfo = Parameters<DenoAdapter['handleUpgrade']>[1]

type ServeInput = {
  request: Request
  info: DenoInfo
  context?: Telefunc.Context
}

interface TelefuncServe {
  serve(input: ServeInput): Promise<Response | undefined>
}

function telefunc(): TelefuncServe {
  const adapter = telefuncWebSocket()

  return {
    async serve({ request, info, context }: ServeInput): Promise<Response | undefined> {
      const upgraded = adapter.handleUpgrade(request, info)
      if (upgraded) return upgraded
      if (!isTelefuncRequest(request)) return undefined

      const httpResponse = await serveTelefunc(context ? { request, context } : { request })
      return toResponse(httpResponse)
    },
  }
}
