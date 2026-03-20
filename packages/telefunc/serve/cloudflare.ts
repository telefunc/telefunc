/// <reference types="@cloudflare/workers-types" />

export { telefunc }

import { telefuncWebSocket } from '../wire-protocol/server/adapter/cloudflare.js'
import type { CloudflareWebSocketOptions } from '../wire-protocol/server/adapter/cloudflare.js'

type ServeOptions = CloudflareWebSocketOptions
type ServeInput = {
  request: Request
  env: Cloudflare.Env
  ctx: ExecutionContext
}

interface TelefuncServe {
  serve(input: ServeInput): Promise<Response> | undefined
  TelefuncDurableObject: ReturnType<ReturnType<typeof telefuncWebSocket>['createDurableObjectClass']>
}

function telefunc(options?: ServeOptions): TelefuncServe {
  const adapter = telefuncWebSocket(options)

  return {
    serve({ request, env, ctx }: ServeInput): Promise<Response> | undefined {
      return adapter.handleTelefunc(request, env, ctx)
    },
    TelefuncDurableObject: adapter.createDurableObjectClass(),
  }
}
