export { handleTelefunc, $TelefuncDurableObject }

import { telefuncWebSocket } from 'telefunc/websocket/cloudflare'

const ws = telefuncWebSocket({ shards: 5 })

const $TelefuncDurableObject = ws.createDurableObjectClass()

function handleTelefunc(request: Request, env: Env, ctx: ExecutionContext) {
  return ws.handleTelefunc(request, env, ctx)
}
