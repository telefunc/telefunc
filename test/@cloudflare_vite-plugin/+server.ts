import vike from 'vike/fetch'
import { telefunc } from 'telefunc/cloudflare'

const tf = telefunc({ scale: 5 })

// Cloudflare requires Durable Object classes to be named exports of the worker entry.
// `wrangler.jsonc`'s `main: "vike:server-entry"` re-exports everything from this file,
// so these reach Cloudflare's binding resolver intact.
export const TelefuncDurableObject = tf.TelefuncDurableObject
export { TodoListDurableObject } from './database/todoItems'

// vike's docs example uses `export default vike` directly — meaning when vike is the
// worker entry's default, Cloudflare invokes vike.fetch with `(request, env, ctx)`.
// Type-wise vike/fetch is generic-typed for universal-middleware, but at runtime it
// IS a Cloudflare handler. Cast for the wrapper call.
const vikeAsCloudflareHandler = vike as unknown as ExportedHandler<Env>

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const resp = await tf.serve({ request, env, ctx })
    return resp ?? vikeAsCloudflareHandler.fetch!(request, env, ctx)
  },
} satisfies ExportedHandler<Env>
