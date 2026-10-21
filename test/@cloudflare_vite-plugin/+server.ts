import vike from 'vike/fetch'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/cloudflare'

const tf = new Telefunc({ scale: 5 })

// The Cloudflare page's Hibernation snippet, assigned after the adapter.
config.channel = {
  reconnectTimeout: 5_000,
  idleTimeout: 5_000,
  pingInterval: 2_000,
}

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
    const url = new URL(request.url)
    // Test-only: the run the backlog page asked for, as its telefunction recorded it; a POST says the page is stalled.
    if (url.pathname === '/backlog-run') {
      const run = `backlog:${url.searchParams.get('channel')}`
      if (request.method === 'POST') await env.TelefuncKV.put(`${run}:stalled`, 'true')
      return new Response(await env.TelefuncKV.get(run))
    }
    const resp = await tf.serve({ request, env, ctx })
    return resp ?? vikeAsCloudflareHandler.fetch!(request, env, ctx)
  },
} satisfies ExportedHandler<Env>
