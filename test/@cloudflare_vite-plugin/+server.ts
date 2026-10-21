import vike from 'vike/fetch'
import { Broadcast, config } from 'telefunc'
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
export class TelefuncDurableObject extends tf.TelefuncDurableObject {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, { ...env, TelefuncKV: refusingFirstPresenceWrite(env.TelefuncKV) })
  }
}
export { TodoListDurableObject } from './database/todoItems'

// Test-only: KV refuses the first write of each `refused-presence:` key's presence, as it does a second write to a key within a second.
const refusedPresences = new Set<string>()
function refusingFirstPresenceWrite(kv: KVNamespace): KVNamespace {
  return new Proxy(kv, {
    get(target, property) {
      if (property === 'put') {
        return (key: string, value: Parameters<KVNamespace['put']>[1], options?: KVNamespacePutOptions) => {
          if (key.startsWith('tfps:refused-presence') && !refusedPresences.has(key)) {
            refusedPresences.add(key)
            return Promise.reject(new Error('KV PUT failed: 429 Too Many Requests'))
          }
          return target.put(key, value, options)
        }
      }
      const value = Reflect.get(target, property)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}

// vike's docs example uses `export default vike` directly — meaning when vike is the
// worker entry's default, Cloudflare invokes vike.fetch with `(request, env, ctx)`.
// Type-wise vike/fetch is generic-typed for universal-middleware, but at runtime it
// IS a Cloudflare handler. Cast for the wrapper call.
const vikeAsCloudflareHandler = vike as unknown as ExportedHandler<Env>

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url)
    // Outside any session DO, as a cron trigger or a queue consumer publishes.
    if (url.pathname === '/__broadcast-publish-from-worker') {
      await Broadcast.publish('worker-publish', { from: 'worker' })
      return new Response(null, { status: 204 })
    }
    if (url.pathname === '/__broadcast-subscribe-from-worker') {
      try {
        Broadcast.subscribe('worker-subscribe', () => {})
        return new Response('subscribed')
      } catch (error) {
        return new Response(String(error))
      }
    }
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
