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
export class TelefuncDurableObject extends (tf.TelefuncDurableObject as TelefuncDurableObjectWithPresence) {
  // Test-only: the key's authority refuses the first presence of each `refused-presence:` key's subscriber.
  override async telefuncBroadcastPresence(request: PresenceRequest) {
    if (request.key.startsWith('refused-presence:') && request.bucket !== null && !refusedPresences.has(request.key)) {
      refusedPresences.add(request.key)
      throw new Error('Test: the authority refused the presence')
    }
    return super.telefuncBroadcastPresence(request)
  }
}
export { TodoListDurableObject } from './database/todoItems'

// The Durable Object's Broadcast presence and extension Durable Object RPCs, which its public type doesn't name.
type PresenceRequest = { key: string; bucket: string | null }
type TelefuncDurableObjectWithPresence = new (
  ctx: DurableObjectState,
  env: Env,
) => InstanceType<typeof tf.TelefuncDurableObject> & {
  telefuncBroadcastPresence(request: PresenceRequest): Promise<void>
  telefuncDurableObjectCall(name: string, method: string, args: unknown[]): Promise<unknown>
}
const refusedPresences = new Set<string>()

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
    // A Room Durable Object call before any .telefunc file loads, as a room's alarm() after the Worker restarts.
    if (url.pathname === '/__room-call-from-worker') {
      const namespace = env.TelefuncDurableObject as DurableObjectNamespace<TelefuncDurableObject>
      await namespace
        .get(namespace.idFromName('__telefunc_room_directory__'))
        .telefuncDurableObjectCall('room', 'directoryGet', ['x'])
      return new Response(null, { status: 204 })
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
