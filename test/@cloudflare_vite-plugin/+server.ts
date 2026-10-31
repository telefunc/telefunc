import { Hono } from 'hono'
import vike from '@vikejs/hono'
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
// `wrangler.jsonc`'s `main: "vike:server"` re-exports everything from this file,
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

const app = new Hono<{ Bindings: Env }>()
vike(app)

// Outside any session DO, as a cron trigger or a queue consumer publishes.
app.all('/__broadcast-publish-from-worker', async (c) => {
  await Broadcast.publish('worker-publish', { from: 'worker' })
  return c.body(null, 204)
})
app.all('/__broadcast-subscribe-from-worker', (c) => {
  try {
    Broadcast.subscribe('worker-subscribe', () => {})
    return c.text('subscribed')
  } catch (error) {
    return c.text(String(error))
  }
})
// A Room Durable Object call before any .telefunc file loads, as a room's alarm() after the Worker restarts.
app.all('/__room-call-from-worker', async (c) => {
  const namespace = c.env.TelefuncDurableObject as DurableObjectNamespace<TelefuncDurableObject>
  await namespace
    .get(namespace.idFromName('__telefunc_room_directory__'))
    .telefuncDurableObjectCall('room', 'directoryGet', ['x'])
  return c.body(null, 204)
})
// Test-only: the run the backlog page asked for, as its telefunction recorded it; a POST says the page is stalled.
app.all('/backlog-run', async (c) => {
  const run = `backlog:${c.req.query('channel')}`
  if (c.req.method === 'POST') await c.env.TelefuncKV.put(`${run}:stalled`, 'true')
  return new Response(await c.env.TelefuncKV.get(run))
})
// Telefunc answers its requests and WebSocket upgrades; Vike renders the rest.
app.use(async (c, next) => {
  const response = await tf.serve({ request: c.req.raw, env: c.env, ctx: c.executionCtx })
  if (response) return response
  await next()
})

export default { fetch: app.fetch } satisfies ExportedHandler<Env>
