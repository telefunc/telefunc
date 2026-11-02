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

  // Test-only: each Room delivery a coordinator forwards, and the region of each session that takes one.
  private region: string | null = null
  override async fetch(request: Request) {
    this.region = request.headers.get('x-telefunc-broadcast-bucket') ?? this.region
    return super.fetch(request)
  }
  override async telefuncBroadcastForward(request: DeliveryRequest & { members: string[] }) {
    if (isTraced(request)) regionTrace.forwards.push({ members: request.members })
    return super.telefuncBroadcastForward(request)
  }
  override async telefuncBroadcastDeliver(request: DeliveryRequest) {
    if (isTraced(request)) regionTrace.deliveries.push({ member: this.ctx.id.toString(), region: this.region })
    return super.telefuncBroadcastDeliver(request)
  }

  // Test-only: the Room Durable Objects called while `/__room-calls` traces, by name, with the room a listing call names.
  override async telefuncDurableObjectCall(name: string, method: string, args: unknown[]) {
    const room = /^(directory|listIn|unlistIn)/.test(method) ? args[0] : null
    roomCalls?.push({ object: this.ctx.id.name ?? null, method, room })
    return super.telefuncDurableObjectCall(name, method, args)
  }
}
export { TodoListDurableObject } from './database/todoItems'

// The Durable Object's Broadcast RPCs and extension Durable Object RPCs, which its public type doesn't name.
type PresenceRequest = { key: string; bucket: string | null }
type DeliveryRequest = { routes: { key: string }[]; payload: string | Uint8Array }
type TelefuncDurableObjectWithPresence = new (
  ctx: DurableObjectState,
  env: Env,
) => InstanceType<typeof tf.TelefuncDurableObject> & {
  fetch(request: Request): Promise<Response>
  telefuncBroadcastPresence(request: PresenceRequest): Promise<void>
  telefuncBroadcastForward(request: DeliveryRequest & { members: string[] }): Promise<void>
  telefuncBroadcastDeliver(request: DeliveryRequest): Promise<void>
  telefuncDurableObjectCall(name: string, method: string, args: unknown[]): Promise<unknown>
}
const refusedPresences = new Set<string>()
let roomCalls: { object: string | null; method: string; room: unknown }[] | null = null

// Test-only: the Room deliveries of the message `/__region-trace` names, as the regions test counts them.
let tracedMessage: string | null = null
const regionTrace = {
  forwards: [] as { members: string[] }[],
  deliveries: [] as { member: string; region: string | null }[],
}
function isTraced({ routes, payload }: DeliveryRequest): boolean {
  if (tracedMessage === null || !routes.some(({ key }) => key.startsWith('__telefunc_room__:'))) return false
  return (typeof payload === 'string' ? payload : new TextDecoder().decode(payload)).includes(tracedMessage)
}
// Test-only: a page's `test-colo` cookie names the Cloudflare colo its requests come from.
function withTestColo(request: Request): Request {
  const colo = /(?:^|;\s*)test-colo=([A-Z]+)/.exec(request.headers.get('cookie') ?? '')?.[1]
  return colo ? new Request(request, { cf: { ...request.cf, colo } }) : request
}

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
    .get(namespace.idFromName('__telefunc_room__:x'))
    .telefuncDurableObjectCall('room', 'directoryGet', ['x'])
  return c.body(null, 204)
})
// Test-only: the run the backlog page asked for, as its telefunction recorded it; a POST says the page is stalled.
app.all('/backlog-run', async (c) => {
  const run = `backlog:${c.req.query('channel')}`
  if (c.req.method === 'POST') await c.env.TelefuncKV.put(`${run}:stalled`, 'true')
  return new Response(await c.env.TelefuncKV.get(run))
})
// Test-only: a PUT starts tracing the Room deliveries of `message`; a GET returns what was traced.
app.all('/__region-trace', (c) => {
  if (c.req.method === 'PUT') {
    tracedMessage = c.req.query('message')!
    regionTrace.forwards.length = 0
    regionTrace.deliveries.length = 0
  }
  return c.json(regionTrace)
})
// Test-only: a PUT starts tracing Room's Durable Object calls; a GET returns them and stops.
app.all('/__room-calls', (c) => {
  if (c.req.method === 'PUT') roomCalls = []
  const calls = roomCalls
  if (c.req.method === 'GET') roomCalls = null
  return c.json(calls)
})
// Telefunc answers its requests and WebSocket upgrades; Vike renders the rest.
app.use(async (c, next) => {
  const response = await tf.serve({ request: withTestColo(c.req.raw), env: c.env, ctx: c.executionCtx })
  if (response) return response
  await next()
})

export default { fetch: app.fetch } satisfies ExportedHandler<Env>
