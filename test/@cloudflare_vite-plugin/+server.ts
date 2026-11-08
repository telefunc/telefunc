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
    // Test-only: the key's authority refuses a `lapsed-presence:` key's refreshes for 100 s, past its record's 90 s.
    if (request.key.startsWith('lapsed-presence:') && request.bucket !== null) {
      const recordedAt = lapsedPresences.get(request.key)
      if (recordedAt === undefined) lapsedPresences.set(request.key, Date.now())
      else if (Date.now() - recordedAt < 100_000) throw new Error('Test: the authority refused the refresh')
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
    // Test-only: the first session DO a `refused-delivery:` key's message 'refused' reaches refuses it.
    const refused = request.routes.find(({ key }) => key.startsWith('refused-delivery:'))?.key
    if (refused !== undefined && payloadText(request).includes('refused') && !refusedDeliveries.has(refused)) {
      refusedDeliveries.add(refused)
      throw new Error('Test: the session refused the delivery')
    }
    return super.telefuncBroadcastDeliver(request)
  }

  // Test-only: the Room Durable Objects called while `/__room-calls` traces, by name, with the room a listing call names.
  // An inbox's registration replies slowly, so what waits for that reply comes after it in the trace.
  override async telefuncDurableObjectCall(name: string, method: string, args: unknown[]) {
    const room = name === 'room-directory' ? args[0] : null
    const object = this.ctx.id.name ?? null
    const inbox = method === 'registerRoute' && (args[0] as { laneKey: string }).laneKey.startsWith('inbox:')
    roomCalls?.push({ object, method: inbox ? 'registerRoute(inbox)' : method, room })
    const result = await super.telefuncDurableObjectCall(name, method, args)
    if (inbox && roomCalls !== null) {
      await new Promise((resolve) => setTimeout(resolve, 200))
      roomCalls?.push({ object, method: 'registerRoute(inbox) replied', room })
    }
    return result
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
  telefuncBroadcastPresence(request: PresenceRequest): Promise<boolean>
  telefuncBroadcastForward(request: DeliveryRequest & { members: string[] }): Promise<void>
  telefuncBroadcastDeliver(request: DeliveryRequest): Promise<void>
  telefuncDurableObjectCall(name: string, method: string, args: unknown[]): Promise<unknown>
}
const refusedPresences = new Set<string>()
const lapsedPresences = new Map<string, number>()
const refusedDeliveries = new Set<string>()
let roomCalls: { object: string | null; method: string; room: unknown }[] | null = null

// Test-only: the Room deliveries of the message `/__region-trace` names, as the regions test counts them.
let tracedMessage: string | null = null
const regionTrace = {
  forwards: [] as { members: string[] }[],
  deliveries: [] as { member: string; region: string | null }[],
}
function isTraced({ routes, payload }: DeliveryRequest): boolean {
  if (tracedMessage === null || !routes.some(({ key }) => key.startsWith('__telefunc_room__:'))) return false
  return payloadText({ routes, payload }).includes(tracedMessage)
}
function payloadText({ payload }: DeliveryRequest): string {
  return typeof payload === 'string' ? payload : new TextDecoder().decode(payload)
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
// Test-only: publishes `?message=` to `?key=` from the Worker, and says whether publish() rejected, with what.
app.all('/__refused-delivery-publish', async (c) => {
  const rejected = await Promise.resolve(Broadcast.publish(c.req.query('key')!, c.req.query('message')!)).then(
    () => null,
    (error: unknown) => String(error),
  )
  return c.json({ rejected })
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
// Test-only: a PUT starts tracing Room's Durable Object calls; a GET returns them, and a DELETE stops as well.
app.all('/__room-calls', (c) => {
  if (c.req.method === 'PUT') roomCalls = []
  const calls = roomCalls
  if (c.req.method === 'DELETE') roomCalls = null
  return c.json(calls)
})
// Telefunc answers its requests and WebSocket upgrades; Vike renders the rest.
app.use(async (c, next) => {
  const response = await tf.serve({ request: withTestColo(c.req.raw), env: c.env })
  if (response) return response
  await next()
})

export default { fetch: app.fetch } satisfies ExportedHandler<Env>
