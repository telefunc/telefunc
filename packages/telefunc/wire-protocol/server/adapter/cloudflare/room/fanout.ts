export { Fanout, dispatchRoomFanout }
export type { RoomFanoutOutcome, RoomFanoutNamespace }

// One ephemeral chain per (incarnation, lane): N+1 starts after N settles, and failed handoffs do not
// poison later frames. Incarnation cleanup discards the chains; each accepted handoff runs at most once.

import type { RouteInstallation } from './routes.js'
import type { RoomSessionDeliveryRequest } from './backend.js'

type DeliveryInfo = { inc: string; laneKey: string; seq: number; timestamp: number }
type DeliverFn = (routes: RouteInstallation[], payload: Uint8Array, info: DeliveryInfo) => Promise<void>

type RoomFanoutRequest = {
  routes: RouteInstallation[]
  payload: Uint8Array
  seq: number
  timestamp: number
}

type RoomFanoutOutcome = { route: RouteInstallation; error?: string }

type RoomFanoutStub = {
  telefuncRoomDeliver(request: RoomSessionDeliveryRequest): Promise<void>
}

type RoomFanoutNamespace = {
  idFromString(id: string): unknown
  get(id: unknown): RoomFanoutStub
}

const noop = (): void => {}

class Fanout {
  readonly #deliver: DeliverFn
  readonly #incarnations = new Map<string, { active: boolean; lanes: Map<string, Promise<void>> }>()
  readonly #deliveries = new Map<string, Promise<void>>()

  constructor(deliver: DeliverFn) {
    this.#deliver = deliver
  }

  enqueue(routes: RouteInstallation[], payload: Uint8Array, info: DeliveryInfo): string {
    let incarnation = this.#incarnations.get(info.inc)
    if (!incarnation) this.#incarnations.set(info.inc, (incarnation = { active: true, lanes: new Map() }))
    const fence = incarnation
    const delivery = (fence.lanes.get(info.laneKey) ?? Promise.resolve()).then(() => {
      if (!fence.active) throw new Error('Cloudflare Room delivery cancelled before handoff')
      return this.#deliver(routes, payload, info)
    })
    fence.lanes.set(info.laneKey, delivery.then(noop, noop))
    const token = crypto.randomUUID()
    this.#deliveries.set(token, delivery)
    return token
  }

  async await(token: string): Promise<void> {
    const delivery = this.#deliveries.get(token)
    if (delivery === undefined) throw new Error('Cloudflare Room delivery has an unknown delivery token')
    try {
      await delivery
    } finally {
      this.#deliveries.delete(token)
    }
  }

  clearIncarnation(inc: string): void {
    const incarnation = this.#incarnations.get(inc)
    if (incarnation !== undefined) incarnation.active = false
    this.#incarnations.delete(inc)
  }
}

/** One call per route, straight to its session DO: a lane has one route per subscribed session DO, so at most the
 *  deployment's session shards (the sum of `scale`), well within a Durable Object invocation's subrequest limit. */
async function dispatchRoomFanout(
  namespace: RoomFanoutNamespace,
  request: RoomFanoutRequest,
): Promise<RoomFanoutOutcome[]> {
  const { payload, seq, timestamp } = request
  return Promise.all(
    request.routes.map(async (route): Promise<RoomFanoutOutcome> => {
      try {
        const stub = namespace.get(namespace.idFromString(route.sessionDoId))
        await stub.telefuncRoomDeliver({ ...route, payload, seq, timestamp })
        return { route }
      } catch (error) {
        return { route, error: errorMessage(error) }
      }
    }),
  )
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
