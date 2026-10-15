export { Fanout }
export type { RoomSessionNamespace, RoomSessionStub }

import type { OrderingInfo } from '../../../../ordering-frame.js'
import { ROOM_HORIZON_MS } from '../../../../room/constants.js'
import type { RouteInstallation } from './routes.js'
import type { RoomSessionDeliveryRequest } from './subscription.js'
import { OrderedStubs, reportLostDeliveries } from '../ordered-stubs.js'

type RoomSessionStub = {
  telefuncRoomDeliver(request: RoomSessionDeliveryRequest): Promise<void>
}

type RoomSessionNamespace = {
  idFromString(id: string): unknown
  get(id: unknown): RoomSessionStub
}

/** A room authority's deliveries. Each session DO's frames go through one ordered stub, so they arrive in commit order
 *  without waiting on each other. A lane has one route per subscribed session DO, at most the deployment's session
 *  shards, so the authority calls each itself, well within an invocation's subrequest limit. */
class Fanout {
  readonly #sessions: RoomSessionNamespace
  readonly #calls = new OrderedStubs<RoomSessionStub>()
  /** A token's delivery, kept until it is awaited or two rotations later, which come a horizon apart at the earliest. */
  #deliveries = new Map<string, Promise<void>>()
  #rotated = new Map<string, Promise<void>>()
  #rotatedAt = Date.now()

  constructor(sessions: RoomSessionNamespace) {
    this.#sessions = sessions
  }

  /** Hands a committed frame to every route's session DO; the returned token awaits the handoffs. */
  send(routes: RouteInstallation[], payload: Uint8Array, info: OrderingInfo): string {
    const handoffs = routes.map(async (route) =>
      this.#calls.call(
        route.sessionDoId,
        () => this.#sessions.get(this.#sessions.idFromString(route.sessionDoId)),
        (session) => session.telefuncRoomDeliver({ ...route, payload, info }),
      ),
    )
    this.#rotate()
    const token = crypto.randomUUID()
    const delivery = Promise.allSettled(handoffs).then((outcomes) =>
      reportLostDeliveries('Cloudflare Room delivery', outcomes),
    )
    this.#deliveries.set(token, delivery)
    return token
  }

  async awaitDelivery(token: string): Promise<void> {
    this.#rotate()
    const delivery = this.#deliveries.get(token) ?? this.#rotated.get(token)
    if (delivery === undefined) throw new Error('Cloudflare Room delivery has an unknown delivery token')
    this.#deliveries.delete(token)
    this.#rotated.delete(token)
    await delivery
  }

  #rotate(): void {
    const now = Date.now()
    if (now - this.#rotatedAt < ROOM_HORIZON_MS) return
    this.#rotated = this.#deliveries
    this.#deliveries = new Map()
    this.#rotatedAt = now
  }
}
