export { Fanout }
export type { RoomSessionNamespace }

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
 *  shards (the sum of `scale`), so the authority calls each itself, well within an invocation's subrequest limit. */
class Fanout {
  readonly #sessions: RoomSessionNamespace
  readonly #calls = new OrderedStubs<RoomSessionStub>()
  readonly #deliveries = new Map<string, Promise<void>>()

  constructor(sessions: RoomSessionNamespace) {
    this.#sessions = sessions
  }

  /** Hands a committed frame to every route's session DO; the returned token awaits the handoffs. */
  send(routes: RouteInstallation[], payload: Uint8Array, seq: number, timestamp: number): string {
    const handoffs = routes.map(async (route) =>
      this.#calls.call(
        route.sessionDoId,
        () => this.#sessions.get(this.#sessions.idFromString(route.sessionDoId)),
        (session) => session.telefuncRoomDeliver({ ...route, payload, seq, timestamp }),
      ),
    )
    const token = crypto.randomUUID()
    const delivery = Promise.allSettled(handoffs).then((outcomes) =>
      reportLostDeliveries('Cloudflare Room delivery', outcomes),
    )
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
}
