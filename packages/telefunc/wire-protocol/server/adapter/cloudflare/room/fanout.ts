export { Fanout }
export type { RoomSessionNamespace }

import type { RouteInstallation } from './routes.js'
import type { RoomSessionDeliveryRequest } from './backend.js'
import { OrderedStubs } from '../ordered-stubs.js'

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
    this.#deliveries.set(token, Promise.allSettled(handoffs).then(reportLostDeliveries))
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

// Delivery is at-most-once: a failed target is loss, not the publisher's error; its route lapses with its lease.
function reportLostDeliveries(outcomes: PromiseSettledResult<void>[]): void {
  const failed = outcomes.filter((outcome) => outcome.status === 'rejected')
  if (failed.length > 0)
    console.error(`Cloudflare Room delivery lost to ${failed.length}/${outcomes.length} routes: ${failed[0]!.reason}`)
}
