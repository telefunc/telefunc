// One ephemeral chain per (incarnation, lane): N+1 starts after N settles, and failed handoffs do not
// poison later frames. Incarnation cleanup discards the chains; each accepted handoff runs at most once.

import type { RouteInstallation } from './routes.js'
import { getDeterministicKeyBucketIndex } from '../routing.js'
import type { RoomSessionDeliveryRequest, RoomSessionInvalidationRequest } from './backend.js'

type DeliveryInfo = { inc: string; laneKey: string; seq: number; timestamp: number }
type DeliverFn = (routes: RouteInstallation[], payload: Uint8Array, info: DeliveryInfo) => Promise<void>

export const ROOM_FANOUT_WIDTH = 64
const ROOM_FANOUT_COORDINATOR_POOL_SIZE = 256

// The recursive tree keeps four invariants: <=64 outgoing calls per node; depth-specific coordinators
// cannot self-RPC; leaf outcomes stay ordered; coordinator failure expands to every descendant.
export type RoomFanoutRequest = { routes: RouteInstallation[]; path: string } & (
  | { operation: 'deliver'; payload: Uint8Array; seq: number; timestamp: number }
  | { operation: 'invalidate'; terminal?: true }
)

export type RoomFanoutOutcome = { route: RouteInstallation; error?: string }

type RoomFanoutStub = {
  telefuncRoomDeliver(request: RoomSessionDeliveryRequest): Promise<void>
  telefuncRoomInvalidate(request: RoomSessionInvalidationRequest): Promise<void>
  telefuncRoomFanout(request: RoomFanoutRequest): Promise<RoomFanoutOutcome[]>
}

export type RoomFanoutNamespace = {
  idFromString(id: string): unknown
  idFromName(name: string): unknown
  get(id: unknown): RoomFanoutStub
}

const noop = (): void => {}

export class Fanout {
  readonly #deliver: DeliverFn
  readonly #defer: (resume: () => void) => void
  readonly #incarnations = new Map<string, { active: boolean; lanes: Map<string, Promise<void>> }>()
  readonly #deliveries = new Map<string, Promise<void>>()

  constructor(deliver: DeliverFn, defer: (resume: () => void) => void = queueMicrotask) {
    this.#deliver = deliver
    this.#defer = defer
  }

  enqueue(routes: RouteInstallation[], payload: Uint8Array, info: DeliveryInfo): string {
    let incarnation = this.#incarnations.get(info.inc)
    if (!incarnation) this.#incarnations.set(info.inc, (incarnation = { active: true, lanes: new Map() }))
    const fence = incarnation
    const delivery = (fence.lanes.get(info.laneKey) ?? Promise.resolve())
      .then(() => new Promise<void>((resolve) => this.#defer(resolve)))
      .then(() => {
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

export async function dispatchRoomFanout(
  namespace: RoomFanoutNamespace,
  request: RoomFanoutRequest,
): Promise<RoomFanoutOutcome[]> {
  if (request.routes.length <= ROOM_FANOUT_WIDTH) {
    return Promise.all(
      request.routes.map(async (route): Promise<RoomFanoutOutcome> => {
        const stub = namespace.get(namespace.idFromString(route.sessionDoId))
        try {
          if (request.operation === 'deliver') {
            const { payload, seq, timestamp } = request
            await stub.telefuncRoomDeliver({ ...route, payload, seq, timestamp })
          } else {
            await stub.telefuncRoomInvalidate({ ...route, ...(request.terminal ? { terminal: true as const } : {}) })
          }
          return { route }
        } catch (error) {
          return { route, error: errorMessage(error) }
        }
      }),
    )
  }
  const groups = partitionIntoAtMost(request.routes, ROOM_FANOUT_WIDTH)
  const outcomes = await Promise.all(
    groups.map((routes, index) => viaCoordinator(namespace, { ...request, routes, path: `${request.path}.${index}` })),
  )
  return outcomes.flat()
}

async function viaCoordinator(
  namespace: RoomFanoutNamespace,
  request: RoomFanoutRequest,
): Promise<RoomFanoutOutcome[]> {
  const first = request.routes[0]!
  const nameIndex = getDeterministicKeyBucketIndex(
    JSON.stringify([first.roomId, first.inc, first.laneKey, request.operation, request.path]),
    ROOM_FANOUT_COORDINATOR_POOL_SIZE,
  )
  // Depth-specific pools prevent recursive self-RPC; stateless peers at one depth may share objects.
  const depth = request.path.split('.').length
  const coordinator = namespace.get(namespace.idFromName(`__telefunc_room_fanout__:${depth}:${nameIndex}`))
  try {
    return await coordinator.telefuncRoomFanout(request)
  } catch (error) {
    return request.routes.map((route) => ({ route, error: errorMessage(error) }))
  }
}

function partitionIntoAtMost<T>(values: T[], maxGroups: number): T[][] {
  const groupSize = Math.ceil(values.length / maxGroups)
  return Array.from({ length: Math.ceil(values.length / groupSize) }, (_, index) =>
    values.slice(index * groupSize, (index + 1) * groupSize),
  )
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
