// One ephemeral chain per (incarnation, lane): N+1 starts after N settles, and failed handoffs do not
// poison later frames. Incarnation cleanup discards the chains; each accepted handoff runs at most once.

import type { RouteInstallation } from './routes.js'
import { getDeterministicKeyBucketIndex } from '../routing.js'
import type { RoomShardDeliveryRequest, RoomShardInvalidationRequest } from './backend.js'

type DeliveryInfo = { inc: string; laneKey: string; seq: number; timestamp: number }
type DeliverFn = (targets: RouteInstallation[], frame: Uint8Array, info: DeliveryInfo) => Promise<void>

export const ROOM_FANOUT_WIDTH = 64
const ROOM_FANOUT_COORDINATOR_POOL_SIZE = 256

// The recursive tree keeps four invariants: <=64 outgoing calls per node; depth-specific coordinators
// cannot self-RPC; leaf outcomes stay ordered; coordinator failure expands to every descendant.
export type RoomShardFanoutRequest = { targets: RouteInstallation[]; path: string } & (
  | { operation: 'deliver'; frame: Uint8Array; seq: number; timestamp: number }
  | { operation: 'invalidate'; terminal?: true }
)

export type RoomShardFanoutOutcome = { target: RouteInstallation; error?: string }

type RoomShardFanoutStub = {
  telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void>
  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): Promise<void>
  telefuncRoomFanout(request: RoomShardFanoutRequest): Promise<RoomShardFanoutOutcome[]>
}

export type RoomShardFanoutNamespace = {
  idFromString(id: string): unknown
  idFromName(name: string): unknown
  get(id: unknown): RoomShardFanoutStub
}

const noop = (): void => {}

export class Fanout {
  readonly #deliver: DeliverFn
  readonly #defer: (resume: () => void) => void
  readonly #incarnations = new Map<string, { active: boolean; lanes: Map<string, Promise<void>> }>()
  readonly #attempts = new Map<string, Promise<void>>()

  constructor(deliver: DeliverFn, defer: (resume: () => void) => void = queueMicrotask) {
    this.#deliver = deliver
    this.#defer = defer
  }

  enqueue(targets: RouteInstallation[], frame: Uint8Array, info: DeliveryInfo): string {
    let incarnation = this.#incarnations.get(info.inc)
    if (!incarnation) this.#incarnations.set(info.inc, (incarnation = { active: true, lanes: new Map() }))
    const fence = incarnation
    const attempt = (fence.lanes.get(info.laneKey) ?? Promise.resolve())
      .then(() => new Promise<void>((resolve) => this.#defer(resolve)))
      .then(() => {
        if (!fence.active) throw new Error('Cloudflare Room delivery cancelled before handoff')
        return this.#deliver(targets, frame, info)
      })
    fence.lanes.set(info.laneKey, attempt.then(noop, noop))
    const token = crypto.randomUUID()
    this.#attempts.set(token, attempt)
    return token
  }

  async await(token: string): Promise<void> {
    const attempt = this.#attempts.get(token)
    if (attempt === undefined) throw new Error('Cloudflare Room delivery has an unknown delivery token')
    try {
      await attempt
    } finally {
      this.#attempts.delete(token)
    }
  }

  clearIncarnation(inc: string): void {
    const incarnation = this.#incarnations.get(inc)
    if (incarnation !== undefined) incarnation.active = false
    this.#incarnations.delete(inc)
  }
}

export async function dispatchRoomShardFanout(
  namespace: RoomShardFanoutNamespace,
  request: RoomShardFanoutRequest,
): Promise<RoomShardFanoutOutcome[]> {
  if (request.targets.length <= ROOM_FANOUT_WIDTH) {
    return Promise.all(
      request.targets.map(async (target): Promise<RoomShardFanoutOutcome> => {
        const stub = namespace.get(namespace.idFromString(target.subscriberDoId))
        try {
          if (request.operation === 'deliver') {
            const { frame, seq, timestamp } = request
            await stub.telefuncRoomDeliver({ ...target, frame, seq, timestamp })
          } else {
            await stub.telefuncRoomInvalidate({ ...target, ...(request.terminal ? { terminal: true as const } : {}) })
          }
          return { target }
        } catch (error) {
          return { target, error: errorMessage(error) }
        }
      }),
    )
  }
  const groups = partitionIntoAtMost(request.targets, ROOM_FANOUT_WIDTH)
  const outcomes = await Promise.all(
    groups.map((targets, index) =>
      viaCoordinator(namespace, { ...request, targets, path: `${request.path}.${index}` }),
    ),
  )
  return outcomes.flat()
}

async function viaCoordinator(
  namespace: RoomShardFanoutNamespace,
  request: RoomShardFanoutRequest,
): Promise<RoomShardFanoutOutcome[]> {
  const first = request.targets[0]!
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
    return request.targets.map((target) => ({ target, error: errorMessage(error) }))
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
