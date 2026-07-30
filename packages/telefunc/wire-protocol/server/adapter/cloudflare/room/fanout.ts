// One ephemeral chain per (incarnation, lane): N+1 starts after N settles, and failed handoffs do not
// poison later frames. Incarnation cleanup discards the chains; each accepted handoff runs at most once.

import type { RouteTarget } from './routes.js'
import { getDeterministicKeyBucketIndex } from '../routing.js'
import type { RoomShardDeliveryRequest, RoomShardInvalidationRequest } from './backend.js'

type DeliveryInfo = { roomId: string; inc: string; laneKey: string; seq: number; timestamp: number }
type DeliverFn = (targets: RouteTarget[], frame: Uint8Array, info: DeliveryInfo, shard?: number) => Promise<void>

export const ROOM_FANOUT_WIDTH = 64
const ROOM_FANOUT_COORDINATOR_POOL_SIZE = 256

type FanoutRequestBase = Omit<DeliveryInfo, 'seq' | 'timestamp'> & { targets: RouteTarget[]; path: string }
export type RoomShardFanoutRequest = FanoutRequestBase &
  (
    | ({ operation: 'deliver'; frame: Uint8Array } & Pick<DeliveryInfo, 'seq' | 'timestamp'>)
    | { operation: 'invalidate'; terminal?: true }
  )

export type RoomShardFanoutOutcome = { target: RouteTarget; error?: string }

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
  readonly #chains = new Map<string, Map<string, Promise<void>>>()
  readonly #incarnationFences = new Map<string, { active: boolean }>()
  readonly #attempts = new Map<string, Promise<void>>()

  constructor(deliver: DeliverFn, defer: (resume: () => void) => void = queueMicrotask) {
    this.#deliver = deliver
    this.#defer = defer
  }

  enqueue(inc: string, laneKey: string, targets: RouteTarget[], frame: Uint8Array, info: DeliveryInfo): string {
    const lanes = this.#chains.get(inc) ?? new Map<string, Promise<void>>()
    this.#chains.set(inc, lanes)
    const acceptedFrame = new Uint8Array(frame)
    const acceptedTargets = targets.map((target) => ({ ...target }))
    const fence = this.#incarnationFences.get(inc) ?? { active: true }
    this.#incarnationFences.set(inc, fence)
    const previous = lanes.get(laneKey) ?? Promise.resolve()
    const attempt = previous
      .then(() => new Promise<void>((resolve) => this.#defer(resolve)))
      .then(() => {
        if (!fence.active) throw new Error('Cloudflare Room delivery cancelled before handoff')
        return this.#fanout(acceptedTargets, acceptedFrame, info)
      })
    lanes.set(laneKey, attempt.then(noop, noop))
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
    const fence = this.#incarnationFences.get(inc)
    if (fence !== undefined) fence.active = false
    this.#incarnationFences.delete(inc)
    this.#chains.delete(inc)
  }

  async #fanout(targets: RouteTarget[], frame: Uint8Array, info: DeliveryInfo): Promise<void> {
    const attempts =
      targets.length <= ROOM_FANOUT_WIDTH
        ? targets.map((target) => this.#deliver([target], frame, info))
        : groupsOfAtMost(targets, ROOM_FANOUT_WIDTH).map((group, index) => this.#deliver(group, frame, info, index))
    const failures = (await Promise.allSettled(attempts))
      .filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected')
      .map((failure) => failure.reason)
    if (failures.length === 1) throw failures[0]
    if (failures.length > 1) throw new AggregateError(failures, 'Cloudflare Room fanout failed')
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
        const { roomId, inc, laneKey } = request
        const route = { roomId, inc, laneKey, ...target }
        try {
          await (request.operation === 'deliver'
            ? stub.telefuncRoomDeliver({
                ...route,
                frame: request.frame,
                seq: request.seq,
                timestamp: request.timestamp,
              })
            : stub.telefuncRoomInvalidate({
                ...route,
                ...(request.terminal === true ? { terminal: true as const } : {}),
              }))
          return { target }
        } catch (error) {
          return { target, error: errorMessage(error) }
        }
      }),
    )
  }

  const groups = groupsOfAtMost(request.targets, ROOM_FANOUT_WIDTH)
  return (
    await Promise.all(
      groups.map((targets, index) => {
        const path = `${request.path}.${index}`
        return dispatchRoomShardFanoutViaCoordinator(namespace, { ...request, targets, path })
      }),
    )
  ).flat()
}

export async function dispatchRoomShardFanoutViaCoordinator(
  namespace: RoomShardFanoutNamespace,
  request: RoomShardFanoutRequest,
): Promise<RoomShardFanoutOutcome[]> {
  const nameIndex = getDeterministicKeyBucketIndex(
    JSON.stringify([request.roomId, request.inc, request.laneKey, request.operation, request.path]),
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

function groupsOfAtMost<T>(values: T[], maxGroups: number): T[][] {
  const groupSize = Math.ceil(values.length / maxGroups)
  return Array.from({ length: Math.ceil(values.length / groupSize) }, (_, index) =>
    values.slice(index * groupSize, (index + 1) * groupSize),
  )
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
