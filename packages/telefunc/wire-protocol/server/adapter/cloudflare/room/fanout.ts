// The ordered at-most-once invariant (I5/I8) — the exact non-poisoning chain of readiness-ordering.md §3.
// One ephemeral promise chain per (incarnation, laneKey): frame N+1's attempt begins only after frame N's
// attempt SETTLED (success or failure), so a failed frame never poisons the lane and each frame's promise
// rejects only on its own failure. The chains live at the unique room authority and are discarded with
// their incarnation, so no state survives recreation and no facade-local tail can reorder a shared room.
//
// `deliver` is the handoff seam: in production the room DO RPCs each target session shard
// (`telefuncRoomDeliver`). The attempt is the backend's ONE handoff — never retried or rolled back.

import type { RouteTarget } from './routes.js'
import { getDeterministicKeyBucketIndex } from '../routing.js'
import type { RoomShardDeliveryRequest, RoomShardInvalidationRequest } from './backend.js'

type DeliveryInfo = { roomId: string; inc: string; laneKey: string; seq: number; timestamp: number }
type DeliverFn = (
  targets: RouteTarget[],
  frame: Uint8Array,
  info: DeliveryInfo,
  coordinatorIndex?: number,
) => Promise<void>

export const ROOM_FANOUT_WIDTH = 64
const ROOM_FANOUT_COORDINATOR_POOL_SIZE = 256

export type RoomShardFanoutRequest =
  | {
      operation: 'deliver'
      roomId: string
      inc: string
      laneKey: string
      targets: RouteTarget[]
      frame: Uint8Array
      seq: number
      timestamp: number
      path: string
    }
  | {
      operation: 'invalidate'
      roomId: string
      inc: string
      laneKey: string
      targets: RouteTarget[]
      terminal?: true
      path: string
    }

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
  // inc -> laneKey -> the lane's current chain tail. Nested so an incarnation's chains drop as a unit and
  // no key separator is needed.
  readonly #chains = new Map<string, Map<string, Promise<void>>>()
  readonly #incarnationFences = new Map<string, { active: boolean }>()
  readonly #attempts = new Map<string, Promise<void>>()

  constructor(deliver: DeliverFn, defer: (resume: () => void) => void = queueMicrotask) {
    this.#deliver = deliver
    this.#defer = defer
  }

  // Enqueue one frame's handoff attempt onto its lane chain. Returns a token the caller resolves later
  // via `await`, so acceptance can return before the attempt runs (no reentrant delivery inside commit).
  enqueue(inc: string, laneKey: string, targets: RouteTarget[], frame: Uint8Array, info: DeliveryInfo): string {
    let lanes = this.#chains.get(inc)
    if (lanes === undefined) {
      lanes = new Map<string, Promise<void>>()
      this.#chains.set(inc, lanes)
    }
    // Acceptance owns immutable delivery inputs. A caller may reuse or mutate its buffer as soon as
    // commit returns; no deferred attempt can observe those later writes.
    const acceptedFrame = new Uint8Array(frame)
    const acceptedTargets = targets.map((target) => ({ ...target }))
    let fence = this.#incarnationFences.get(inc)
    if (fence === undefined) {
      fence = { active: true }
      this.#incarnationFences.set(inc, fence)
    }
    const previous = lanes.get(laneKey) ?? Promise.resolve()
    const attempt = previous
      .then(() => new Promise<void>((resolve) => this.#defer(resolve)))
      .then(() => {
        if (!fence.active) throw new Error('Cloudflare Room delivery cancelled before handoff')
        return this.#fanout(acceptedTargets, acceptedFrame, info)
      })
    // Settlement gate: the next frame starts after this one settles, success OR failure.
    lanes.set(laneKey, attempt.then(noop, noop))
    // The token crosses authority reconstruction: a process-local sequence can alias a newer
    // instance's attempt and settle/delete the wrong caller's delivery.
    const token = crypto.randomUUID()
    this.#attempts.set(token, attempt)
    return token
  }

  // The caller's `delivery` promise: rejects only on this frame's own handoff failure.
  async await(token: string): Promise<void> {
    const attempt = this.#attempts.get(token)
    if (attempt === undefined) throw new Error('Cloudflare Room delivery has an unknown delivery token')
    try {
      await attempt
    } finally {
      this.#attempts.delete(token)
    }
  }

  // Incarnation-scoped teardown: a dropped/closed generation's chains never continue into a recreation.
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
    const outcomes = await Promise.allSettled(attempts)
    const failures = outcomes.filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected')
    if (failures.length === 1) throw failures[0]!.reason
    if (failures.length > 1) {
      throw new AggregateError(
        failures.map((failure) => failure.reason),
        'Cloudflare Room fanout failed',
      )
    }
  }
}

export async function dispatchRoomShardFanout(
  namespace: RoomShardFanoutNamespace,
  request: RoomShardFanoutRequest,
): Promise<RoomShardFanoutOutcome[]> {
  if (request.targets.length <= ROOM_FANOUT_WIDTH) {
    const outcomes = await Promise.allSettled(
      request.targets.map((target) => {
        const stub = namespace.get(namespace.idFromString(target.subscriberDoId))
        return request.operation === 'deliver'
          ? stub.telefuncRoomDeliver({
              roomId: request.roomId,
              inc: request.inc,
              laneKey: request.laneKey,
              subscriberDoId: target.subscriberDoId,
              leaseId: target.leaseId,
              generationToken: target.generationToken,
              frame: request.frame,
              seq: request.seq,
              timestamp: request.timestamp,
            })
          : stub.telefuncRoomInvalidate({
              roomId: request.roomId,
              inc: request.inc,
              laneKey: request.laneKey,
              subscriberDoId: target.subscriberDoId,
              leaseId: target.leaseId,
              generationToken: target.generationToken,
              ...(request.terminal === true ? { terminal: true as const } : {}),
            })
      }),
    )
    return outcomes.map((outcome, index) =>
      outcome.status === 'fulfilled'
        ? { target: request.targets[index]! }
        : { target: request.targets[index]!, error: errorMessage(outcome.reason) },
    )
  }

  const groups = groupsOfAtMost(request.targets, ROOM_FANOUT_WIDTH)
  const nested = await Promise.all(
    groups.map((targets, index) => {
      const path = `${request.path}.${index}`
      return dispatchRoomShardFanoutViaCoordinator(namespace, { ...request, targets, path })
    }),
  )
  return nested.flat()
}

export async function dispatchRoomShardFanoutViaCoordinator(
  namespace: RoomShardFanoutNamespace,
  request: RoomShardFanoutRequest,
): Promise<RoomShardFanoutOutcome[]> {
  const nameIndex = getDeterministicKeyBucketIndex(
    JSON.stringify([request.roomId, request.inc, request.laneKey, request.operation, request.path]),
    ROOM_FANOUT_COORDINATOR_POOL_SIZE,
  )
  // Separate each level's pool so a coordinator can never RPC into itself and deadlock while
  // recursively awaiting a child. Coordinators at the same level may share a stateless pool object.
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
  const groups: T[][] = []
  for (let offset = 0; offset < values.length; offset += groupSize) {
    groups.push(values.slice(offset, offset + groupSize))
  }
  return groups
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
