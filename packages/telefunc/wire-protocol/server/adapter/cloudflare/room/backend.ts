/// <reference types="@cloudflare/workers-types" />

import { getRawContext, isAsyncMode, restoreContext } from '../../../../../node/server/context/context.js'
import type { BroadcastDriver, BroadcastLane, PublishResult } from '../../../../backend/broadcast/contract.js'
import type {
  CellMutation,
  CellSelector,
  CellsRead,
  CommitOptions,
  CommitResult,
  CxResult,
  DirectoryPage,
  HeadCx,
  HeadCxResult,
  HeadNext,
  LaneId,
  RetainedFrame,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from '../../../../backend/room/contract.js'
import type { BackendReceiver, SubscriptionBinding, SubscriptionDriver } from '../../../../backend/subscription.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { CloudflareRoomSubscriptionAttempt } from './subscription.js'
import type { TelefuncRoomDurableObject } from './do.js'
import type { RouteInstallation } from './routes.js'

const DIRECTORY_DO_NAME = '__telefunc_room_directory__'
const ROOM_MANAGER = Symbol('telefunc.cloudflare.room-manager')

export const CLOUDFLARE_ROOM_CONTEXT_ERROR =
  // spellcheck-ignore  nodejs_als is a real Cloudflare compatibility flag (AsyncLocalStorage), not a typo
  'Cloudflare Room requires await-safe context. Import "telefunc/async_hooks" and enable the Cloudflare "nodejs_als" or "nodejs_compat" compatibility flag.'
export const CLOUDFLARE_ROOM_SESSION_ERROR =
  'A Cloudflare Room subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.'

export type RoomSessionDeliveryRequest = RouteInstallation & {
  payload: Uint8Array
  seq: number
  timestamp: number
}

export type RoomSessionInvalidationRequest = RouteInstallation & { terminal?: true }

export type CloudflareRoomAuthorityStub = Omit<TelefuncRoomDurableObject, 'alarm'>

export type CloudflareRoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): CloudflareRoomAuthorityStub
}

const entryKey = (route: Pick<RouteInstallation, 'roomId' | 'inc' | 'laneKey'>) =>
  JSON.stringify([route.roomId, route.inc, route.laneKey])

function roomAuthority(namespace: CloudflareRoomNamespace, roomId: string): CloudflareRoomAuthorityStub {
  return namespace.get(namespace.idFromName(roomId))
}

export class CloudflareRoomSessionManager {
  readonly #id: string
  readonly #subscriptionPartition = crypto.randomUUID()
  readonly #entries = new Map<string, CloudflareRoomSubscriptionAttempt>()
  #disposed = false

  constructor(sessionId: string) {
    this.#id = sessionId
  }

  openSubscription(
    { roomId, inc, lane }: RoomSubscriptionSource,
    authority: CloudflareRoomAuthorityStub,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    if (this.#disposed) throw new Error('Cloudflare Room session manager is disposed')
    const source = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.#id, authority }
    const key = entryKey(source)
    const attempt: CloudflareRoomSubscriptionAttempt = new CloudflareRoomSubscriptionAttempt(source, receiver, {
      onClosed: () => {
        if (this.#entries.get(key) === attempt) this.#entries.delete(key)
      },
    })
    this.#entries.set(key, attempt)
    attempt.start()
    return attempt
  }

  async deliver(request: RoomSessionDeliveryRequest): Promise<void> {
    const entry = this.#entries.get(entryKey(request))
    if (entry?.leaseId !== request.leaseId) throw new Error('Cloudflare Room delivery lease is not installed')
    await entry.deliver(request.payload, request.seq, request.timestamp)
  }

  invalidate(request: RoomSessionInvalidationRequest): void {
    const entry = this.#entries.get(entryKey(request))
    if (entry?.leaseId === request.leaseId) {
      if (request.terminal === true) entry.terminate()
      else entry.invalidate()
    }
  }

  dispose(): void {
    if (this.#disposed) return
    this.#disposed = true
    for (const attempt of this.#entries.values()) attempt.terminate()
    this.#entries.clear()
  }

  get subscriptionPartition(): string {
    return this.#subscriptionPartition
  }

  valid(): boolean {
    return !this.#disposed
  }
}

/** Runs `fn` with a session's Room manager in context; `createManager` runs at most once per scope. */
export function withCloudflareRoomSessionManager<T>(createManager: () => CloudflareRoomSessionManager, fn: () => T): T {
  let manager: CloudflareRoomSessionManager | undefined
  return restoreContext({ [ROOM_MANAGER]: () => (manager ??= createManager()) }, fn)
}

export function materializeCloudflareRoomSessionManager(): CloudflareRoomSessionManager {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  const manager = getRawContext()?.[ROOM_MANAGER] as (() => CloudflareRoomSessionManager) | undefined
  if (manager === undefined) throw new Error(CLOUDFLARE_ROOM_SESSION_ERROR)
  return manager()
}

type CloudflareSubscriptionSource = BroadcastLane | RoomSubscriptionSource

/** Room reads and commits address the authority straight from the bindings; only a subscription needs its session, from context. */
export class CloudflareRoomBackend implements BroadcastDriver, RoomDriver {
  readonly broadcast: CloudflareBroadcastTransport
  readonly subscriptions: SubscriptionDriver<CloudflareSubscriptionSource>
  readonly #rooms: () => CloudflareRoomNamespace
  #disposed = false

  constructor({ rooms, broadcast }: { rooms: () => CloudflareRoomNamespace; broadcast: CloudflareBroadcastTransport }) {
    this.#rooms = rooms
    this.broadcast = broadcast
    this.subscriptions = {
      bind: (source) => this.#bindSubscription(source),
    }
  }

  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    return this.broadcast.publish(lane, payload)
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    return this.#stub(roomId).readHead()
  }

  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    return this.#stub(roomId).compareExchangeHead(cx, next)
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    return this.#stub(roomId).readCells(inc, sel)
  }

  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    return this.#stub(roomId).compareExchangeCells(inc, revision, mutations)
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const stub = this.#stub(roomId)
    const wire = await stub.commitLane(inc, lane, payload, opts)
    if ('stale' in wire) return wire
    const delivery = stub.awaitDelivery(wire.deliveryToken)
    return { accepted: true, seq: wire.seq, timestamp: wire.timestamp, receivers: wire.receivers, delivery }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return this.#stub(roomId).readRetained(inc, lane)
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    return this.#stub(roomId).listRetained(inc)
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this.#stub(roomId).deleteRetained(inc, lane, opts)
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    return this.#stub(roomId).dropGeneration(inc)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    return this.#directory().directoryPut(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.#directory().directoryDelete(roomId, incTag)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return this.#directory().directoryList(prefix, cursor)
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    await this.broadcast.dispose()
  }

  #bindSubscription(source: CloudflareSubscriptionSource): SubscriptionBinding {
    if (!('roomId' in source)) {
      return {
        partition: '',
        valid: () => !this.#disposed,
        open: (receiver) => this.broadcast.openSubscription(source, receiver),
      }
    }
    const manager = materializeCloudflareRoomSessionManager()
    return {
      partition: manager.subscriptionPartition,
      valid: () => manager.valid(),
      // The authority stub resolves before the manager installs any local state.
      open: (receiver) => manager.openSubscription(source, this.#stub(source.roomId), receiver),
    }
  }

  #stub(roomId: string): CloudflareRoomAuthorityStub {
    return roomAuthority(this.#rooms(), roomId)
  }

  #directory(): CloudflareRoomAuthorityStub {
    return this.#stub(DIRECTORY_DO_NAME)
  }
}
