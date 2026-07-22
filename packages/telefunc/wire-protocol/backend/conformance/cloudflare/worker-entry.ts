// Test-only workerd module. The session Durable Object below is an observation/control facet around the
// production Cloudflare Room backend and session manager. Receiver callbacks, readiness, renewal and
// reentrant seed work all execute in this isolate; Node receives only serializable commands/observations.

import { DurableObject } from 'cloudflare:workers'
import '../../../../node/server/async_hooks.js'
import type { CellMutation, HeadNext, LaneId, LaneSubscription, ReadinessState, RoomHead } from '../../spi.js'
import type { ReceiverCommand, RemoteReceiverObservation } from '../receiver.js'
import type { SessionRoomCommand, SessionRoomReply } from './commands.js'
import {
  CloudflareRoomBackend,
  CloudflareRoomSessionManager,
  type CloudflareRoomAuthorityStub,
  type CloudflareRoomNamespace,
  type RoomShardDeliveryRequest,
  type RoomShardInvalidationRequest,
  getCloudflareRoomSessionManager,
  withCloudflareRoomSessionManager,
} from '../../../server/adapter/cloudflare/room/backend.js'
import {
  type HeadNextWire,
  type HeadWire,
  TelefuncRoomDurableObject as ProductionTelefuncRoomDurableObject,
} from '../../../server/adapter/cloudflare/room/do.js'
import { base64ToBytes, bytesToBase64 } from '../../../server/adapter/cloudflare/room/codec.js'
import type { SubscriptionScheduler } from '../../../server/adapter/cloudflare/room/subscription.js'

let controlledClock = 0

function headToWire(head: RoomHead): HeadWire {
  const wire: HeadWire = {
    rev: head.rev,
    currentInc: head.currentInc,
    state: head.state,
    configB64: bytesToBase64(head.config),
  }
  if (head.closeLease !== undefined) wire.closeLease = { ...head.closeLease }
  return wire
}

function nextFromWire(next: HeadNextWire): HeadNext {
  if ('delete' in next) return next
  const head: Extract<HeadNext, { head: unknown }>['head'] = {
    currentInc: next.head.currentInc,
    state: next.head.state,
    config: base64ToBytes(next.head.configB64),
  }
  if (next.head.closeLease !== undefined) head.closeLease = { ...next.head.closeLease }
  return next.ttlMs === undefined ? { head } : { head, ttlMs: next.ttlMs }
}

type SessionEnv = {
  ROOM: CloudflareRoomNamespace
}

type SubscriptionRecord = {
  sub: LaneSubscription
  receiverId: string
  roomId: string
  inc: string
  lane: LaneId
  command: ReceiverCommand
  observations: RemoteReceiverObservation[]
  events: ReadinessState[]
  released: boolean
  gate: Promise<void>
  releaseGate: () => void
  seeded: boolean
  watermark: number
  pending: RemoteReceiverObservation[]
  sequenceIndex: number
}

type DeliveryStatus = { state: 'pending' } | { state: 'resolved' } | { state: 'rejected'; error: string }

class ManualScheduler implements SubscriptionScheduler {
  #now = 0
  #sequence = 0
  readonly #tasks = new Map<number, { at: number; task: () => Promise<void> }>()

  schedule(delayMs: number, task: () => Promise<void>): () => void {
    const id = ++this.#sequence
    this.#tasks.set(id, { at: this.#now + delayMs, task })
    return () => this.#tasks.delete(id)
  }

  async advance(ms: number): Promise<void> {
    const target = this.#now + ms
    for (;;) {
      let next: { id: number; at: number; task: () => Promise<void> } | undefined
      for (const [id, entry] of this.#tasks) {
        if (
          entry.at <= target &&
          (next === undefined || entry.at < next.at || (entry.at === next.at && id < next.id))
        ) {
          next = { id, ...entry }
        }
      }
      if (next === undefined) break
      this.#tasks.delete(next.id)
      this.#now = next.at
      await next.task()
    }
    this.#now = target
  }

  clear(): void {
    this.#tasks.clear()
  }
}

export class ConformanceSessionDurableObject extends DurableObject {
  readonly #scheduler = new ManualScheduler()
  #manager: CloudflareRoomSessionManager
  #backend: CloudflareRoomBackend
  readonly #namespace: CloudflareRoomNamespace
  readonly #subscriptions = new Map<string, SubscriptionRecord>()
  readonly #deliveries = new Map<string, DeliveryStatus>()
  #forcedRenewalFailures = 0
  #forcedEstablishmentFailures = 0
  #forcedGenerationCaptureFailures = 0
  #forcedInvalidationFailures = 0
  #managerDispatchDepth = 0

  constructor(ctx: DurableObjectState, env: SessionEnv) {
    super(ctx, env as never)
    const namespace = env.ROOM
    this.#namespace = namespace
    this.#manager = this.#createManager()
    this.#backend = new CloudflareRoomBackend(namespace, this.#manager)
  }

  async roomCommand(serialized: string): Promise<string> {
    let reply: SessionRoomReply
    try {
      const command = JSON.parse(serialized) as SessionRoomCommand
      reply = { ok: true, value: await this.#executeRoomCommand(command) }
    } catch (error) {
      reply = { ok: false, error: error instanceof Error ? error.message : String(error) }
    }
    return JSON.stringify(reply)
  }

  async commitLaneB64(roomId: string, inc: string, lane: LaneId, payloadB64: string) {
    const binary = atob(payloadB64)
    const payload = Uint8Array.from(binary, (character) => character.charCodeAt(0))
    const stub = this.#namespace.get(this.#namespace.idFromName(roomId))
    const result = await stub.commitLane(roomId, inc, lane, payload)
    if ('error' in result) throw new Error(result.error)
    if ('stale' in result) return result
    const deliveryToken = this.#trackDelivery(stub.awaitDelivery(result.deliveryToken))
    return {
      accepted: true as const,
      seq: result.seq,
      timestamp: result.timestamp,
      receivers: result.receivers,
      deliveryToken,
    }
  }

  deliveryStatus(token: string): DeliveryStatus {
    const status = this.#deliveries.get(token)
    if (status === undefined) throw new Error(`unknown session delivery '${token}'`)
    if (status.state !== 'pending') this.#deliveries.delete(token)
    return status
  }

  contextProbe(delayMs: number): Promise<boolean> {
    return this.#run(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, delayMs))
      return getCloudflareRoomSessionManager() === this.#manager
    })
  }

  async createSubscription(
    subscriptionId: string,
    receiverId: string,
    roomId: string,
    inc: string,
    lane: LaneId,
    command: ReceiverCommand,
  ): Promise<{ ready: true; state: ReadinessState } | { ready: false; state: ReadinessState; error: string }> {
    if (this.#subscriptions.has(subscriptionId)) throw new Error(`duplicate subscription '${subscriptionId}'`)
    let releaseGate!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })
    const record = {
      sub: undefined as unknown as LaneSubscription,
      receiverId,
      roomId,
      inc,
      lane,
      command,
      observations: [],
      events: [],
      released: false,
      gate,
      releaseGate,
      seeded: false,
      watermark: 0,
      pending: [],
      sequenceIndex: 0,
    } satisfies SubscriptionRecord
    const receiver = async (payload: Uint8Array, info: { seq: number; timestamp: number }): Promise<void> => {
      if (this.#managerDispatchDepth < 1) {
        throw new Error('Cloudflare conformance receiver bypassed the production session manager')
      }
      const observation = { payload: new TextDecoder().decode(payload), seq: info.seq, timestamp: info.timestamp }
      if (command.kind === 'seeded') {
        if (record.seeded) this.#emitSeeded(record, observation, 'live')
        else record.pending.push(observation)
        return
      }
      record.observations.push(observation)
      if (command.kind === 'stall' && !record.released) await record.gate
      if (command.kind === 'sequence' && command.outcomes[record.sequenceIndex++] === 'throw') {
        throw new Error(command.message)
      }
      if (command.kind === 'throw' && (command.payload === undefined || command.payload === observation.payload)) {
        throw new Error(command.message)
      }
    }
    record.sub = this.#run(() => this.#backend.subscribeLane(roomId, inc, lane, receiver))
    record.sub.onStateChange((state) => record.events.push(state))
    this.#subscriptions.set(subscriptionId, record)
    try {
      await record.sub.ready
      return { ready: true, state: record.sub.state() }
    } catch (error) {
      return { ready: false, state: record.sub.state(), error: (error as Error).message }
    }
  }

  subscriptionState(subscriptionId: string): { state: ReadinessState; events: ReadinessState[] } {
    const record = this.#record(subscriptionId)
    return { state: record.sub.state(), events: record.events.splice(0) }
  }

  async unsubscribeSubscription(subscriptionId: string): Promise<void> {
    const record = this.#record(subscriptionId)
    await this.#run(() => record.sub.unsubscribe())
  }

  pollReceiver(subscriptionId: string, receiverId: string): RemoteReceiverObservation[] {
    const record = this.#recordExact(subscriptionId, receiverId)
    return record.observations.splice(0)
  }

  releaseReceiver(subscriptionId: string, receiverId: string): void {
    const record = this.#recordExact(subscriptionId, receiverId)
    if (record.command.kind !== 'stall') throw new Error('receiver is not stalled')
    if (record.released) return
    record.released = true
    record.releaseGate()
  }

  async seedReceiver(subscriptionId: string, receiverId: string): Promise<void> {
    const record = this.#recordExact(subscriptionId, receiverId)
    if (record.command.kind !== 'seeded') throw new Error('receiver is not a seed gate')
    const retained = await this.#run(() => this.#backend.readRetained(record.roomId, record.inc, record.lane))
    if (retained !== null) {
      this.#emitSeeded(
        record,
        {
          payload: new TextDecoder().decode(retained.payload),
          seq: retained.seq,
          timestamp: retained.timestamp,
        },
        'seed',
      )
    }
    record.seeded = true
    for (const pending of record.pending.splice(0)) this.#emitSeeded(record, pending, 'live')
  }

  forceRenewalFailures(count: number): void {
    this.#forcedRenewalFailures = count
  }

  forceEstablishmentFailures(count: number): void {
    this.#forcedEstablishmentFailures = count
  }

  forceGenerationCaptureFailures(count: number): void {
    this.#forcedGenerationCaptureFailures = count
  }

  forceInvalidationFailures(count: number): void {
    this.#forcedInvalidationFailures = count
  }

  async resetSessionEpoch(): Promise<void> {
    this.#manager.dispose()
    await this.#backend.dispose()
    this.#scheduler.clear()
    this.#subscriptions.clear()
    this.#deliveries.clear()
    this.#forcedRenewalFailures = 0
    this.#forcedEstablishmentFailures = 0
    this.#forcedGenerationCaptureFailures = 0
    this.#forcedInvalidationFailures = 0
    this.#manager = this.#createManager()
    this.#backend = new CloudflareRoomBackend(this.#namespace, this.#manager)
  }

  async advanceRenewalTimers(ms: number): Promise<void> {
    await this.#run(() => this.#scheduler.advance(ms))
  }

  async disposeBackend(): Promise<void> {
    this.#manager.dispose()
    this.#scheduler.clear()
    await this.#backend.dispose()
  }

  async telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void> {
    this.#managerDispatchDepth += 1
    try {
      await this.#run(() => this.#manager.deliver(request))
    } finally {
      this.#managerDispatchDepth -= 1
    }
  }

  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): void {
    if (this.#forcedInvalidationFailures > 0) {
      this.#forcedInvalidationFailures -= 1
      throw new Error('forced session invalidation failure')
    }
    this.#run(() => this.#manager.invalidate(request))
  }

  #createManager(): CloudflareRoomSessionManager {
    return new CloudflareRoomSessionManager(this.ctx.id.toString(), () => this.#namespace, {
      scheduler: this.#scheduler,
      now: () => controlledClock,
      jitter: () => 0.5,
      authority: (roomId) => this.#controlledAuthority(roomId),
    })
  }

  async #executeRoomCommand(command: SessionRoomCommand): Promise<unknown> {
    switch (command.kind) {
      case 'read-head': {
        const result = await this.#run(() => this.#backend.readHead(command.roomId))
        return result === null ? null : { head: headToWire(result.head) }
      }
      case 'compare-exchange-head': {
        const result = await this.#run(() =>
          this.#backend.compareExchangeHead(command.roomId, command.cx, nextFromWire(command.next)),
        )
        if ('conflict' in result) {
          return { conflict: true, current: result.current === null ? null : headToWire(result.current) }
        }
        return 'deleted' in result ? { ok: true, deleted: true } : { ok: true, head: headToWire(result.head) }
      }
      case 'read-cells': {
        const result = await this.#run(() => this.#backend.readCells(command.roomId, command.inc, command.selection))
        if ('staleInc' in result) return result
        return {
          revision: result.revision,
          cells: [...result.cells].map(([key, value]) => [key, bytesToBase64(value)]),
        }
      }
      case 'compare-exchange-cells': {
        const mutations: CellMutation[] = command.mutations.map((mutation) =>
          mutation.set === undefined
            ? { key: mutation.key }
            : {
                key: mutation.key,
                set: { bytes: base64ToBytes(mutation.set.bytesB64), ttlMs: mutation.set.ttlMs },
              },
        )
        return await this.#run(() =>
          this.#backend.compareExchangeCells(command.roomId, command.inc, command.revision, mutations),
        )
      }
      case 'commit-lane': {
        const result = await this.#run(() =>
          this.#backend.commitLane(
            command.roomId,
            command.inc,
            command.lane,
            base64ToBytes(command.payloadB64),
            command.options,
          ),
        )
        if ('stale' in result) return result
        const deliveryToken = this.#trackDelivery(result.delivery)
        return {
          accepted: true,
          seq: result.seq,
          timestamp: result.timestamp,
          receivers: result.receivers,
          deliveryToken,
        }
      }
      case 'read-retained': {
        const result = await this.#run(() => this.#backend.readRetained(command.roomId, command.inc, command.lane))
        return result === null
          ? null
          : { payloadB64: bytesToBase64(result.payload), seq: result.seq, timestamp: result.timestamp }
      }
      case 'list-retained':
        return await this.#run(() => this.#backend.listRetained(command.roomId, command.inc))
      case 'delete-retained':
        await this.#run(() => this.#backend.deleteRetained(command.roomId, command.inc, command.lane))
        return null
      case 'list-generations':
        return await this.#run(() => this.#backend.listGenerations(command.roomId))
      case 'drop-generation':
        await this.#run(() => this.#backend.dropGeneration(command.roomId, command.inc))
        return null
      case 'directory-put':
        await this.#run(() => this.#backend.directoryPut(command.roomId, command.incTag))
        return null
      case 'directory-delete':
        await this.#run(() => this.#backend.directoryDelete(command.roomId, command.incTag))
        return null
      case 'directory-list':
        return await this.#run(() => this.#backend.directoryList(command.prefix, command.cursor))
    }
  }

  #run<T>(operation: () => T): T {
    return withCloudflareRoomSessionManager(this.#manager, operation)
  }

  #trackDelivery(delivery: Promise<void>): string {
    const token = crypto.randomUUID()
    this.#deliveries.set(token, { state: 'pending' })
    const observation = delivery.then(
      () => this.#deliveries.set(token, { state: 'resolved' }),
      (error) =>
        this.#deliveries.set(token, {
          state: 'rejected',
          error: error instanceof Error ? error.message : String(error),
        }),
    )
    this.ctx.waitUntil(observation.then(() => undefined))
    return token
  }

  #record(subscriptionId: string): SubscriptionRecord {
    const record = this.#subscriptions.get(subscriptionId)
    if (record === undefined) throw new Error(`unknown subscription '${subscriptionId}'`)
    return record
  }

  #recordExact(subscriptionId: string, receiverId: string): SubscriptionRecord {
    const record = this.#record(subscriptionId)
    if (record.receiverId !== receiverId) throw new Error('stale receiver handle')
    return record
  }

  #emitSeeded(record: SubscriptionRecord, frame: RemoteReceiverObservation, source: 'seed' | 'live'): void {
    if (frame.seq <= record.watermark) return
    record.watermark = frame.seq
    record.observations.push({ ...frame, source })
  }

  #controlledAuthority(roomId: string): CloudflareRoomAuthorityStub {
    const stub = this.#namespace.get(this.#namespace.idFromName(roomId))
    return {
      readHead: () => stub.readHead(),
      compareExchangeHead: (cx, next) => stub.compareExchangeHead(cx, next),
      readCells: (inc, selection) => stub.readCells(inc, selection),
      compareExchangeCells: (inc, revision, mutations) => stub.compareExchangeCells(inc, revision, mutations),
      commitLane: (targetRoomId, inc, lane, payload, options) =>
        stub.commitLane(targetRoomId, inc, lane, payload, options),
      awaitDelivery: (token) => stub.awaitDelivery(token),
      readRetained: (inc, lane) => stub.readRetained(inc, lane),
      listRetained: (inc) => stub.listRetained(inc),
      deleteRetainedLane: (inc, lane) => stub.deleteRetainedLane(inc, lane),
      captureRouteGeneration: async (...args) => {
        const result = await stub.captureRouteGeneration(...args)
        if (this.#forcedGenerationCaptureFailures > 0) {
          this.#forcedGenerationCaptureFailures -= 1
          throw new Error('forced generation-capture transport failure')
        }
        return result
      },
      releaseRouteGenerationCapture: (attemptId) => stub.releaseRouteGenerationCapture(attemptId),
      registerRoute: async (...args) => {
        if (this.#forcedEstablishmentFailures > 0) {
          this.#forcedEstablishmentFailures -= 1
          throw new Error('forced establishment transport failure')
        }
        return stub.registerRoute(...args)
      },
      renewRoute: (...args) => {
        if (this.#forcedRenewalFailures > 0) {
          this.#forcedRenewalFailures -= 1
          return Promise.resolve({ ok: false })
        }
        return stub.renewRoute(...args)
      },
      unsubscribeRoute: (inc, laneKey, subscriberDoId, leaseId) =>
        stub.unsubscribeRoute(inc, laneKey, subscriberDoId, leaseId),
      listGenerations: () => stub.listGenerations(),
      dropGeneration: (inc) => stub.dropGeneration(inc),
      directoryPut: (targetRoomId, incTag) => stub.directoryPut(targetRoomId, incTag),
      directoryDelete: (targetRoomId, incTag) => stub.directoryDelete(targetRoomId, incTag),
      directoryList: (prefix, cursor) => stub.directoryList(prefix, cursor),
    }
  }
}

export class TelefuncRoomDurableObject extends ProductionTelefuncRoomDurableObject {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env, 'TelefuncDurableObject', () => controlledClock)
  }
}

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/clock/set') {
      controlledClock = Number(url.searchParams.get('v'))
      return new Response('ok')
    }
    if (url.pathname === '/clock/get') return new Response(String(controlledClock))
    return new Response('telefunc-room-conformance-worker')
  },
}
