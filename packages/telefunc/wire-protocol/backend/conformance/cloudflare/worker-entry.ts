// Test-only workerd module. The session Durable Object below is an observation/control facet around the
// production Cloudflare Room backend and session manager. Receiver callbacks, readiness, renewal and
// reentrant seed work all execute in this isolate; Node receives only serializable commands/observations.

import { DurableObject } from 'cloudflare:workers'
import '../../../../node/server/async_hooks.js'
import { getRawContext } from '../../../../node/server/context/context.js'
import { Telefunc } from '../../../../serve/cloudflare.js'
import type { BackendSubscription, CellMutation, HeadNext, LaneId, RoomHead, SubscriptionState } from '../../spi.js'
import { superviseBackend } from '../../supervised-backend.js'
import { ServerChannel } from '../../../server/channel.js'
import { getChannelMux } from '../../../server/mux.js'
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
  requireCloudflareRoomNamespace,
  withCloudflareRoomSessionManager,
} from '../../../server/adapter/cloudflare/room/backend.js'
import {
  type HeadNextWire,
  type HeadWire,
  TelefuncRoomDurableObject as ProductionTelefuncRoomDurableObject,
} from '../../../server/adapter/cloudflare/room/do.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from '../../../server/adapter/cloudflare/room/codec.js'
import type { SubscriptionScheduler } from '../../../server/adapter/cloudflare/room/subscription.js'
import { resolveSessionRoutingTarget } from '../../../server/adapter/cloudflare/routing.js'

let controlledClock = 0
const sharedCloudflareRoomDriver = new CloudflareRoomBackend()
const sharedCloudflareRoomBackend = superviseBackend(sharedCloudflareRoomDriver)
const recoveryTelefunc = new Telefunc({
  bindingName: 'RecoverySession',
  kvBindingName: 'RECOVERY_KV',
  instanceName: 'recovery',
  roomBindingName: 'RecoveryRoom',
})

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

type ReceiverState = {
  command: ReceiverCommand
  observations: RemoteReceiverObservation[]
  released: boolean
  gate: Promise<void>
  releaseGate: () => void
  seeded: boolean
  watermark: number
  pending: RemoteReceiverObservation[]
  sequenceIndex: number
  attachments: number
  receiver: (payload: Uint8Array, info: { seq: number; timestamp: number }) => Promise<void>
}

type SubscriptionRecord = {
  sub: BackendSubscription
  receiverId: string
  roomId: string
  inc: string
  lane: LaneId
  events: SubscriptionState[]
  receiverState: ReceiverState
}

type DeliveryStatus = { state: 'pending' } | { state: 'resolved' } | { state: 'rejected'; error: string }

class ManualScheduler implements SubscriptionScheduler {
  private _now = 0
  private _sequence = 0
  private readonly _tasks = new Map<number, { at: number; task: () => Promise<void> }>()

  schedule(delayMs: number, task: () => Promise<void>): () => void {
    const id = ++this._sequence
    this._tasks.set(id, { at: this._now + delayMs, task })
    return () => this._tasks.delete(id)
  }

  async advance(ms: number): Promise<void> {
    const target = this._now + ms
    for (;;) {
      let next: { id: number; at: number; task: () => Promise<void> } | undefined
      for (const [id, entry] of this._tasks) {
        if (
          entry.at <= target &&
          (next === undefined || entry.at < next.at || (entry.at === next.at && id < next.id))
        ) {
          next = { id, ...entry }
        }
      }
      if (next === undefined) break
      this._tasks.delete(next.id)
      this._now = next.at
      await next.task()
    }
    this._now = target
  }

  clear(): void {
    this._tasks.clear()
  }
}

export class ConformanceSessionDurableObject extends DurableObject {
  private readonly _scheduler = new ManualScheduler()
  private _manager: CloudflareRoomSessionManager
  private readonly _driver = sharedCloudflareRoomDriver
  private readonly _backend = sharedCloudflareRoomBackend
  private readonly _namespace: CloudflareRoomNamespace
  private readonly _subscriptions = new Map<string, SubscriptionRecord>()
  private readonly _receiverStates = new Map<string, ReceiverState>()
  private readonly _deliveries = new Map<string, DeliveryStatus>()
  private _forcedRenewalFailures = 0
  private _forcedEstablishmentFailures = 0
  private _forcedPostCommitEstablishmentFailures = 0
  private _forcedGenerationCaptureFailures = 0
  private _forcedInvalidationFailures = 0
  private _forcedUnsubscribeFailures = 0
  private readonly _registrationLeaseHistory: string[] = []
  private readonly _probeSubscriptions = new Set<BackendSubscription>()
  private _managerDispatchDepth = 0

  constructor(ctx: DurableObjectState, env: SessionEnv) {
    super(ctx, env as never)
    const namespace = env.ROOM
    this._namespace = namespace
    this._manager = this._createManager()
  }

  async roomCommand(serialized: string): Promise<string> {
    let reply: SessionRoomReply
    try {
      const command = JSON.parse(serialized) as SessionRoomCommand
      reply = { ok: true, value: await this._executeRoomCommand(command) }
    } catch (error) {
      reply = { ok: false, error: error instanceof Error ? error.message : String(error) }
    }
    return JSON.stringify(reply)
  }

  async commitLaneB64(roomId: string, inc: string, lane: LaneId, payloadB64: string) {
    const binary = atob(payloadB64)
    const payload = Uint8Array.from(binary, (character) => character.charCodeAt(0))
    const stub = this._namespace.get(this._namespace.idFromName(roomId))
    const result = await stub.commitLane(roomId, inc, lane, payload)
    if ('error' in result) throw new Error(result.error)
    if ('stale' in result) return result
    const deliveryToken = this._trackDelivery(stub.awaitDelivery(result.deliveryToken))
    return {
      accepted: true as const,
      seq: result.seq,
      timestamp: result.timestamp,
      receivers: result.receivers,
      deliveryToken,
    }
  }

  deliveryStatus(token: string): DeliveryStatus {
    const status = this._deliveries.get(token)
    if (status === undefined) throw new Error(`unknown session delivery '${token}'`)
    if (status.state !== 'pending') this._deliveries.delete(token)
    return status
  }

  contextProbe(delayMs: number): Promise<boolean> {
    return this._run(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, delayMs))
      return getCloudflareRoomSessionManager() === this._manager
    })
  }

  async createSubscription(
    subscriptionId: string,
    receiverId: string,
    roomId: string,
    inc: string,
    lane: LaneId,
    command: ReceiverCommand,
  ): Promise<{ ready: true; state: SubscriptionState } | { ready: false; state: SubscriptionState; error: string }> {
    if (this._subscriptions.has(subscriptionId)) throw new Error(`duplicate subscription '${subscriptionId}'`)
    let receiverState = this._receiverStates.get(receiverId)
    if (receiverState !== undefined && JSON.stringify(receiverState.command) !== JSON.stringify(command)) {
      throw new Error(`receiver '${receiverId}' changed command while attached`)
    }
    if (receiverState === undefined) receiverState = this._createReceiverState(command)
    receiverState.attachments += 1
    this._receiverStates.set(receiverId, receiverState)
    const record = {
      sub: undefined as unknown as BackendSubscription,
      receiverId,
      roomId,
      inc,
      lane,
      events: [],
      receiverState,
    } satisfies SubscriptionRecord
    record.sub = this._run(() => this._backend.subscribeLane(roomId, inc, lane, receiverState.receiver))
    record.sub.onStateChange((state) => record.events.push(state))
    this._subscriptions.set(subscriptionId, record)
    try {
      await record.sub.ready
      const state = record.sub.state()
      // The Node control-plane mirror cannot observe a listener before createSubscription returns.
      // Do not replay establishment transitions that happened before that observation boundary.
      record.events.length = 0
      return { ready: true, state }
    } catch (error) {
      const state = record.sub.state()
      record.events.length = 0
      return { ready: false, state, error: (error as Error).message }
    }
  }

  subscriptionState(subscriptionId: string): { state: SubscriptionState; events: SubscriptionState[] } {
    const record = this._record(subscriptionId)
    return { state: record.sub.state(), events: record.events.splice(0) }
  }

  async unsubscribeSubscription(subscriptionId: string): Promise<string | null> {
    const record = this._record(subscriptionId)
    try {
      await this._run(() => record.sub.unsubscribe())
      return null
    } catch (error) {
      return error instanceof Error ? error.message : String(error)
    } finally {
      this._subscriptions.delete(subscriptionId)
      record.receiverState.attachments -= 1
      if (record.receiverState.attachments === 0) this._receiverStates.delete(record.receiverId)
    }
  }

  pollReceiver(subscriptionId: string, receiverId: string): RemoteReceiverObservation[] {
    const record = this._recordExact(subscriptionId, receiverId)
    return record.receiverState.observations.splice(0)
  }

  releaseReceiver(subscriptionId: string, receiverId: string): void {
    const record = this._recordExact(subscriptionId, receiverId)
    const state = record.receiverState
    if (state.command.kind !== 'stall') throw new Error('receiver is not stalled')
    if (state.released) return
    state.released = true
    state.releaseGate()
  }

  async seedReceiver(subscriptionId: string, receiverId: string): Promise<void> {
    const record = this._recordExact(subscriptionId, receiverId)
    const state = record.receiverState
    if (state.command.kind !== 'seeded') throw new Error('receiver is not a seed gate')
    const retained = await this._run(() => this._backend.readRetained(record.roomId, record.inc, record.lane))
    if (retained !== null) {
      this._emitSeeded(
        state,
        {
          payload: new TextDecoder().decode(retained.payload),
          seq: retained.seq,
          timestamp: retained.timestamp,
        },
        'seed',
      )
    }
    state.seeded = true
    for (const pending of state.pending.splice(0)) this._emitSeeded(state, pending, 'live')
  }

  forceRenewalFailures(count: number): void {
    this._forcedRenewalFailures = count
  }

  forceEstablishmentFailures(count: number): void {
    this._forcedEstablishmentFailures = count
  }

  forcePostCommitEstablishmentFailures(count: number): void {
    this._registrationLeaseHistory.length = 0
    this._forcedPostCommitEstablishmentFailures = count
  }

  registrationLeaseHistory(): string[] {
    return [...this._registrationLeaseHistory]
  }

  forceGenerationCaptureFailures(count: number): void {
    this._forcedGenerationCaptureFailures = count
  }

  forceInvalidationFailures(count: number): void {
    this._forcedInvalidationFailures = count
  }

  forceUnsubscribeFailures(count: number): void {
    this._forcedUnsubscribeFailures = count
  }

  async sharedBackendOwnershipProbe(roomId: string, inc: string, delayMs: number): Promise<string> {
    return this._run(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, delayMs))
      const sub = this._backend.subscribeLane(roomId, inc, { kind: 'semantic' }, () => {})
      this._probeSubscriptions.add(sub)
      await sub.ready
      return this.ctx.id.toString()
    })
  }

  async clearOwnershipProbes(): Promise<void> {
    const probes = [...this._probeSubscriptions]
    this._probeSubscriptions.clear()
    await Promise.all(probes.map((sub) => this._run(() => sub.unsubscribe())))
  }

  async missingBindingSubscriptionProbe(roomId: string, inc: string): Promise<string> {
    let bindingEnabled = false
    const probeManager = new CloudflareRoomSessionManager(
      this.ctx.id.toString(),
      () => (bindingEnabled ? this._namespace : requireCloudflareRoomNamespace({}, 'ROOM')),
      { scheduler: this._scheduler, now: () => controlledClock },
    )
    const source = { kind: 'durable', roomId, inc, lane: { kind: 'semantic' } } as const
    const bind = () => this._driver.subscriptions.bind(source)
    let failure = ''
    try {
      withCloudflareRoomSessionManager(probeManager, bind).open(
        () => {},
        () => 1,
      )
    } catch (error) {
      failure = error instanceof Error ? error.message : String(error)
    }
    bindingEnabled = true
    const sub = withCloudflareRoomSessionManager(probeManager, bind).open(
      () => {},
      () => 1,
    )
    await sub.ready
    await withCloudflareRoomSessionManager(probeManager, () => sub.unsubscribe())
    probeManager.dispose()
    return failure
  }

  async ambientCommitCaptureProbe(roomId: string, inc: string): Promise<string> {
    const realAuthority = this._namespace.get(this._namespace.idFromName(roomId))
    const wrongManager = new (class extends CloudflareRoomSessionManager {
      override settleDelivery(_roomId: string, _inc: string, _lane: LaneId, _attempt: Promise<void>): Promise<void> {
        return Promise.reject(new Error('commitLane re-read the ambient manager after its authority await'))
      }
    })(`${this.ctx.id.toString()}:wrong`, () => this._namespace)
    let capturedManager!: CloudflareRoomSessionManager
    const authority = {
      commitLane: async (
        targetRoomId: string,
        targetInc: string,
        lane: LaneId,
        payload: Uint8Array,
        options?: { retain?: boolean; closingLease?: string },
      ) => {
        const wire = await realAuthority.commitLane(targetRoomId, targetInc, lane, payload, options)
        const raw = getRawContext()
        const managerKey =
          raw === null ? undefined : Object.getOwnPropertySymbols(raw).find((key) => raw[key] === capturedManager)
        if (raw === null || managerKey === undefined)
          throw new Error('ambient capture probe did not find its manager context')
        raw[managerKey] = wrongManager
        return wire
      },
      awaitDelivery: (token: string) => realAuthority.awaitDelivery(token),
    } as CloudflareRoomAuthorityStub
    capturedManager = new CloudflareRoomSessionManager(`${this.ctx.id.toString()}:captured`, () => this._namespace, {
      authority: () => authority,
    })
    try {
      const result = await withCloudflareRoomSessionManager(capturedManager, () =>
        this._driver.commitLane(roomId, inc, { kind: 'semantic' }, new TextEncoder().encode('captured-manager')),
      )
      if ('stale' in result) return 'stale'
      await result.delivery
      return 'captured'
    } catch (error) {
      return error instanceof Error ? error.message : String(error)
    } finally {
      capturedManager.dispose()
      wrongManager.dispose()
    }
  }

  async resetSessionEpoch(): Promise<void> {
    this._manager.dispose()
    this._scheduler.clear()
    this._subscriptions.clear()
    this._receiverStates.clear()
    this._deliveries.clear()
    this._forcedRenewalFailures = 0
    this._forcedEstablishmentFailures = 0
    this._forcedPostCommitEstablishmentFailures = 0
    this._forcedGenerationCaptureFailures = 0
    this._forcedInvalidationFailures = 0
    this._forcedUnsubscribeFailures = 0
    this._registrationLeaseHistory.length = 0
    this._manager = this._createManager()
  }

  async advanceRenewalTimers(ms: number): Promise<void> {
    await this._run(() => this._scheduler.advance(ms))
  }

  async disposeBackend(): Promise<void> {
    this._manager.dispose()
    this._scheduler.clear()
    this._receiverStates.clear()
    this._probeSubscriptions.clear()
  }

  async telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void> {
    this._managerDispatchDepth += 1
    try {
      await this._run(() => this._manager.deliver(request))
    } finally {
      this._managerDispatchDepth -= 1
    }
  }

  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): void {
    if (this._forcedInvalidationFailures > 0) {
      this._forcedInvalidationFailures -= 1
      throw new Error('forced session invalidation failure')
    }
    this._run(() => this._manager.invalidate(request))
  }

  private _createManager(): CloudflareRoomSessionManager {
    return new CloudflareRoomSessionManager(this.ctx.id.toString(), () => this._namespace, {
      scheduler: this._scheduler,
      now: () => controlledClock,
      authority: (roomId) => this._controlledAuthority(roomId),
    })
  }

  private async _executeRoomCommand(command: SessionRoomCommand): Promise<unknown> {
    switch (command.kind) {
      case 'read-head': {
        const result = await this._run(() => this._backend.readHead(command.roomId))
        return result === null ? null : { head: headToWire(result.head) }
      }
      case 'compare-exchange-head': {
        const result = await this._run(() =>
          this._backend.compareExchangeHead(command.roomId, command.cx, nextFromWire(command.next)),
        )
        if ('conflict' in result) {
          return { conflict: true, current: result.current === null ? null : headToWire(result.current) }
        }
        return 'deleted' in result ? { ok: true, deleted: true } : { ok: true, head: headToWire(result.head) }
      }
      case 'read-cells': {
        const result = await this._run(() => this._backend.readCells(command.roomId, command.inc, command.selection))
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
        return await this._run(() =>
          this._backend.compareExchangeCells(command.roomId, command.inc, command.revision, mutations),
        )
      }
      case 'commit-lane': {
        const result = await this._run(() =>
          this._backend.commitLane(
            command.roomId,
            command.inc,
            command.lane,
            base64ToBytes(command.payloadB64),
            command.options,
          ),
        )
        if ('stale' in result) return result
        const deliveryToken = this._trackDelivery(result.delivery)
        return {
          accepted: true,
          seq: result.seq,
          timestamp: result.timestamp,
          receivers: result.receivers,
          deliveryToken,
        }
      }
      case 'read-retained': {
        const result = await this._run(() => this._backend.readRetained(command.roomId, command.inc, command.lane))
        return result === null
          ? null
          : { payloadB64: bytesToBase64(result.payload), seq: result.seq, timestamp: result.timestamp }
      }
      case 'list-retained':
        return await this._run(() => this._backend.listRetained(command.roomId, command.inc))
      case 'delete-retained':
        await this._run(() => this._backend.deleteRetained(command.roomId, command.inc, command.lane, command.options))
        return null
      case 'list-generations':
        return await this._run(() => this._backend.listGenerations(command.roomId))
      case 'drop-generation':
        await this._run(() => this._backend.dropGeneration(command.roomId, command.inc))
        return null
      case 'directory-put':
        await this._run(() => this._backend.directoryPut(command.roomId, command.incTag))
        return null
      case 'directory-delete':
        await this._run(() => this._backend.directoryDelete(command.roomId, command.incTag))
        return null
      case 'directory-list':
        return await this._run(() => this._backend.directoryList(command.prefix, command.cursor))
      case 'run-order-maintenance': {
        const stub = this._namespace.get(this._namespace.idFromName(command.roomId)) as CloudflareRoomAuthorityStub & {
          telefuncRoomRunMaintenance(): Promise<{ prunedRoutes: number }>
        }
        await stub.telefuncRoomRunMaintenance()
        return null
      }
      case 'reconstruct-order-authority': {
        const stub = this._namespace.get(this._namespace.idFromName(command.roomId)) as CloudflareRoomAuthorityStub & {
          telefuncRoomReconstructForTest(): Promise<void>
        }
        await stub.telefuncRoomReconstructForTest()
        return null
      }
      case 'seed-order-watermark': {
        const stub = this._namespace.get(this._namespace.idFromName(command.roomId)) as CloudflareRoomAuthorityStub & {
          telefuncRoomSeedOrderWatermarkForTest(
            inc: string,
            lane: LaneId,
            seq: number,
            timestamp: number,
          ): Promise<void>
        }
        await stub.telefuncRoomSeedOrderWatermarkForTest(command.inc, command.lane, command.seq, command.timestamp)
        return null
      }
    }
  }

  private _run<T>(operation: () => T): T {
    return withCloudflareRoomSessionManager(this._manager, operation)
  }

  private _trackDelivery(delivery: Promise<void>): string {
    const token = crypto.randomUUID()
    this._deliveries.set(token, { state: 'pending' })
    const observation = delivery.then(
      () => this._deliveries.set(token, { state: 'resolved' }),
      (error) =>
        this._deliveries.set(token, {
          state: 'rejected',
          error: error instanceof Error ? error.message : String(error),
        }),
    )
    this.ctx.waitUntil(observation.then(() => undefined))
    return token
  }

  private _record(subscriptionId: string): SubscriptionRecord {
    const record = this._subscriptions.get(subscriptionId)
    if (record === undefined) throw new Error(`unknown subscription '${subscriptionId}'`)
    return record
  }

  private _recordExact(subscriptionId: string, receiverId: string): SubscriptionRecord {
    const record = this._record(subscriptionId)
    if (record.receiverId !== receiverId) throw new Error('stale receiver handle')
    return record
  }

  private _emitSeeded(state: ReceiverState, frame: RemoteReceiverObservation, source: 'seed' | 'live'): void {
    if (frame.seq <= state.watermark) return
    state.watermark = frame.seq
    state.observations.push({ ...frame, source })
  }

  private _createReceiverState(command: ReceiverCommand): ReceiverState {
    let releaseGate!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })
    const state = {
      command,
      observations: [],
      released: false,
      gate,
      releaseGate,
      seeded: false,
      watermark: 0,
      pending: [],
      sequenceIndex: 0,
      attachments: 0,
      receiver: undefined as unknown as ReceiverState['receiver'],
    } satisfies ReceiverState
    state.receiver = async (payload, info) => {
      if (this._managerDispatchDepth < 1) {
        throw new Error('Cloudflare conformance receiver bypassed the production session manager')
      }
      const observation = { payload: new TextDecoder().decode(payload), seq: info.seq, timestamp: info.timestamp }
      if (command.kind === 'seeded') {
        if (state.seeded) this._emitSeeded(state, observation, 'live')
        else state.pending.push(observation)
        return
      }
      state.observations.push(observation)
      if (command.kind === 'stall' && !state.released) await state.gate
      if (command.kind === 'sequence' && command.outcomes[state.sequenceIndex++] === 'throw') {
        throw new Error(command.message)
      }
      if (command.kind === 'throw' && (command.payload === undefined || command.payload === observation.payload)) {
        throw new Error(command.message)
      }
    }
    return state
  }

  private _controlledAuthority(roomId: string): CloudflareRoomAuthorityStub {
    const stub = this._namespace.get(this._namespace.idFromName(roomId))
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
      deleteRetainedLane: (inc, lane, opts) => stub.deleteRetainedLane(inc, lane, opts),
      captureRouteGeneration: async (...args) => {
        const result = await stub.captureRouteGeneration(...args)
        if (this._forcedGenerationCaptureFailures > 0) {
          this._forcedGenerationCaptureFailures -= 1
          throw new Error('forced generation-capture transport failure')
        }
        return result
      },
      releaseRouteGenerationCapture: (attemptId) => stub.releaseRouteGenerationCapture(attemptId),
      registerRoute: async (...args) => {
        this._registrationLeaseHistory.push(args[4])
        if (this._forcedEstablishmentFailures > 0) {
          this._forcedEstablishmentFailures -= 1
          throw new Error('forced establishment transport failure')
        }
        const result = await stub.registerRoute(...args)
        if (this._forcedPostCommitEstablishmentFailures > 0) {
          this._forcedPostCommitEstablishmentFailures -= 1
          throw new Error('forced post-commit establishment acknowledgement loss')
        }
        return result
      },
      renewRoute: (...args) => {
        if (this._forcedRenewalFailures > 0) {
          this._forcedRenewalFailures -= 1
          return Promise.resolve({ ok: false })
        }
        return stub.renewRoute(...args)
      },
      unsubscribeRoute: (inc, laneKey, subscriberDoId, leaseId) => {
        if (this._forcedUnsubscribeFailures > 0) {
          this._forcedUnsubscribeFailures -= 1
          return Promise.reject(new Error('forced unsubscribe transport failure'))
        }
        return stub.unsubscribeRoute(inc, laneKey, subscriberDoId, leaseId)
      },
      listGenerations: () => stub.listGenerations(),
      dropGeneration: (inc) => stub.dropGeneration(inc),
      directoryPut: (targetRoomId, incTag) => stub.directoryPut(targetRoomId, incTag),
      directoryDelete: (targetRoomId, incTag) => stub.directoryDelete(targetRoomId, incTag),
      directoryList: (prefix, cursor) => stub.directoryList(prefix, cursor),
    }
  }
}

export class TelefuncRoomDurableObject extends ProductionTelefuncRoomDurableObject {
  private readonly _testEnv: unknown
  private _reconstructed: ProductionTelefuncRoomDurableObject | null = null

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env, 'TelefuncDurableObject', () => controlledClock)
    this._testEnv = env
  }

  override commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string },
  ) {
    return this._reconstructed === null
      ? super.commitLane(roomId, inc, lane, payload, opts)
      : this._reconstructed.commitLane(roomId, inc, lane, payload, opts)
  }

  telefuncRoomSeedOrderWatermarkForTest(inc: string, lane: LaneId, seq: number, timestamp: number): void {
    this.ctx.storage.sql.exec(
      'INSERT OR REPLACE INTO ord (inc, domain, seq, ts) VALUES (?, ?, ?, ?)',
      inc,
      laneKeyOf(lane),
      seq,
      timestamp,
    )
  }

  telefuncRoomDropRetainedChunksForTest(): void {
    this.ctx.storage.sql.exec('DROP TABLE rt_chunk')
  }

  telefuncRoomReconstructForTest(): void {
    this._reconstructed = new ProductionTelefuncRoomDurableObject(
      this.ctx,
      this._testEnv,
      'TelefuncDurableObject',
      () => controlledClock,
    )
  }
}

type RecoveryState = {
  roomId: string
  inc: string
  wants: number
  readyEpochs: number
  payloads: string[]
  errors: string[]
  sub: BackendSubscription | null
}

export class RecoveryTelefuncDurableObject extends recoveryTelefunc.TelefuncDurableObject {
  private _recovery: RecoveryState | null = null
  private _subscribingManager: CloudflareRoomSessionManager | null = null
  private _recoveryChannelId: string | null = null

  prepareRecoveryChannel(channelId: string, roomId: string, inc: string): void {
    if (this._recovery !== null) throw new Error('recovery channel already prepared')
    const state: RecoveryState = { roomId, inc, wants: 0, readyEpochs: 0, payloads: [], errors: [], sub: null }
    this._recovery = state
    this._recoveryChannelId = channelId
    const channel = new ServerChannel<unknown, unknown>({ id: channelId })
    channel.listen(async (request) => {
      if (!isBinaryWant(request)) return
      state.wants += 1
      if (state.sub?.state() === 'ready' || state.sub?.state() === 'establishing') return
      try {
        this._subscribingManager = getCloudflareRoomSessionManager()
        const sub = sharedCloudflareRoomBackend.subscribeLane(roomId, inc, { kind: 'semantic' }, (payload) => {
          state.payloads.push(new TextDecoder().decode(payload))
        })
        state.sub = sub
        await sub.ready
        state.readyEpochs += 1
      } catch (error) {
        state.errors.push(error instanceof Error ? error.message : String(error))
      }
    })
    getChannelMux().registerChannel(channel)
  }

  forceProductionRecoveryEpoch(): {
    ownedByCurrentManager: boolean
    before: SubscriptionState | 'absent'
    after: SubscriptionState | 'absent'
  } {
    const before = this._recovery?.sub?.state() ?? 'absent'
    const current = this.runWithRoomManager(() => getCloudflareRoomSessionManager())
    ;(this as unknown as { resetRoomSessionEpoch(): void }).resetRoomSessionEpoch()
    return {
      ownedByCurrentManager: current === this._subscribingManager,
      before,
      after: this._recovery?.sub?.state() ?? 'absent',
    }
  }

  recoveryObservation(): Omit<RecoveryState, 'sub'> & { state: SubscriptionState | 'absent' } {
    const recovery = this._recovery
    if (recovery === null) throw new Error('recovery channel is not prepared')
    return {
      roomId: recovery.roomId,
      inc: recovery.inc,
      wants: recovery.wants,
      readyEpochs: recovery.readyEpochs,
      payloads: [...recovery.payloads],
      errors: [...recovery.errors],
      state: recovery.sub?.state() ?? 'absent',
    }
  }

  async teardownRecoveryChannel(): Promise<void> {
    const channelId = this._recoveryChannelId
    const sub = this._recovery?.sub
    this._recovery = null
    this._recoveryChannelId = null
    this._subscribingManager = null
    if (channelId !== null) getChannelMux().unregisterChannel(channelId)
    await sub?.unsubscribe()
  }
}

export class RecoveryTelefuncRoomDurableObject extends ProductionTelefuncRoomDurableObject {
  constructor(ctx: DurableObjectState, env: unknown) {
    // This schedule drives the unmodified production manager, whose capture epochs use Date.now(). Keep
    // its authority on the same production clock; the deterministic conformance namespace stays separate.
    super(ctx, env, 'RecoverySession')
  }
}

function isBinaryWant(value: unknown): boolean {
  return (
    typeof value === 'object' && value !== null && '__r' in value && (value as { __r?: unknown }).__r === 'sub-binary'
  )
}

export default {
  async fetch(request: Request, env: Cloudflare.Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/clock/set') {
      controlledClock = Number(url.searchParams.get('v'))
      return new Response('ok')
    }
    if (url.pathname === '/clock/get') return new Response(String(controlledClock))
    if (url.pathname === '/recovery/session-name') {
      return new Response(resolveSessionRoutingTarget('recovery', undefined, request, 'weur').sessionInstanceName)
    }
    const response = await recoveryTelefunc.serve({ request, env, ctx })
    if (response !== undefined) return response
    return new Response('telefunc-room-conformance-worker')
  },
}
