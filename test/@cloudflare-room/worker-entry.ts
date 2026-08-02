/// <reference types="@cloudflare/workers-types" />
import { DurableObject } from 'cloudflare:workers'
import '../../packages/telefunc/node/server/async_hooks.js'
import {
  BACKEND_SPI_VERSION,
  type BackendDriverPair,
} from '../../packages/telefunc/wire-protocol/backend/driver-pair.js'
import { setDefaultBackend } from '../../packages/telefunc/wire-protocol/backend/install.js'
import type { RoomHead } from '../../packages/telefunc/wire-protocol/backend/room/contract.js'
import type {
  SubscriptionAttempt,
  SubscriptionAttemptState,
} from '../../packages/telefunc/wire-protocol/backend/subscription.js'
import { Room } from '../../packages/telefunc/wire-protocol/room/server.js'
import {
  CloudflareRoomBackend,
  CloudflareRoomSessionManager,
  withCloudflareRoomSessionManager,
  type CloudflareRoomNamespace,
  type RoomShardDeliveryRequest,
  type RoomShardInvalidationRequest,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/backend.js'
import {
  TelefuncRoomDurableObject as ProductionRoomDurableObject,
  createTelefuncRoomDurableObjectClass,
  type CommitWire,
  type HeadCxResult,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/do.js'
import {
  dispatchRoomShardFanout,
  ROOM_FANOUT_WIDTH,
  type RoomShardFanoutNamespace,
  type RoomShardFanoutRequest,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/fanout.js'
const publicRoomBackend = new CloudflareRoomBackend()
const publicRoomPair: BackendDriverPair = {
  spiVersion: BACKEND_SPI_VERSION,
  driver: publicRoomBackend,
  dispose: () => publicRoomBackend.dispose(),
}
setDefaultBackend(() => publicRoomPair, 'cloudflare-room-ci-public')
const PublicRoomDurableObjectBase = createTelefuncRoomDurableObjectClass('PUBLIC_SESSION')
const textEncoder = new TextEncoder()
const CONTROL_HORIZON_MS = 2_000
type Deferred = ReturnType<typeof Promise.withResolvers<void>>
export class PublicRoomSessionDurableObject extends DurableObject {
  readonly #manager: CloudflareRoomSessionManager
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env as Env)
    this.#manager = new CloudflareRoomSessionManager(
      ctx.id.toString(),
      () => (env as { PUBLIC_ROOM: CloudflareRoomNamespace }).PUBLIC_ROOM,
    )
  }
  publicRoomLifecycle(roomId: string) {
    return this.#run(async () => {
      const room = await Room.create(roomId, { meta: { purpose: 'cloudflare-room-ci' } })
      const received: unknown[] = []
      let receivedFromPublisher = false
      let publisherId = ''
      room.subscribe((data, _info, from) => {
        received.push(data)
        receivedFromPublisher = from.id === publisherId
      })
      const participant = await room.join({ meta: { name: 'public-path' } })
      publisherId = participant.id
      await participant.publish({ kind: 'public-path' })
      const joined = room.count === 1
      await Room.close(roomId)
      return {
        created: room.id === roomId,
        joined,
        publishedAndSubscribed: received,
        receivedFromPublisher,
        closed: room.isClosed,
      }
    })
  }
  telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void> {
    return this.#run(() => this.#manager.deliver(request))
  }
  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): void {
    return this.#run(() => this.#manager.invalidate(request))
  }
  telefuncRoomFanout(request: RoomShardFanoutRequest) {
    return dispatchRoomShardFanout((this.env as Env).PUBLIC_SESSION as unknown as RoomShardFanoutNamespace, request)
  }
  #run<T>(fn: () => T): T {
    return withCloudflareRoomSessionManager(() => this.#manager, fn)
  }
}
export class PublicRoomDurableObject extends PublicRoomDurableObjectBase {}
type DeliveryState = {
  blockFirst: boolean
  fail: boolean
  started: boolean
  delivered: number[]
  invalidations: Array<'recoverable' | 'terminal'>
  gate: Deferred
}
export class SessionDurableObject extends DurableObject {
  readonly #deliveries = new Map<string, DeliveryState>()
  readonly #attempts = new Map<string, SubscriptionAttempt>()
  readonly #manager: CloudflareRoomSessionManager
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env as Env)
    this.#manager = new CloudflareRoomSessionManager(
      ctx.id.toString(),
      () => (env as { ROOM: CloudflareRoomNamespace }).ROOM,
    )
  }
  prepareDelivery(roomId: string, blockFirst: boolean, fail: boolean = false): void {
    this.#deliveries.set(roomId, {
      blockFirst,
      fail,
      started: false,
      delivered: [],
      invalidations: [],
      gate: Promise.withResolvers<void>(),
    })
  }
  deliveryState(roomId: string): Pick<DeliveryState, 'started' | 'delivered' | 'invalidations'> {
    const state = this.#delivery(roomId)
    return { started: state.started, delivered: [...state.delivered], invalidations: [...state.invalidations] }
  }
  releaseDelivery(roomId: string): void {
    this.#delivery(roomId).gate.resolve()
  }
  setDeliveryFailure(roomId: string, fail: boolean): void {
    this.#delivery(roomId).fail = fail
  }
  async openSubscription(roomId: string, inc: string, waitForReady: boolean = true): Promise<void> {
    const attempt = this.#manager.openSubscription(roomId, inc, { kind: 'semantic' }, () => {})
    this.#attempts.set(roomId, attempt)
    if (waitForReady) await attempt.ready
  }
  async subscriptionReadyOutcome(roomId: string): Promise<string> {
    try {
      await this.#attempt(roomId).ready
      return 'fulfilled'
    } catch (error) {
      return error instanceof Error ? error.message : String(error)
    }
  }
  subscriptionState(roomId: string): SubscriptionAttemptState {
    return this.#attempt(roomId).state()
  }
  async telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void> {
    const state = this.#delivery(request.roomId, 'delivery reached an unprepared session')
    state.delivered.push(request.seq)
    if (state.blockFirst && request.seq === 1) {
      state.started = true
      await state.gate.promise
    }
    if (state.fail) throw new Error('delivery probe rejected')
  }
  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): void {
    const state = this.#delivery(request.roomId, 'invalidation reached an unprepared session')
    if (request.roomId.startsWith('coordinator-drop-') && (request.laneKey !== request.leaseId || !request.terminal)) {
      throw new Error('wide drop lost exact terminal installation metadata')
    }
    state.invalidations.push(request.terminal === true ? 'terminal' : 'recoverable')
    this.#manager.invalidate(request)
  }
  telefuncRoomFanout(request: RoomShardFanoutRequest) {
    return dispatchRoomShardFanout(
      (this.env as Env).TelefuncDurableObject as unknown as RoomShardFanoutNamespace,
      request,
    )
  }
  #delivery(roomId: string, error = 'delivery probe was not prepared'): DeliveryState {
    const state = this.#deliveries.get(roomId)
    if (state === undefined) throw new Error(error)
    return state
  }
  #attempt(roomId: string): SubscriptionAttempt {
    const attempt = this.#attempts.get(roomId)
    if (attempt === undefined) throw new Error('subscription probe was not prepared')
    return attempt
  }
}
type AuthorityControl =
  | 'reconstruct'
  | 'alarm'
  | `prepare-${'response-reordering' | 'registration-hold'}`
  | `${'wait' | 'release'}-${'first-commit' | 'registration'}`
  | 'release-second-delivery'
type ResponseReordering = {
  firstCommit: Deferred
  firstResponse: Deferred
  secondDelivery: Deferred
  secondToken?: string
}
type RegistrationHold = { installed: Deferred; release: Deferred }
export class TelefuncRoomDurableObject extends ProductionRoomDurableObject {
  readonly #probeEnv: unknown
  readonly #authoritySubrequestBudget: { remaining: number }
  #reconstructed: ProductionRoomDurableObject | null = null
  #responseReordering?: ResponseReordering
  #registrationHold?: RegistrationHold
  constructor(ctx: DurableObjectState, env: unknown) {
    const authoritySubrequestBudget = { remaining: Number.POSITIVE_INFINITY }
    super(ctx, {
      ...(env as object),
      TelefuncDurableObject: budgetNamespace((env as Env).TelefuncDurableObject, authoritySubrequestBudget),
    })
    this.#probeEnv = env
    this.#authoritySubrequestBudget = authoritySubrequestBudget
  }
  override commitLane(...args: Parameters<ProductionRoomDurableObject['commitLane']>) {
    if (this.#reconstructed !== null) return this.#reconstructed.commitLane(...args)
    const probe = this.#responseReordering
    if (probe === undefined) return super.commitLane(...args)
    return (async () => {
      const result = await super.commitLane(...args)
      if ('accepted' in result) {
        const payload = args[3]
        if (payload[0] === 1) {
          probe.firstCommit.resolve()
          await probe.firstResponse.promise
        } else if (payload[0] === 2) probe.secondToken = result.deliveryToken
      }
      return result
    })()
  }
  override registerRoute(...args: Parameters<ProductionRoomDurableObject['registerRoute']>) {
    const hold = this.#registrationHold
    if (hold === undefined) return super.registerRoute(...args)
    return super.registerRoute(...args).then(async (result) => {
      if ('ok' in result) {
        hold.installed.resolve()
        await hold.release.promise
      }
      return result
    })
  }
  override awaitDelivery(token: string): Promise<void> {
    const gate =
      this.#responseReordering?.secondToken === token ? this.#responseReordering.secondDelivery.promise : null
    return gate === null ? this.#awaitDelivery(token) : gate.then(() => this.#awaitDelivery(token))
  }
  #awaitDelivery(token: string): Promise<void> {
    return this.#reconstructed === null ? super.awaitDelivery(token) : this.#reconstructed.awaitDelivery(token)
  }
  async telefuncRoomWideDropForTest(roomId: string, inc: string, subscriberDoId: string): Promise<void> {
    const authority = this as unknown as Authority
    const opened = await openHead(authority, inc, 'wide drop open')
    for (let index = 0; index < 1_001; index++) {
      await this.registerRoute(roomId, inc, `lane-${index}`, subscriberDoId, `lane-${index}`)
    }
    this.#authoritySubrequestBudget.remaining = ROOM_FANOUT_WIDTH
    await closeAndDrop(authority, inc, opened, 'wide-drop-close')
    this.#authoritySubrequestBudget.remaining = Number.POSITIVE_INFINITY
  }
  async telefuncRoomControlForTest(action: AuthorityControl): Promise<number | null | void> {
    switch (action) {
      case 'reconstruct':
        this.#reconstructed = new ProductionRoomDurableObject(this.ctx, this.#probeEnv)
        return
      case 'prepare-response-reordering':
        this.#responseReordering = {
          firstCommit: Promise.withResolvers<void>(),
          firstResponse: Promise.withResolvers<void>(),
          secondDelivery: Promise.withResolvers<void>(),
        }
        return
      case 'wait-first-commit':
        return this.#responseProbe().firstCommit.promise
      case 'release-first-commit':
        return this.#responseProbe().firstResponse.resolve()
      case 'release-second-delivery':
        return this.#responseProbe().secondDelivery.resolve()
      case 'prepare-registration-hold':
        this.#registrationHold = { installed: Promise.withResolvers<void>(), release: Promise.withResolvers<void>() }
        return
      case 'wait-registration':
        return this.#registrationProbe().installed.promise
      case 'release-registration':
        return this.#registrationProbe().release.resolve()
      case 'alarm':
        return this.ctx.storage.getAlarm()
    }
  }
  #responseProbe(): ResponseReordering {
    if (this.#responseReordering === undefined) throw new Error('response reordering probe was not prepared')
    return this.#responseReordering
  }
  #registrationProbe(): RegistrationHold {
    if (this.#registrationHold === undefined) throw new Error('registration hold probe was not prepared')
    return this.#registrationHold
  }
}
type RpcMethods<T> = {
  [K in keyof T]: T[K] extends (...args: infer Args) => infer Result
    ? (...args: Args) => Promise<Awaited<Result>>
    : never
}
type Authority = RpcMethods<TelefuncRoomDurableObject>
type Session = RpcMethods<SessionDurableObject>
type PublicSession = RpcMethods<Pick<PublicRoomSessionDurableObject, 'publicRoomLifecycle'>>
type Env = {
  ROOM: DurableObjectNamespace
  TelefuncDurableObject: DurableObjectNamespace
  PUBLIC_ROOM: DurableObjectNamespace
  PUBLIC_SESSION: DurableObjectNamespace
}
function budgetNamespace(namespace: DurableObjectNamespace, budget: { remaining: number }): RoomShardFanoutNamespace {
  return {
    idFromString: (id) => namespace.idFromString(id),
    idFromName: (name) => namespace.idFromName(name),
    get(id) {
      if (budget.remaining-- <= 0) throw new Error('authority subrequest budget exceeded')
      return namespace.get(id as DurableObjectId) as never
    },
  }
}
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const suffix = crypto.randomUUID()
      if (new URL(request.url).pathname === '/large-retained') {
        return Response.json(await largeRetainedReplay(env, suffix))
      }
      const publicSession = env.PUBLIC_SESSION.get(
        env.PUBLIC_SESSION.idFromName(`public-session-${suffix}`),
      ) as unknown as PublicSession
      const sessionId = env.TelefuncDurableObject.idFromName(`session-${suffix}`)
      const session = env.TelefuncDurableObject.get(sessionId) as unknown as Session
      return Response.json({
        publicLifecycle: await publicSession.publicRoomLifecycle(`public-room-${suffix}`),
        facadeSettlementOrdering: await facadeResponseOrdering(env, suffix),
        lifecycle: await successfulLifecycle(env, sessionId, session, suffix),
        terminalDrop: await terminalGenerationDrop(env, session, suffix),
        preAckTerminalDrop: await preAckTerminalGenerationDrop(env, session, suffix),
        ...(await cancelledDelivery(env, sessionId, session, suffix)),
        fanoutOrdering: await rejectedFanoutOrdering(env, sessionId, session, suffix),
        coordinatorFanout: await coordinatorFanoutSmoke(env, sessionId, session, suffix),
        transientFailureRecovery: await transientDeliveryFailureRecovery(env, sessionId, session, suffix),
        restartSettlement: await authorityRestart(env, suffix),
        alarmPolicy: await alarmScheduling(env, sessionId, suffix),
        controlPreconditions: await unpreparedControlFailures(env, suffix),
        nativeRpc: await nativeRpcRoundTrip(env, suffix),
      })
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 })
    }
  },
}
function roomProbe(env: Env, suffix: string, name: string) {
  const roomId = `${name}-${suffix}`
  const inc = `${name}-inc-${suffix}`
  const authority = env.ROOM.get(env.ROOM.idFromName(roomId)) as unknown as Authority
  const lease = (role = 'lease') => `${name}-${role}-${suffix}`
  const open = (operation = `${name} open`) => openHead(authority, inc, operation)
  const join = async (sessionId: DurableObjectId, role = 'lease') => {
    const registration = await authority.registerRoute(roomId, inc, 'semantic', sessionId.toString(), lease(role))
    if (!('ok' in registration)) throw new Error(`route registration failed: ${registration.reason}`)
  }
  return {
    roomId,
    inc,
    authority,
    open,
    join,
    async openAndJoin(sessionId: DurableObjectId, role = 'lease') {
      const opened = await open()
      await join(sessionId, role)
      return opened
    },
    async commit(payload: string | number, operation: string) {
      const frame = typeof payload === 'string' ? textEncoder.encode(payload) : new Uint8Array([payload])
      return accepted(await authority.commitLane(roomId, inc, { kind: 'semantic' }, frame), operation)
    },
    close: (opened: RoomHead, role = 'close') => closeAndDrop(authority, inc, opened, lease(role)),
    control: (action: AuthorityControl) => authority.telefuncRoomControlForTest(action),
    settle: (commit: Extract<CommitWire, { accepted: true }>) => authority.awaitDelivery(commit.deliveryToken),
  }
}
async function successfulLifecycle(env: Env, sessionId: DurableObjectId, session: Session, suffix: string) {
  const probe = roomProbe(env, suffix, 'lifecycle')
  const opened = await probe.openAndJoin(sessionId)
  await session.prepareDelivery(probe.roomId, false)
  const commit = await probe.commit('delivered', 'lifecycle')
  await within(probe.settle(commit), 'lifecycle delivery')
  await probe.close(opened)
  return {
    receivers: commit.receivers,
    delivered: (await session.deliveryState(probe.roomId)).delivered,
    closed: (await probe.authority.readHead())?.state === 'closed',
    generations: await probe.authority.listGenerations(),
    invalidations: (await session.deliveryState(probe.roomId)).invalidations,
  }
}
async function coordinatorFanoutSmoke(env: Env, sessionId: DurableObjectId, session: Session, suffix: string) {
  const roomId = `coordinator-fanout-${suffix}`
  await session.prepareDelivery(roomId, false)
  const targets = Array.from({ length: ROOM_FANOUT_WIDTH + 1 }, (_, index) => ({
    subscriberDoId: sessionId.toString(),
    leaseId: `coordinator-lease-${index}`,
    generationToken: 'coordinator-generation',
  }))
  const outcomes = await dispatchRoomShardFanout(env.TelefuncDurableObject as unknown as RoomShardFanoutNamespace, {
    operation: 'deliver',
    roomId,
    inc: 'coordinator-inc',
    laneKey: 'semantic',
    targets,
    frame: new Uint8Array([1]),
    seq: 1,
    timestamp: 1,
    path: 'workerd',
  })
  if (outcomes.some((outcome) => outcome.error !== undefined)) {
    throw new Error('coordinator fanout did not settle every leaf successfully')
  }
  const drop = roomProbe(env, suffix, 'coordinator-drop')
  await session.prepareDelivery(drop.roomId, false)
  await drop.authority.telefuncRoomWideDropForTest(drop.roomId, drop.inc, sessionId.toString())
  return {
    outcomes: outcomes.length,
    deliveries: (await session.deliveryState(roomId)).delivered.length,
  }
}
async function facadeResponseOrdering(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'facade-order')
  const opened = await probe.open('facade ordering open')
  const manager = new CloudflareRoomSessionManager('0'.repeat(64), () => env.ROOM as unknown as CloudflareRoomNamespace)
  const result = await withCloudflareRoomSessionManager(manager, async () => {
    const commit = (value: number) =>
      publicRoomBackend.commitLane(probe.roomId, probe.inc, { kind: 'semantic' }, new Uint8Array([value]))
    await probe.control('prepare-response-reordering')
    const firstPromise = commit(1)
    await within(probe.control('wait-first-commit'), 'first facade commit acceptance')
    const second = await within(commit(2), 'second facade commit response')
    await probe.control('release-first-commit')
    const first = await within(firstPromise, 'first facade commit response')
    if (!('accepted' in first) || !('accepted' in second)) throw new Error('facade ordering commit was stale')
    const firstBeforeSecond = await Promise.race([
      first.delivery.then(() => 'settled' as const),
      new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 100)),
    ])
    const secondBeforeRelease = await Promise.race([
      second.delivery.then(() => 'settled' as const),
      new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 100)),
    ])
    if (secondBeforeRelease !== 'pending') throw new Error('second facade delivery control was not engaged')
    await probe.control('release-second-delivery')
    await Promise.all([first.delivery, second.delivery])
    return { firstSeq: first.seq, secondSeq: second.seq, firstBeforeSecond, secondBeforeRelease }
  })
  manager.dispose()
  await probe.close(opened)
  return result
}
async function terminalGenerationDrop(env: Env, session: Session, suffix: string) {
  const probe = roomProbe(env, suffix, 'terminal')
  const opened = await probe.open()
  await session.prepareDelivery(probe.roomId, false)
  await session.openSubscription(probe.roomId, probe.inc)
  await probe.close(opened)
  return {
    state: await session.subscriptionState(probe.roomId),
    invalidations: (await session.deliveryState(probe.roomId)).invalidations,
  }
}
async function preAckTerminalGenerationDrop(env: Env, session: Session, suffix: string) {
  const probe = roomProbe(env, suffix, 'pre-ack-terminal')
  const opened = await probe.open('pre-ack terminal open')
  await session.prepareDelivery(probe.roomId, false)
  await probe.control('prepare-registration-hold')
  await session.openSubscription(probe.roomId, probe.inc, false)
  await within(probe.control('wait-registration'), 'held route registration')
  if ((await session.subscriptionState(probe.roomId)) !== 'establishing') {
    throw new Error('pre-ack terminal control did not hold the subscription in establishing state')
  }
  await probe.close(opened)
  await probe.control('release-registration')
  await waitUntil(async () => (await session.subscriptionState(probe.roomId)) !== 'establishing')
  return {
    ready: await session.subscriptionReadyOutcome(probe.roomId),
    state: await session.subscriptionState(probe.roomId),
    invalidations: (await session.deliveryState(probe.roomId)).invalidations,
    generations: await probe.authority.listGenerations(),
  }
}
async function cancelledDelivery(env: Env, sessionId: DurableObjectId, session: Session, suffix: string) {
  const probe = roomProbe(env, suffix, 'cancel')
  const opened = await probe.openAndJoin(sessionId)
  await session.prepareDelivery(probe.roomId, true)
  const first = await probe.commit(1, 'first cancellation control')
  const second = await probe.commit(2, 'second cancellation control')
  await waitUntil(async () => (await session.deliveryState(probe.roomId)).started)
  await probe.close(opened)
  await session.releaseDelivery(probe.roomId)
  await within(probe.settle(first), 'first cancellation control')
  return {
    cancelled: await rejectionOf(probe.settle(second), 'cancelled delivery settlement'),
    cancellationDeliveries: (await session.deliveryState(probe.roomId)).delivered,
  }
}
async function transientDeliveryFailureRecovery(
  env: Env,
  sessionId: DurableObjectId,
  session: Session,
  suffix: string,
) {
  const probe = roomProbe(env, suffix, 'transient-failure')
  const opened = await probe.openAndJoin(sessionId)
  await session.prepareDelivery(probe.roomId, false, true)
  const settlements = await rejectedCommits(probe, 'transient failed delivery', 5)
  session.setDeliveryFailure(probe.roomId, false)
  const recoveredCommit = await probe.commit(6, 'recovered delivery')
  await within(probe.settle(recoveredCommit), 'recovered delivery')
  const deliveries = (await session.deliveryState(probe.roomId)).delivered
  await probe.close(opened)
  const invalidations = (await session.deliveryState(probe.roomId)).invalidations
  return { settlements, recovered: 'resolved', deliveries, invalidations }
}
async function rejectedFanoutOrdering(env: Env, fastSessionId: DurableObjectId, fastSession: Session, suffix: string) {
  const probe = roomProbe(env, suffix, 'fanout')
  const opened = await probe.openAndJoin(fastSessionId, 'fast')
  const slowSessionId = env.TelefuncDurableObject.idFromName(`slow-session-${suffix}`)
  const slowSession = env.TelefuncDurableObject.get(slowSessionId) as unknown as Session
  await fastSession.prepareDelivery(probe.roomId, false, true)
  await slowSession.prepareDelivery(probe.roomId, true)
  await probe.join(slowSessionId, 'slow')
  const first = await probe.commit(1, 'fanout first')
  const second = await probe.commit(2, 'fanout second')
  const firstSettlement = probe.settle(first).then(
    () => 'fulfilled' as const,
    () => 'rejected' as const,
  )
  await waitUntil(async () => (await slowSession.deliveryState(probe.roomId)).started)
  const firstSettlementBeforeRelease = await Promise.race([
    firstSettlement,
    new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 100)),
  ])
  if (firstSettlementBeforeRelease === 'fulfilled') throw new Error('failed fanout unexpectedly fulfilled')
  const fastDeliveriesBeforeRelease = (await fastSession.deliveryState(probe.roomId)).delivered
  const slowDeliveriesBeforeRelease = (await slowSession.deliveryState(probe.roomId)).delivered
  await slowSession.releaseDelivery(probe.roomId)
  const firstSettlementAfterRelease = await within(firstSettlement, 'fanout first settlement')
  const secondSettlement = await rejectionOf(probe.settle(second), 'fanout second')
  await probe.close(opened)
  return {
    firstSettlementBeforeRelease,
    firstSettlementAfterRelease,
    secondSettlement,
    fastDeliveriesBeforeRelease,
    slowDeliveriesBeforeRelease,
  }
}
async function authorityRestart(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'restart')
  await probe.open()
  const oldCommit = await probe.commit(1, 'old authority restart')
  await probe.control('reconstruct')
  const newCommit = await probe.commit(2, 'new authority restart')
  return {
    old: await rejectionOf(probe.settle(oldCommit), 'old-token settlement'),
    new: await rejectionOf(probe.settle(newCommit), 'new-token settlement'),
  }
}
async function alarmScheduling(env: Env, sessionId: DurableObjectId, suffix: string) {
  const probe = roomProbe(env, suffix, 'alarm')
  const idle = await probe.control('alarm')
  await probe.open()
  await probe.join(sessionId)
  const afterRoute = (await probe.control('alarm')) === null ? 'idle' : 'armed'
  await probe.authority.unsubscribeRoute(probe.inc, 'semantic', sessionId.toString(), `alarm-lease-${suffix}`)
  const afterUnsubscribe = await probe.control('alarm')
  return { idle, afterRoute, afterUnsubscribe }
}
async function unpreparedControlFailures(env: Env, suffix: string) {
  const authority = roomProbe(env, suffix, 'unprepared').authority
  const controls = [
    ['waitForCommit', 'wait-first-commit', 'unprepared response wait'],
    ['releaseCommit', 'release-first-commit', 'unprepared response release'],
    ['releaseDelivery', 'release-second-delivery', 'unprepared delivery release'],
    ['waitForRegistration', 'wait-registration', 'unprepared registration wait'],
    ['releaseRegistration', 'release-registration', 'unprepared registration release'],
  ] as const
  const results: Record<string, string> = {}
  for (const [key, action, label] of controls) {
    results[key] = await rejectionOf(authority.telefuncRoomControlForTest(action), label)
  }
  return results
}
async function largeRetainedReplay(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'large-retained')
  await probe.open('large retained open')
  const payload = new Uint8Array(25 * 1024 * 1024)
  payload.fill(0xa5)
  payload[0] = 0x11
  payload[payload.length - 1] = 0xee
  const lane = { kind: 'binary' as const, member: 'member', track: 'track' }
  const commit = accepted(
    await probe.authority.commitLane(probe.roomId, probe.inc, lane, payload, { retain: true }),
    'large retained',
  )
  await probe.settle(commit)
  const retained = await probe.authority.readRetained(probe.inc, lane)
  const replayed = (retained as { payload?: unknown } | null)?.payload
  if (!(replayed instanceof Uint8Array)) throw new Error('large retained replay did not return native bytes')
  return {
    bytes: replayed.byteLength,
    first: replayed[0],
    last: replayed[replayed.length - 1],
  }
}
async function nativeRpcRoundTrip(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'native-rpc')
  const config = new Uint8Array([0x11, 0x22, 0x33])
  const opened = expectHead(
    await probe.authority.compareExchangeHead(
      { expect: 'absent' },
      { head: { currentInc: probe.inc, state: 'open', config } },
    ),
    'native RPC open',
  )
  const initialCells = await probe.authority.readCells(probe.inc, { keys: ['native'] })
  if ('staleInc' in initialCells) throw new Error('native RPC cell read was stale')
  const cellResult = await probe.authority.compareExchangeCells(probe.inc, initialCells.revision, [
    { key: 'native', set: { bytes: new Uint8Array([0x44, 0x55]) } },
  ])
  if (cellResult !== 'committed') throw new Error(`native RPC cell write returned ${cellResult}`)
  const storedCells = await probe.authority.readCells(probe.inc, { keys: ['native'] })
  if ('staleInc' in storedCells) throw new Error('native RPC cell reread was stale')
  const stored = storedCells.cells.get('native')
  return {
    headConfig: [...opened.config],
    cell: stored === undefined ? null : [...stored],
    validationError: await rejectionOf(
      probe.authority.compareExchangeHead({ expect: 'absent' }, { delete: true }),
      'native RPC validation error',
    ),
  }
}
async function openHead(authority: Authority, inc: string, operation: string): Promise<RoomHead> {
  return expectHead(
    await authority.compareExchangeHead(
      { expect: 'absent' },
      { head: { currentInc: inc, state: 'open', config: textEncoder.encode('{}') } },
    ),
    operation,
  )
}
async function closeAndDrop(authority: Authority, inc: string, opened: RoomHead, leaseId: string): Promise<void> {
  const closing = expectHead(
    await authority.compareExchangeHead(
      { expect: { rev: opened.rev } },
      {
        head: {
          currentInc: inc,
          state: 'closing',
          config: opened.config,
          closeLease: { id: leaseId, durationMs: 60_000 },
        },
      },
    ),
    'enter closing',
  )
  expectHead(
    await authority.compareExchangeHead(
      { expect: { rev: closing.rev, closingLease: leaseId } },
      { head: { currentInc: null, state: 'closed', config: closing.config }, ttlMs: 60_000 },
    ),
    'finalize close',
  )
  await authority.dropGeneration(inc)
}
async function rejectedCommits(
  probe: ReturnType<typeof roomProbe>,
  label: string,
  attempts: number = 3,
): Promise<string[]> {
  const settlements: string[] = []
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const operation = `${label} ${attempt}`
    const commit = await probe.commit(attempt, operation)
    settlements.push(await rejectionOf(probe.settle(commit), operation))
  }
  return settlements
}
function expectHead(result: HeadCxResult, operation: string): RoomHead {
  if ('conflict' in result) throw new Error(`${operation} conflicted`)
  if (!('head' in result)) throw new Error(`${operation} returned no head`)
  return result.head
}
function accepted(result: CommitWire, operation: string): Extract<CommitWire, { accepted: true }> {
  if ('stale' in result) throw new Error(`${operation} commit was stale`)
  return result
}
async function waitUntil(predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + CONTROL_HORIZON_MS
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${CONTROL_HORIZON_MS}ms`)
    await new Promise<void>((resolve) => setTimeout(resolve, 10))
  }
}
async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer!: ReturnType<typeof setTimeout>
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} did not settle within ${CONTROL_HORIZON_MS}ms`)),
      CONTROL_HORIZON_MS,
    )
  })
  try {
    return await Promise.race([promise, timeout])
  } finally {
    clearTimeout(timer)
  }
}
async function rejectionOf(promise: Promise<unknown>, label: string): Promise<string> {
  try {
    await within(promise, label)
    return 'resolved'
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}
