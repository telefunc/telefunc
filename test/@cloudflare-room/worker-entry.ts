/// <reference types="@cloudflare/workers-types" />

import { DurableObject } from 'cloudflare:workers'
import '../../packages/telefunc/node/server/async_hooks.js'
import { setDefaultBackend } from '../../packages/telefunc/wire-protocol/backend/install.js'
import { Room } from '../../packages/telefunc/wire-protocol/room/server.js'
import type {
  LaneId,
  RoomHead,
  SubscriptionAttempt,
  SubscriptionAttemptState,
} from '../../packages/telefunc/wire-protocol/backend/spi.js'
import {
  CloudflareRoomBackend,
  CloudflareRoomSessionManager,
  withCloudflareRoomSessionManager,
  type CloudflareRoomAuthorityStub,
  type CloudflareRoomNamespace,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/backend.js'
import {
  TelefuncRoomDurableObject as ProductionRoomDurableObject,
  createTelefuncRoomDurableObjectClass,
  type CommitWire,
  type HeadCxResult,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/do.js'

const publicRoomBackend = new CloudflareRoomBackend()
setDefaultBackend(() => publicRoomBackend, 'cloudflare-room-ci-public')
const PublicRoomDurableObjectBase = createTelefuncRoomDurableObjectClass('PUBLIC_SESSION')

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
    return withCloudflareRoomSessionManager(
      () => this.#manager,
      async () => {
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
      },
    )
  }

  telefuncRoomDeliver(request: DeliveryRequest): Promise<void> {
    return withCloudflareRoomSessionManager(
      () => this.#manager,
      () => this.#manager.deliver(request),
    )
  }

  telefuncRoomInvalidate(request: InvalidationRequest): void {
    return withCloudflareRoomSessionManager(
      () => this.#manager,
      () => this.#manager.invalidate(request),
    )
  }
}

export class PublicRoomDurableObject extends PublicRoomDurableObjectBase {}

type DeliveryRequest = {
  roomId: string
  inc: string
  laneKey: string
  subscriberDoId: string
  leaseId: string
  generationToken: string
  frame: Uint8Array
  seq: number
  timestamp: number
}
type InvalidationRequest = Omit<DeliveryRequest, 'frame' | 'seq' | 'timestamp'> & { terminal?: true }
type DeliveryState = {
  blockFirst: boolean
  fail: boolean
  started: boolean
  delivered: number[]
  invalidations: Array<'recoverable' | 'terminal'>
  gate: Promise<void>
  release: () => void
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
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    this.#deliveries.set(roomId, {
      blockFirst,
      fail,
      started: false,
      delivered: [],
      invalidations: [],
      gate,
      release,
    })
  }

  deliveryState(roomId: string): {
    started: boolean
    delivered: number[]
    invalidations: Array<'recoverable' | 'terminal'>
  } {
    const state = this.#deliveries.get(roomId)
    if (state === undefined) throw new Error('delivery probe was not prepared')
    return { started: state.started, delivered: [...state.delivered], invalidations: [...state.invalidations] }
  }

  releaseDelivery(roomId: string): void {
    const state = this.#deliveries.get(roomId)
    if (state === undefined) throw new Error('delivery probe was not prepared')
    state.release()
  }

  async openSubscription(roomId: string, inc: string, waitForReady: boolean = true): Promise<void> {
    const attempt = this.#manager.openSubscription(roomId, inc, { kind: 'semantic' }, () => {})
    this.#attempts.set(roomId, attempt)
    if (waitForReady) await attempt.ready
  }

  async subscriptionReadyOutcome(roomId: string): Promise<string> {
    const attempt = this.#attempts.get(roomId)
    if (attempt === undefined) throw new Error('subscription probe was not prepared')
    try {
      await attempt.ready
      return 'fulfilled'
    } catch (error) {
      return error instanceof Error ? error.message : String(error)
    }
  }

  subscriptionState(roomId: string): SubscriptionAttemptState {
    const attempt = this.#attempts.get(roomId)
    if (attempt === undefined) throw new Error('subscription probe was not prepared')
    return attempt.state()
  }

  async telefuncRoomDeliver(request: DeliveryRequest): Promise<void> {
    const state = this.#deliveries.get(request.roomId)
    if (state === undefined) throw new Error('delivery reached an unprepared session')
    state.delivered.push(request.seq)
    if (state.blockFirst && request.seq === 1) {
      state.started = true
      await state.gate
    }
    if (state.fail) throw new Error('delivery probe rejected')
  }

  telefuncRoomInvalidate(request: InvalidationRequest): void {
    const state = this.#deliveries.get(request.roomId)
    if (state === undefined) throw new Error('invalidation reached an unprepared session')
    state.invalidations.push(request.terminal === true ? 'terminal' : 'recoverable')
    this.#manager.invalidate(request)
  }
}

export class TelefuncRoomDurableObject extends ProductionRoomDurableObject {
  readonly #probeEnv: unknown
  #reconstructed: ProductionRoomDurableObject | null = null
  #responseReordering:
    | {
        firstCommit: ReturnType<typeof deferred>
        firstResponse: ReturnType<typeof deferred>
        secondDelivery: ReturnType<typeof deferred>
        secondToken?: string
      }
    | undefined
  #registrationHold: { installed: ReturnType<typeof deferred>; release: ReturnType<typeof deferred> } | undefined

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    this.#probeEnv = env
  }

  override commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitWire> {
    if (this.#reconstructed !== null) return this.#reconstructed.commitLane(roomId, inc, lane, payload, opts)
    const probe = this.#responseReordering
    if (probe === undefined) return super.commitLane(roomId, inc, lane, payload, opts)
    return (async () => {
      const result = await super.commitLane(roomId, inc, lane, payload, opts)
      if ('accepted' in result) {
        if (payload[0] === 1) {
          probe.firstCommit.resolve()
          await probe.firstResponse.promise
        } else if (payload[0] === 2) probe.secondToken = result.deliveryToken
      }
      return result
    })()
  }

  override registerRoute(roomId: string, inc: string, laneKey: string, subscriberDoId: string, leaseId: string) {
    const hold = this.#registrationHold
    if (hold === undefined) return super.registerRoute(roomId, inc, laneKey, subscriberDoId, leaseId)
    return super.registerRoute(roomId, inc, laneKey, subscriberDoId, leaseId).then(async (result) => {
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

  telefuncRoomReconstructForTest(): void {
    this.#reconstructed = new ProductionRoomDurableObject(this.ctx, this.#probeEnv)
  }

  telefuncRoomPrepareResponseReorderingForTest(): void {
    this.#responseReordering = {
      firstCommit: deferred(),
      firstResponse: deferred(),
      secondDelivery: deferred(),
    }
  }

  async telefuncRoomWaitForFirstCommitForTest(): Promise<void> {
    const probe = this.#responseReordering
    if (probe === undefined) throw new Error('response reordering probe was not prepared')
    await probe.firstCommit.promise
  }

  telefuncRoomReleaseFirstCommitForTest(): void {
    const probe = this.#responseReordering
    if (probe === undefined) throw new Error('response reordering probe was not prepared')
    probe.firstResponse.resolve()
  }

  telefuncRoomReleaseSecondDeliveryForTest(): void {
    const probe = this.#responseReordering
    if (probe === undefined) throw new Error('response reordering probe was not prepared')
    probe.secondDelivery.resolve()
  }

  telefuncRoomPrepareRegistrationHoldForTest(): void {
    this.#registrationHold = { installed: deferred(), release: deferred() }
  }

  async telefuncRoomWaitForRegistrationForTest(): Promise<void> {
    const hold = this.#registrationHold
    if (hold === undefined) throw new Error('registration hold probe was not prepared')
    await hold.installed.promise
  }

  telefuncRoomReleaseRegistrationForTest(): void {
    const hold = this.#registrationHold
    if (hold === undefined) throw new Error('registration hold probe was not prepared')
    hold.release.resolve()
  }

  telefuncRoomAlarmForTest(): Promise<number | null> {
    return this.ctx.storage.getAlarm()
  }
}

type Authority = Pick<
  CloudflareRoomAuthorityStub,
  | 'readHead'
  | 'readCells'
  | 'compareExchangeHead'
  | 'compareExchangeCells'
  | 'registerRoute'
  | 'commitLane'
  | 'awaitDelivery'
  | 'readRetained'
  | 'unsubscribeRoute'
  | 'dropGeneration'
  | 'listGenerations'
> & {
  telefuncRoomReconstructForTest(): Promise<void>
  telefuncRoomPrepareResponseReorderingForTest(): Promise<void>
  telefuncRoomWaitForFirstCommitForTest(): Promise<void>
  telefuncRoomReleaseFirstCommitForTest(): Promise<void>
  telefuncRoomReleaseSecondDeliveryForTest(): Promise<void>
  telefuncRoomPrepareRegistrationHoldForTest(): Promise<void>
  telefuncRoomWaitForRegistrationForTest(): Promise<void>
  telefuncRoomReleaseRegistrationForTest(): Promise<void>
  telefuncRoomAlarmForTest(): Promise<number | null>
}
type Session = {
  prepareDelivery(roomId: string, blockFirst: boolean, fail?: boolean): Promise<void>
  deliveryState(
    roomId: string,
  ): Promise<{ started: boolean; delivered: number[]; invalidations: Array<'recoverable' | 'terminal'> }>
  releaseDelivery(roomId: string): Promise<void>
  openSubscription(roomId: string, inc: string, waitForReady?: boolean): Promise<void>
  subscriptionReadyOutcome(roomId: string): Promise<string>
  subscriptionState(roomId: string): Promise<SubscriptionAttemptState>
}
type PublicSession = {
  publicRoomLifecycle(roomId: string): Promise<{
    created: boolean
    joined: boolean
    publishedAndSubscribed: unknown[]
    receivedFromPublisher: boolean
    closed: boolean
  }>
}
type Env = {
  ROOM: DurableObjectNamespace
  TelefuncDurableObject: DurableObjectNamespace
  PUBLIC_ROOM: DurableObjectNamespace
  PUBLIC_SESSION: DurableObjectNamespace
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
      const publicLifecycle = await publicSession.publicRoomLifecycle(`public-room-${suffix}`)
      const facadeSettlementOrdering = await facadeResponseOrdering(env, suffix)
      const sessionId = env.TelefuncDurableObject.idFromName(`session-${suffix}`)
      const session = env.TelefuncDurableObject.get(sessionId) as unknown as Session
      const lifecycle = await successfulLifecycle(env, sessionId, session, suffix)
      const terminalDrop = await terminalGenerationDrop(env, session, suffix)
      const preAckTerminalDrop = await preAckTerminalGenerationDrop(env, session, suffix)
      const preAckRecoverableDrop = await preAckRecoverableRouteDrop(env, session, suffix)
      const cancellation = await cancelledDelivery(env, sessionId, session, suffix)
      const fanoutOrdering = await rejectedFanoutOrdering(env, sessionId, session, suffix)
      const evictionInvalidations = await failedDeliveryEviction(env, sessionId, session, suffix)
      const restartSettlement = await authorityRestart(env, suffix)
      const alarmPolicy = await alarmScheduling(env, sessionId, suffix)
      const controlPreconditions = await unpreparedControlFailures(env, suffix)
      const nativeRpc = await nativeRpcRoundTrip(env, suffix)
      return Response.json({
        publicLifecycle,
        facadeSettlementOrdering,
        lifecycle,
        terminalDrop,
        preAckTerminalDrop,
        preAckRecoverableDrop,
        ...cancellation,
        fanoutOrdering,
        evictionInvalidations,
        restartSettlement,
        alarmPolicy,
        controlPreconditions,
        nativeRpc,
      })
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 })
    }
  },
}

async function successfulLifecycle(env: Env, sessionId: DurableObjectId, session: Session, suffix: string) {
  const roomId = `lifecycle-${suffix}`
  const inc = `lifecycle-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openAndJoin(authority, sessionId, roomId, inc, `lifecycle-lease-${suffix}`)
  await session.prepareDelivery(roomId, false)
  const commit = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new TextEncoder().encode('delivered')),
    'lifecycle',
  )
  await within(authority.awaitDelivery(commit.deliveryToken), 2_000, 'lifecycle delivery')
  await closeAndDrop(authority, inc, opened, `lifecycle-close-${suffix}`)
  return {
    receivers: commit.receivers,
    delivered: (await session.deliveryState(roomId)).delivered,
    closed: (await authority.readHead())?.state === 'closed',
    generations: await authority.listGenerations(),
    invalidations: (await session.deliveryState(roomId)).invalidations,
  }
}

async function facadeResponseOrdering(env: Env, suffix: string) {
  const roomId = `facade-order-${suffix}`
  const inc = `facade-order-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openHead(authority, inc, 'facade ordering open')
  const manager = new CloudflareRoomSessionManager('0'.repeat(64), () => env.ROOM as unknown as CloudflareRoomNamespace)
  const result = await withCloudflareRoomSessionManager(manager, async () => {
    await authority.telefuncRoomPrepareResponseReorderingForTest()
    const firstPromise = publicRoomBackend.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([1]))
    await within(authority.telefuncRoomWaitForFirstCommitForTest(), 2_000, 'first facade commit acceptance')
    const second = await within(
      publicRoomBackend.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([2])),
      2_000,
      'second facade commit response',
    )
    await authority.telefuncRoomReleaseFirstCommitForTest()
    const first = await within(firstPromise, 2_000, 'first facade commit response')
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
    await authority.telefuncRoomReleaseSecondDeliveryForTest()
    await Promise.all([first.delivery, second.delivery])
    return { firstSeq: first.seq, secondSeq: second.seq, firstBeforeSecond, secondBeforeRelease }
  })
  manager.dispose()
  await closeAndDrop(authority, inc, opened, `facade-order-close-${suffix}`)
  return result
}

async function terminalGenerationDrop(
  env: Env,
  session: Session,
  suffix: string,
): Promise<{ state: SubscriptionAttemptState; invalidations: Array<'recoverable' | 'terminal'> }> {
  const roomId = `terminal-${suffix}`
  const inc = `terminal-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openHead(authority, inc, 'terminal open')
  await session.prepareDelivery(roomId, false)
  await session.openSubscription(roomId, inc)
  await closeAndDrop(authority, inc, opened, `terminal-close-${suffix}`)
  return {
    state: await session.subscriptionState(roomId),
    invalidations: (await session.deliveryState(roomId)).invalidations,
  }
}

async function preAckTerminalGenerationDrop(env: Env, session: Session, suffix: string) {
  const roomId = `pre-ack-terminal-${suffix}`
  const inc = `pre-ack-terminal-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openHead(authority, inc, 'pre-ack terminal open')
  await session.prepareDelivery(roomId, false)
  await authority.telefuncRoomPrepareRegistrationHoldForTest()
  await session.openSubscription(roomId, inc, false)
  await within(authority.telefuncRoomWaitForRegistrationForTest(), 2_000, 'held route registration')
  if ((await session.subscriptionState(roomId)) !== 'establishing') {
    throw new Error('pre-ack terminal control did not hold the subscription in establishing state')
  }
  await closeAndDrop(authority, inc, opened, `pre-ack-terminal-close-${suffix}`)
  await authority.telefuncRoomReleaseRegistrationForTest()
  await waitUntil(async () => (await session.subscriptionState(roomId)) !== 'establishing', 2_000)
  return {
    ready: await session.subscriptionReadyOutcome(roomId),
    state: await session.subscriptionState(roomId),
    invalidations: (await session.deliveryState(roomId)).invalidations,
    generations: await authority.listGenerations(),
  }
}

async function preAckRecoverableRouteDrop(env: Env, session: Session, suffix: string) {
  const roomId = `pre-ack-recoverable-${suffix}`
  const inc = `pre-ack-recoverable-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openHead(authority, inc, 'pre-ack recoverable open')
  await session.prepareDelivery(roomId, false, true)
  await authority.telefuncRoomPrepareRegistrationHoldForTest()
  await session.openSubscription(roomId, inc, false)
  await within(authority.telefuncRoomWaitForRegistrationForTest(), 2_000, 'held recoverable route registration')
  if ((await session.subscriptionState(roomId)) !== 'establishing') {
    throw new Error('pre-ack recoverable control did not hold the subscription in establishing state')
  }
  const settlements: string[] = []
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const commit = accepted(
      await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([attempt])),
      `pre-ack failed delivery ${attempt}`,
    )
    settlements.push(
      await rejectionOf(authority.awaitDelivery(commit.deliveryToken), 2_000, `pre-ack failed delivery ${attempt}`),
    )
  }
  const invalidations = (await session.deliveryState(roomId)).invalidations
  await authority.telefuncRoomReleaseRegistrationForTest()
  const ready = await session.subscriptionReadyOutcome(roomId)
  const state = await session.subscriptionState(roomId)
  await closeAndDrop(authority, inc, opened, `pre-ack-recoverable-close-${suffix}`)
  return { state, ready, settlements, invalidations }
}

async function cancelledDelivery(env: Env, sessionId: DurableObjectId, session: Session, suffix: string) {
  const roomId = `cancel-${suffix}`
  const inc = `cancel-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openAndJoin(authority, sessionId, roomId, inc, `cancel-lease-${suffix}`)
  await session.prepareDelivery(roomId, true)
  const first = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([1])),
    'first cancellation control',
  )
  const second = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([2])),
    'second cancellation control',
  )
  await waitUntil(async () => (await session.deliveryState(roomId)).started, 2_000)
  await closeAndDrop(authority, inc, opened, `cancel-close-${suffix}`)
  await session.releaseDelivery(roomId)
  await within(authority.awaitDelivery(first.deliveryToken), 2_000, 'first cancellation control')
  return {
    cancelled: await rejectionOf(authority.awaitDelivery(second.deliveryToken), 2_000, 'cancelled delivery settlement'),
    cancellationDeliveries: (await session.deliveryState(roomId)).delivered,
  }
}

async function failedDeliveryEviction(
  env: Env,
  sessionId: DurableObjectId,
  session: Session,
  suffix: string,
): Promise<{
  settlements: string[]
  invalidations: Array<'recoverable' | 'terminal'>
}> {
  const roomId = `evict-${suffix}`
  const inc = `evict-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openAndJoin(authority, sessionId, roomId, inc, `evict-lease-${suffix}`)
  await session.prepareDelivery(roomId, false, true)
  const settlements: string[] = []
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const commit = accepted(
      await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([attempt])),
      `failed delivery ${attempt}`,
    )
    settlements.push(
      await rejectionOf(authority.awaitDelivery(commit.deliveryToken), 2_000, `failed delivery ${attempt}`),
    )
  }
  const invalidations = (await session.deliveryState(roomId)).invalidations
  await closeAndDrop(authority, inc, opened, `evict-close-${suffix}`)
  return { settlements, invalidations }
}

async function rejectedFanoutOrdering(
  env: Env,
  fastSessionId: DurableObjectId,
  fastSession: Session,
  suffix: string,
): Promise<{
  firstSettlementBeforeRelease: 'pending' | 'rejected'
  firstSettlementAfterRelease: 'rejected' | 'fulfilled'
  secondSettlement: string
  fastDeliveriesBeforeRelease: number[]
  slowDeliveriesBeforeRelease: number[]
}> {
  const roomId = `fanout-${suffix}`
  const inc = `fanout-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const opened = await openAndJoin(authority, fastSessionId, roomId, inc, `fanout-fast-${suffix}`)
  const slowSessionId = env.TelefuncDurableObject.idFromName(`slow-session-${suffix}`)
  const slowSession = env.TelefuncDurableObject.get(slowSessionId) as unknown as Session
  await fastSession.prepareDelivery(roomId, false, true)
  await slowSession.prepareDelivery(roomId, true)
  await join(authority, slowSessionId, roomId, inc, `fanout-slow-${suffix}`)
  const first = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([1])),
    'fanout first',
  )
  const second = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([2])),
    'fanout second',
  )
  const firstSettlement = authority.awaitDelivery(first.deliveryToken).then(
    () => 'fulfilled' as const,
    () => 'rejected' as const,
  )
  await waitUntil(async () => (await slowSession.deliveryState(roomId)).started, 2_000)
  const firstSettlementBeforeRelease = await Promise.race([
    firstSettlement,
    new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), 100)),
  ])
  if (firstSettlementBeforeRelease === 'fulfilled') throw new Error('failed fanout unexpectedly fulfilled')
  const fastDeliveriesBeforeRelease = (await fastSession.deliveryState(roomId)).delivered
  const slowDeliveriesBeforeRelease = (await slowSession.deliveryState(roomId)).delivered
  await slowSession.releaseDelivery(roomId)
  const firstSettlementAfterRelease = await within(firstSettlement, 2_000, 'fanout first settlement')
  const secondSettlement = await rejectionOf(authority.awaitDelivery(second.deliveryToken), 2_000, 'fanout second')
  await closeAndDrop(authority, inc, opened, `fanout-close-${suffix}`)
  return {
    firstSettlementBeforeRelease,
    firstSettlementAfterRelease,
    secondSettlement,
    fastDeliveriesBeforeRelease,
    slowDeliveriesBeforeRelease,
  }
}

async function authorityRestart(env: Env, suffix: string) {
  const roomId = `restart-${suffix}`
  const inc = `restart-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  await openHead(authority, inc, 'restart open')
  const oldCommit = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([1])),
    'old authority restart',
  )
  await authority.telefuncRoomReconstructForTest()
  const newCommit = accepted(
    await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([2])),
    'new authority restart',
  )
  return {
    old: await rejectionOf(authority.awaitDelivery(oldCommit.deliveryToken), 2_000, 'old-token settlement'),
    new: await rejectionOf(authority.awaitDelivery(newCommit.deliveryToken), 2_000, 'new-token settlement'),
  }
}

async function alarmScheduling(env: Env, sessionId: DurableObjectId, suffix: string) {
  const roomId = `alarm-${suffix}`
  const inc = `alarm-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  const idle = await authority.telefuncRoomAlarmForTest()
  await openHead(authority, inc, 'alarm open')
  const leaseId = `alarm-lease-${suffix}`
  await join(authority, sessionId, roomId, inc, leaseId)
  const afterRoute = (await authority.telefuncRoomAlarmForTest()) === null ? 'idle' : 'armed'
  await authority.unsubscribeRoute(inc, 'semantic', sessionId.toString(), leaseId)
  const afterUnsubscribe = await authority.telefuncRoomAlarmForTest()
  return { idle, afterRoute, afterUnsubscribe }
}

async function unpreparedControlFailures(env: Env, suffix: string) {
  const authority = roomAuthority(env, `unprepared-${suffix}`)
  return {
    waitForCommit: await rejectionOf(
      authority.telefuncRoomWaitForFirstCommitForTest(),
      2_000,
      'unprepared response wait',
    ),
    releaseCommit: await rejectionOf(
      authority.telefuncRoomReleaseFirstCommitForTest(),
      2_000,
      'unprepared response release',
    ),
    releaseDelivery: await rejectionOf(
      authority.telefuncRoomReleaseSecondDeliveryForTest(),
      2_000,
      'unprepared delivery release',
    ),
    waitForRegistration: await rejectionOf(
      authority.telefuncRoomWaitForRegistrationForTest(),
      2_000,
      'unprepared registration wait',
    ),
    releaseRegistration: await rejectionOf(
      authority.telefuncRoomReleaseRegistrationForTest(),
      2_000,
      'unprepared registration release',
    ),
  }
}

async function largeRetainedReplay(env: Env, suffix: string) {
  const roomId = `large-retained-${suffix}`
  const inc = `large-retained-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  await openHead(authority, inc, 'large retained open')
  const payload = new Uint8Array(25 * 1024 * 1024)
  payload.fill(0xa5)
  payload[0] = 0x11
  payload[payload.length - 1] = 0xee
  const commit = accepted(
    await authority.commitLane(roomId, inc, { kind: 'binary', member: 'member', track: 'track' }, payload, {
      retain: true,
    }),
    'large retained',
  )
  await authority.awaitDelivery(commit.deliveryToken)
  const retained = await authority.readRetained(inc, { kind: 'binary', member: 'member', track: 'track' })
  const replayed = (retained as { payload?: unknown } | null)?.payload
  if (!(replayed instanceof Uint8Array)) throw new Error('large retained replay did not return native bytes')
  return {
    bytes: replayed.byteLength,
    first: replayed[0],
    last: replayed[replayed.length - 1],
  }
}

async function nativeRpcRoundTrip(env: Env, suffix: string) {
  const authority = roomAuthority(env, `native-rpc-${suffix}`)
  const inc = `native-rpc-inc-${suffix}`
  const config = new Uint8Array([0x11, 0x22, 0x33])
  const opened = expectHead(
    await authority.compareExchangeHead(
      { expect: 'absent' },
      { head: { currentInc: inc, state: 'open', config } },
    ),
    'native RPC open',
  )
  const initialCells = await authority.readCells(inc, { keys: ['native'] })
  if ('staleInc' in initialCells) throw new Error('native RPC cell read was stale')
  const cellResult = await authority.compareExchangeCells(inc, initialCells.revision, [
    { key: 'native', set: { bytes: new Uint8Array([0x44, 0x55]) } },
  ])
  if (cellResult !== 'committed') throw new Error(`native RPC cell write returned ${cellResult}`)
  const storedCells = await authority.readCells(inc, { keys: ['native'] })
  if ('staleInc' in storedCells) throw new Error('native RPC cell reread was stale')
  const stored = storedCells.cells.get('native')
  return {
    headConfig: [...opened.config],
    cell: stored === undefined ? null : [...stored],
    validationError: await rejectionOf(
      authority.compareExchangeHead({ expect: 'absent' }, { delete: true }),
      2_000,
      'native RPC validation error',
    ),
  }
}

async function openAndJoin(
  authority: Authority,
  sessionId: DurableObjectId,
  roomId: string,
  inc: string,
  leaseId: string,
): Promise<RoomHead> {
  const opened = await openHead(authority, inc, 'open')
  await join(authority, sessionId, roomId, inc, leaseId)
  return opened
}

async function openHead(authority: Authority, inc: string, operation: string): Promise<RoomHead> {
  return expectHead(
    await authority.compareExchangeHead(
      { expect: 'absent' },
      { head: { currentInc: inc, state: 'open', config: new TextEncoder().encode('{}') } },
    ),
    operation,
  )
}

async function join(
  authority: Authority,
  sessionId: DurableObjectId,
  roomId: string,
  inc: string,
  leaseId: string,
): Promise<void> {
  const registration = await authority.registerRoute(roomId, inc, 'semantic', sessionId.toString(), leaseId)
  if (!('ok' in registration)) throw new Error(`route registration failed: ${registration.reason}`)
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

function roomAuthority(env: Env, roomId: string): Authority {
  return env.ROOM.get(env.ROOM.idFromName(roomId)) as unknown as Authority
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

async function waitUntil(predicate: () => Promise<boolean>, horizonMs: number): Promise<void> {
  const deadline = Date.now() + horizonMs
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${horizonMs}ms`)
    await new Promise<void>((resolve) => setTimeout(resolve, 10))
  }
}

async function within<T>(promise: Promise<T>, horizonMs: number, label: string): Promise<T> {
  let timer!: ReturnType<typeof setTimeout>
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} did not settle within ${horizonMs}ms`)), horizonMs)
  })
  try {
    return await Promise.race([promise, timeout])
  } finally {
    clearTimeout(timer)
  }
}

async function rejectionOf(promise: Promise<unknown>, horizonMs: number, label: string): Promise<string> {
  try {
    await within(promise, horizonMs, label)
    return 'resolved'
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void
  const promise = new Promise<void>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}
