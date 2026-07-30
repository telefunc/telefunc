/// <reference types="@cloudflare/workers-types" />

import { DurableObject } from 'cloudflare:workers'
import type { HeadCx, LaneId } from '../../packages/telefunc/wire-protocol/backend/spi.js'
import {
  TelefuncRoomDurableObject as ProductionRoomDurableObject,
  type HeadNextWire,
  type HeadWire,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/do.js'

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
type DeliveryState = {
  blockFirst: boolean
  started: boolean
  delivered: number[]
  gate: Promise<void>
  release: () => void
}

export class SessionDurableObject extends DurableObject {
  readonly #deliveries = new Map<string, DeliveryState>()

  prepareDelivery(roomId: string, blockFirst: boolean): void {
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    this.#deliveries.set(roomId, { blockFirst, started: false, delivered: [], gate, release })
  }

  deliveryState(roomId: string): { started: boolean; delivered: number[] } {
    const state = this.#deliveries.get(roomId)
    if (state === undefined) throw new Error('delivery probe was not prepared')
    return { started: state.started, delivered: [...state.delivered] }
  }

  releaseDelivery(roomId: string): void {
    const state = this.#deliveries.get(roomId)
    if (state === undefined) throw new Error('delivery probe was not prepared')
    state.release()
  }

  async telefuncRoomDeliver(request: DeliveryRequest): Promise<void> {
    const state = this.#deliveries.get(request.roomId)
    if (state === undefined) throw new Error('delivery reached an unprepared session')
    state.delivered.push(request.seq)
    if (state.blockFirst && request.seq === 1) {
      state.started = true
      await state.gate
    }
  }

  telefuncRoomInvalidate(): void {}
}

export class TelefuncRoomDurableObject extends ProductionRoomDurableObject {
  readonly #probeEnv: unknown
  #reconstructed: ProductionRoomDurableObject | null = null

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    this.#probeEnv = env
  }

  override awaitDelivery(token: string): Promise<void> {
    return this.#reconstructed === null ? super.awaitDelivery(token) : this.#reconstructed.awaitDelivery(token)
  }

  telefuncRoomReconstructForTest(): void {
    this.#reconstructed = new ProductionRoomDurableObject(this.ctx, this.#probeEnv)
  }
}

type HeadResult =
  | { ok: true; head: HeadWire }
  | { ok: true; deleted: true }
  | { conflict: true; current: HeadWire | null }
  | { error: string }
type CommitResult =
  | { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string }
  | { stale: true }
  | { error: string }
type Authority = {
  readHead(): Promise<HeadWire | null>
  compareExchangeHead(cx: HeadCx, next: HeadNextWire): Promise<HeadResult>
  captureRouteGeneration(
    inc: string,
  ): Promise<{ ok: true; generationToken: string } | { rejected: true; reason: string }>
  registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    generationToken: string,
  ): Promise<{ ok: true } | { rejected: true; reason: string }>
  commitLane(roomId: string, inc: string, lane: LaneId, payload: Uint8Array): Promise<CommitResult>
  awaitDelivery(token: string): Promise<void>
  dropGeneration(inc: string): Promise<{ droppedSubscribers: unknown[] } | { error: string }>
  listGenerations(): Promise<string[]>
  telefuncRoomReconstructForTest(): Promise<void>
}
type Session = {
  prepareDelivery(roomId: string, blockFirst: boolean): Promise<void>
  deliveryState(roomId: string): Promise<{ started: boolean; delivered: number[] }>
  releaseDelivery(roomId: string): Promise<void>
}
type Env = {
  ROOM: DurableObjectNamespace
  TelefuncDurableObject: DurableObjectNamespace
}

export default {
  async fetch(_request: Request, env: Env): Promise<Response> {
    try {
      const suffix = crypto.randomUUID()
      const sessionId = env.TelefuncDurableObject.idFromName(`session-${suffix}`)
      const session = env.TelefuncDurableObject.get(sessionId) as unknown as Session
      const lifecycle = await successfulLifecycle(env, sessionId, session, suffix)
      const cancellation = await cancelledDelivery(env, sessionId, session, suffix)
      const unknown = await authorityRestart(env, suffix)
      return Response.json({ lifecycle, ...cancellation, unknown })
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
  }
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

async function authorityRestart(env: Env, suffix: string): Promise<string> {
  const roomId = `restart-${suffix}`
  const inc = `restart-inc-${suffix}`
  const authority = roomAuthority(env, roomId)
  expectHead(
    await authority.compareExchangeHead(
      { expect: 'absent' },
      { head: { currentInc: inc, state: 'open', configB64: 'e30=' } },
    ),
    'restart open',
  )
  const commit = accepted(await authority.commitLane(roomId, inc, { kind: 'semantic' }, new Uint8Array([1])), 'restart')
  await authority.telefuncRoomReconstructForTest()
  return rejectionOf(authority.awaitDelivery(commit.deliveryToken), 2_000, 'unknown-token settlement')
}

async function openAndJoin(
  authority: Authority,
  sessionId: DurableObjectId,
  roomId: string,
  inc: string,
  leaseId: string,
): Promise<HeadWire> {
  const opened = expectHead(
    await authority.compareExchangeHead(
      { expect: 'absent' },
      { head: { currentInc: inc, state: 'open', configB64: 'e30=' } },
    ),
    'open',
  )
  const capture = await authority.captureRouteGeneration(inc)
  if (!('ok' in capture)) throw new Error(`generation capture failed: ${capture.reason}`)
  const registration = await authority.registerRoute(
    roomId,
    inc,
    'semantic',
    sessionId.toString(),
    leaseId,
    capture.generationToken,
  )
  if (!('ok' in registration)) throw new Error(`route registration failed: ${registration.reason}`)
  return opened
}

async function closeAndDrop(authority: Authority, inc: string, opened: HeadWire, leaseId: string): Promise<void> {
  const closing = expectHead(
    await authority.compareExchangeHead(
      { expect: { rev: opened.rev } },
      {
        head: {
          currentInc: inc,
          state: 'closing',
          configB64: opened.configB64,
          closeLease: { id: leaseId, durationMs: 60_000 },
        },
      },
    ),
    'enter closing',
  )
  expectHead(
    await authority.compareExchangeHead(
      { expect: { rev: closing.rev, closingLease: leaseId } },
      { head: { currentInc: null, state: 'closed', configB64: closing.configB64 }, ttlMs: 60_000 },
    ),
    'finalize close',
  )
  const dropped = await authority.dropGeneration(inc)
  if ('error' in dropped) throw new Error(dropped.error)
}

function roomAuthority(env: Env, roomId: string): Authority {
  return env.ROOM.get(env.ROOM.idFromName(roomId)) as unknown as Authority
}

function expectHead(result: HeadResult, operation: string): HeadWire {
  if ('error' in result) throw new Error(`${operation} failed: ${result.error}`)
  if ('conflict' in result) throw new Error(`${operation} conflicted`)
  if (!('head' in result)) throw new Error(`${operation} returned no head`)
  return result.head
}

function accepted(result: CommitResult, operation: string): Extract<CommitResult, { accepted: true }> {
  if ('error' in result) throw new Error(`${operation} commit failed: ${result.error}`)
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

async function rejectionOf(promise: Promise<void>, horizonMs: number, label: string): Promise<string> {
  try {
    await within(promise, horizonMs, label)
    return 'resolved'
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}
