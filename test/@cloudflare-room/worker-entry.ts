/// <reference types="@cloudflare/workers-types" />
import { DurableObject } from 'cloudflare:workers'
import '../../packages/telefunc/node/server/async_hooks.js'
import {
  BACKEND_SPI_VERSION,
  type BackendDriverPair,
} from '../../packages/telefunc/wire-protocol/backend/driver-pair.js'
import { setDefaultBackend } from '../../packages/telefunc/wire-protocol/backend/install.js'
import type { RoomHead } from '../../packages/telefunc/wire-protocol/backend/room/contract.js'
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
export class SessionDurableObject extends DurableObject {}
type AuthorityControl = 'reconstruct' | 'alarm'
export class TelefuncRoomDurableObject extends ProductionRoomDurableObject {
  readonly #probeEnv: unknown
  #reconstructed: ProductionRoomDurableObject | null = null
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    this.#probeEnv = env
  }
  override commitLane(...args: Parameters<ProductionRoomDurableObject['commitLane']>) {
    return this.#reconstructed === null ? super.commitLane(...args) : this.#reconstructed.commitLane(...args)
  }
  override awaitDelivery(token: string): Promise<void> {
    return this.#reconstructed === null ? super.awaitDelivery(token) : this.#reconstructed.awaitDelivery(token)
  }
  async telefuncRoomControlForTest(action: AuthorityControl): Promise<number | null | void> {
    if (action === 'reconstruct') {
      this.#reconstructed = new ProductionRoomDurableObject(this.ctx, this.#probeEnv)
      return
    }
    return this.ctx.storage.getAlarm()
  }
}
type RpcMethods<T> = {
  [K in keyof T]: T[K] extends (...args: infer Args) => infer Result
    ? (...args: Args) => Promise<Awaited<Result>>
    : never
}
type Authority = RpcMethods<TelefuncRoomDurableObject>
type PublicSession = RpcMethods<Pick<PublicRoomSessionDurableObject, 'publicRoomLifecycle'>>
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
      const sessionId = env.TelefuncDurableObject.idFromName(`session-${suffix}`)
      return Response.json({
        publicLifecycle: await publicSession.publicRoomLifecycle(`public-room-${suffix}`),
        restartSettlement: await authorityRestart(env, suffix),
        alarmPolicy: await alarmScheduling(env, sessionId, suffix),
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
    async commit(payload: string | number, operation: string) {
      const frame = typeof payload === 'string' ? textEncoder.encode(payload) : new Uint8Array([payload])
      return accepted(await authority.commitLane(roomId, inc, { kind: 'semantic' }, frame), operation)
    },
    control: (action: AuthorityControl) => authority.telefuncRoomControlForTest(action),
    settle: (commit: Extract<CommitWire, { accepted: true }>) => authority.awaitDelivery(commit.deliveryToken),
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
function expectHead(result: HeadCxResult, operation: string): RoomHead {
  if ('conflict' in result) throw new Error(`${operation} conflicted`)
  if (!('head' in result)) throw new Error(`${operation} returned no head`)
  return result.head
}
function accepted(result: CommitWire, operation: string): Extract<CommitWire, { accepted: true }> {
  if ('stale' in result) throw new Error(`${operation} commit was stale`)
  return result
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
