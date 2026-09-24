/// <reference types="@cloudflare/workers-types" />
import { DurableObject, env as workerEnv } from 'cloudflare:workers'
import '../../packages/telefunc/node/server/async_hooks.js'
import { installBackend } from '../../packages/telefunc/wire-protocol/backend/install.js'
import type { HeadCxResult, RoomHead } from '../../packages/telefunc/wire-protocol/backend/room/contract.js'
import { Room } from '../../packages/telefunc/wire-protocol/room/server/statics.js'
import {
  CloudflareRoomBackend,
  CloudflareRoomSessionManager,
  type CloudflareRoomNamespace,
  type RoomSessionDeliveryRequest,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/backend.js'
import {
  RoomAuthority,
  RoomAuthorityHost,
  type CommitWire,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/do.js'
import {
  CloudflareBroadcastAuthorityState,
  CloudflareBroadcastTransport,
  type BroadcastCalls,
  type BroadcastDeliverRequest,
  type BroadcastForwardRequest,
  type BroadcastPresenceRequest,
  type BroadcastPublishRequest,
  type CloudflareBroadcastMember,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/broadcast.js'
import { OrderedStubs } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/ordered-stubs.js'
import { withCloudflareSession } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/session.js'
import { ServerBroadcast } from '../../packages/telefunc/wire-protocol/server/server-broadcast.js'
import {
  dispatchRoomFanout,
  type RoomFanoutNamespace,
  type RoomFanoutRequest,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/fanout.js'
const broadcast = new CloudflareBroadcastTransport({
  baseInstanceName: 'telefunc',
  namespace: () => (workerEnv as unknown as Env).PUBLIC,
})
installBackend(
  () =>
    new CloudflareRoomBackend({
      rooms: () => (workerEnv as unknown as Env).PUBLIC as unknown as CloudflareRoomNamespace,
      broadcast,
    }),
  ['cloudflare-room-ci-public'],
)
const fanoutNamespace = (namespace: DurableObjectNamespace) => namespace as unknown as RoomFanoutNamespace
const textEncoder = new TextEncoder()
const CONTROL_HORIZON_MS = 2_000
// Like the production class: one namespace hosts sessions, room authorities and Broadcast authorities.
export class PublicDurableObject extends RoomAuthorityHost<Env> {
  readonly #manager: CloudflareRoomSessionManager
  readonly #calls: BroadcastCalls = new OrderedStubs()
  readonly #broadcastAuthority: CloudflareBroadcastAuthorityState
  readonly #member: CloudflareBroadcastMember
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, fanoutNamespace(env.PUBLIC))
    this.#manager = new CloudflareRoomSessionManager(ctx.id.toString())
    this.#broadcastAuthority = new CloudflareBroadcastAuthorityState(ctx)
    this.#member = broadcast.member(ctx.id.toString(), this.#calls)
    this.#member.locate('weur')
  }
  // Each subscriber records into its own DO's storage: I/O that only that DO may do, like a socket send.
  broadcastSubscribe(key: string): Promise<void> {
    return this.#run(async () => {
      this.ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS probe_received (seq INTEGER NOT NULL, text TEXT NOT NULL)')
      new ServerBroadcast<string>({ key }).subscribe((text, info) => {
        this.ctx.storage.sql.exec('INSERT INTO probe_received (seq, text) VALUES (?, ?)', info.seq, text)
      })
    })
  }
  broadcastPublish(key: string, texts: string[]): Promise<void> {
    return this.#run(async () => {
      const channel = new ServerBroadcast<string>({ key })
      await Promise.all(texts.map((text) => channel.publish(text)))
    })
  }
  broadcastReceived(): Array<{ seq: number; text: string }> {
    return this.ctx.storage.sql
      .exec<{ seq: number; text: string }>('SELECT seq, text FROM probe_received ORDER BY rowid')
      .toArray()
  }
  telefuncBroadcastPublish(request: BroadcastPublishRequest) {
    return broadcast.publishToSubscribers(this.#broadcastAuthority, this.#calls, request)
  }
  telefuncBroadcastForward(request: BroadcastForwardRequest) {
    return broadcast.forwardToBucket(this.#calls, request)
  }
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest) {
    return this.#run(() => this.#member.deliver(request))
  }
  telefuncBroadcastPresence(request: BroadcastPresenceRequest) {
    return this.#broadcastAuthority.setPresence(request)
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
  telefuncRoomDeliver(request: RoomSessionDeliveryRequest): void {
    return this.#run(() => this.#manager.deliver(request))
  }
  telefuncRoomFanout(request: RoomFanoutRequest) {
    return dispatchRoomFanout(fanoutNamespace(this.env.PUBLIC), request)
  }
  #run<T>(fn: () => T): T {
    return withCloudflareSession({ room: () => this.#manager, broadcast: () => this.#member }, fn)
  }
}
// A session without Room delivery methods, so every handoff to it fails.
export class SessionDurableObject extends DurableObject {}
type AuthorityControl = 'reconstruct' | 'alarm'
export class RoomProbeDurableObject extends RoomAuthorityHost<Env> {
  #reconstructed: RoomAuthority | null = null
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, fanoutNamespace(env.TelefuncDurableObject))
  }
  override commitLane(...args: Parameters<RoomAuthority['commitLane']>) {
    return this.#reconstructed === null ? super.commitLane(...args) : this.#reconstructed.commitLane(...args)
  }
  override awaitDelivery(token: string): Promise<void> {
    return this.#reconstructed === null ? super.awaitDelivery(token) : this.#reconstructed.awaitDelivery(token)
  }
  async telefuncRoomControlForTest(action: AuthorityControl): Promise<number | null | void> {
    if (action === 'reconstruct') {
      this.#reconstructed = new RoomAuthority(this.ctx, fanoutNamespace(this.env.TelefuncDurableObject))
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
type Authority = RpcMethods<RoomProbeDurableObject>
type PublicSession = RpcMethods<Pick<PublicDurableObject, 'publicRoomLifecycle'>>
type BroadcastSession = RpcMethods<
  Pick<PublicDurableObject, 'broadcastSubscribe' | 'broadcastPublish' | 'broadcastReceived'>
>
type Env = {
  ROOM: DurableObjectNamespace
  TelefuncDurableObject: DurableObjectNamespace
  PUBLIC: DurableObjectNamespace
}
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const suffix = crypto.randomUUID()
      if (new URL(request.url).pathname === '/large-retained') {
        return Response.json(await largeRetainedReplay(env, suffix))
      }
      if (new URL(request.url).pathname === '/broadcast-sessions') {
        return Response.json(await broadcastAcrossSessions(env, suffix))
      }
      const publicSession = env.PUBLIC.get(
        env.PUBLIC.idFromName(`public-session-${suffix}`),
      ) as unknown as PublicSession
      const sessionId = env.TelefuncDurableObject.idFromName(`session-${suffix}`)
      return Response.json({
        publicLifecycle: await publicSession.publicRoomLifecycle(`public-room-${suffix}`),
        restartSettlement: await authorityRestart(env, suffix),
        lostTarget: await lostTarget(env, sessionId, suffix),
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
    const registration = await authority.registerRoute({
      roomId,
      inc,
      laneKey: 'semantic',
      sessionDoId: sessionId.toString(),
      leaseId: lease(role),
    })
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
      return accepted(await authority.commitLane(inc, { kind: 'semantic' }, frame), operation)
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
async function lostTarget(env: Env, sessionId: DurableObjectId, suffix: string) {
  const probe = roomProbe(env, suffix, 'lost-target')
  await probe.open()
  // SessionDurableObject has no telefuncRoomDeliver, so every handoff to this route fails.
  await probe.join(sessionId)
  const commit = await probe.commit(1, 'lost target')
  return { receivers: commit.receivers, settlement: await rejectionOf(probe.settle(commit), 'lost-target settlement') }
}
async function alarmScheduling(env: Env, sessionId: DurableObjectId, suffix: string) {
  const probe = roomProbe(env, suffix, 'alarm')
  const idle = await probe.control('alarm')
  await probe.open()
  await probe.join(sessionId)
  const afterRoute = (await probe.control('alarm')) === null ? 'idle' : 'armed'
  await probe.authority.unsubscribeRoute({
    roomId: probe.roomId,
    inc: probe.inc,
    laneKey: 'semantic',
    sessionDoId: sessionId.toString(),
    leaseId: `alarm-lease-${suffix}`,
  })
  const afterUnsubscribe = await probe.control('alarm')
  return { idle, afterRoute, afterUnsubscribe }
}
// Two session DOs in one isolate, as local workerd runs them: both subscribe, one publishes.
async function broadcastAcrossSessions(env: Env, suffix: string) {
  const key = `broadcast-${suffix}`
  const session = (name: string) =>
    env.PUBLIC.get(env.PUBLIC.idFromName(`broadcast-session-${name}-${suffix}`)) as unknown as BroadcastSession
  const [a, b] = [session('a'), session('b')]
  await a.broadcastSubscribe(key)
  await b.broadcastSubscribe(key)
  await a.broadcastPublish(key, ['one', 'two', 'three'])
  return { a: await a.broadcastReceived(), b: await b.broadcastReceived() }
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
    await probe.authority.commitLane(probe.inc, lane, payload, { retain: true }),
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
      { form: 'absent' },
      { head: { currentInc: probe.inc, state: 'open', config } },
    ),
    'native RPC open',
  )
  const initialCells = await probe.authority.readCells(probe.inc, { keys: ['native'] })
  if ('staleInc' in initialCells) throw new Error('native RPC cell read was stale')
  const cellResult = await probe.authority.compareExchangeCells(probe.inc, initialCells.revision, [
    { key: 'native', bytes: new Uint8Array([0x44, 0x55]) },
  ])
  if (cellResult !== 'committed') throw new Error(`native RPC cell write returned ${cellResult}`)
  const storedCells = await probe.authority.readCells(probe.inc, { keys: ['native'] })
  if ('staleInc' in storedCells) throw new Error('native RPC cell reread was stale')
  const stored = storedCells.cells.get('native')
  return {
    headConfig: [...opened.config],
    cell: stored === undefined ? null : [...stored],
    staleCell: await probe.authority.commitLane(probe.inc, { kind: 'semantic' }, new Uint8Array([1]), {
      requiredCellKeys: ['m:missing'],
    }),
  }
}
async function openHead(authority: Authority, inc: string, operation: string): Promise<RoomHead> {
  return expectHead(
    await authority.compareExchangeHead(
      { form: 'absent' },
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
