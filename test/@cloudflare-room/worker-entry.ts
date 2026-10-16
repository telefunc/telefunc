/// <reference types="@cloudflare/workers-types" />
import { DurableObject } from 'cloudflare:workers'
import { Telefunc } from '../../packages/telefunc/serve/cloudflare.js'
import { decorateTelefunction } from '../../packages/telefunc/node/server/runTelefunc/decorateTelefunction.js'
import type { HeadCxResult, RoomHead } from '../../packages/telefunc/wire-protocol/backend/room/contract.js'
import type { RoomSessionDeliveryRequest } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/subscription.js'
import {
  RoomAuthority,
  type AcceptedCommitWire,
  type CommitWire,
} from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/room/do.js'
import { TELEFUNC_BROADCAST_BUCKET_HEADER } from '../../packages/telefunc/wire-protocol/server/adapter/cloudflare/routing.js'
import { ServerBroadcast } from '../../packages/telefunc/wire-protocol/server/server-broadcast.js'
import { getChannelMux } from '../../packages/telefunc/wire-protocol/server/mux.js'
import { parseHttpRequest } from '../../packages/telefunc/node/server/runTelefunc/parseHttpRequest.js'
import { createRequestContext } from '../../packages/telefunc/node/server/context/requestContext.js'
import { getServerConfig } from '../../packages/telefunc/node/server/serverConfig.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../packages/telefunc/wire-protocol/constants.js'
import { Room } from '../../packages/telefunc/wire-protocol/room/server/statics.js'
import { raceTimeout } from '../../packages/telefunc/utils/raceTimeout.js'
import { stringify } from '@brillout/json-serializer/stringify'
const { TelefuncDurableObject } = new Telefunc({ bindingName: 'TELEFUNC' })
// A probe's code runs as an app's code does: in a telefunction, which the session DO's fetch() runs in its session.
const PROBE_FILE = '/probe.telefunc.ts'
const probeRuns = new Map<string, () => Promise<void>>()
decorateTelefunction(async (runId: unknown) => probeRuns.get(runId as string)!(), 'onProbe', PROBE_FILE, '/')
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const INBOX_LANE = { kind: 'inbox', member: 'member' } as const
const INBOX_LANE_KEY = 'inbox:member'
const CONTROL_HORIZON_MS = 2_000
// The isolate's intervals, each with the session DO whose call made it and how often it has fired.
const intervals: { by: string; fired: number }[] = []
let makingFor = ''
const nativeSetInterval = globalThis.setInterval

globalThis.setInterval = ((callback: () => void, ms?: number) => {
  const interval = { by: makingFor, fired: 0 }
  intervals.push(interval)
  return nativeSetInterval(() => {
    interval.fired++
    callback()
  }, ms)
}) as typeof setInterval

export class TelefuncProbeDurableObject extends TelefuncDurableObject {
  readonly #frames: number[] = []
  readonly #held = new Map<string, unknown[]>()

  // Each subscriber records into its own DO's storage: I/O that only that DO may do, like a socket send.
  broadcastSubscribe(key: string): Promise<void> {
    return this.#inSession(async () => {
      this.ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS probe_received (seq INTEGER NOT NULL, text TEXT NOT NULL)')
      new ServerBroadcast<string>({ key }).subscribe((text, info) => {
        this.ctx.storage.sql.exec('INSERT INTO probe_received (seq, text) VALUES (?, ?)', info.seq, text)
      })
    })
  }

  broadcastPublish(key: string, texts: string[]): Promise<void> {
    return this.#inSession(async () => {
      const channel = new ServerBroadcast<string>({ key })
      await Promise.all(texts.map((text) => channel.publish(text)))
    })
  }

  broadcastReceived(): Array<{ seq: number; text: string }> {
    return this.ctx.storage.sql
      .exec<{ seq: number; text: string }>('SELECT seq, text FROM probe_received ORDER BY rowid')
      .toArray()
  }

  // A call whose argument is a callback, as a page makes it: the telefunction holds the callback, and the response
  // went out.
  callWithCallback(name: string): Promise<string> {
    return this.#inSession(async () => {
      const callback = { channelId: crypto.randomUUID() }
      const body = stringify(
        { file: '/pages/Upload.telefunc.ts', name: 'onUpload', args: [callback] },
        {
          replacer: (_key, value, serializer) =>
            value === callback
              ? {
                  replacement: SERIALIZER_PREFIX_FUNCTION + serializer({ channelId: callback.channelId }),
                  resolved: true,
                }
              : undefined,
        },
      )
      const request = new Request('https://room.test/_telefunc', { method: 'POST', body })
      const requestContext = createRequestContext(request)
      const parsed = await parseHttpRequest({
        request,
        requestContext,
        logMalformedRequests: false,
        serverConfig: getServerConfig(),
      })
      if (parsed.isMalformedRequest || parsed.isSseRequest) throw new Error('expected a telefunction request')
      makingFor = name
      const resolved = parsed.resolveRequest((() => undefined) as never)
      makingFor = ''
      if (resolved.isMalformedRequest) throw new Error('expected a resolved request')
      requestContext.markComplete()
      this.#held.set(callback.channelId, resolved.telefunctionArgs)
      return callback.channelId
    })
  }

  // Its page leaves: the callback's channel closes.
  leave(channelId: string): Promise<void> {
    return this.#inSession(async () => {
      getChannelMux()['channels'].get(channelId)!._onPeerClose()
      this.#held.delete(channelId)
    })
  }

  // A server creates a room nothing ever holds.
  createRoom(roomId: string, emptyTimeout: number): Promise<void> {
    return this.#inSession(async () => {
      await Room.create(roomId, { emptyTimeout })
    })
  }

  // A server writes a room's meta, and reads its keys after each step.
  metaKeyOrder(roomId: string): Promise<string[][]> {
    return this.#inSession(async () => {
      const room = await Room.create(roomId, { meta: { b: 1, a: 1 } })
      const seen: string[][] = []
      const see = async () => seen.push(Object.keys((await Room.get(room.id)).meta))
      await Room.setAttributes(room.id, { c: 1, a: 2 })
      await see()
      await Room.setAttributes(room.id, { aa: 1, b: undefined, a: 3 })
      await Room.setAttributes(room.id, { b: 2 })
      await see()
      await Room.setMeta(room.id, { z: 1, y: 1, x: 1 })
      await see()
      await Room.setAttributes(room.id, { y: 2, w: 1 })
      await see()
      return seen
    })
  }

  // A recipient answers an acknowledged send whose reply's frame is lost, then sends the sender a later DM.
  lostDmAck(roomId: string): Promise<unknown> {
    return this.#inSession(async () => {
      const room = await Room.create(roomId)
      const sender = await room.join()
      const recipient = await room.join()
      recipient.listen((data) => `handled:${String(data)}`)
      type Replica = { _onDm(serialized: string, ...rest: unknown[]): void }
      const replica = (room as unknown as { _replica: Replica })._replica
      const receive = replica._onDm.bind(replica)
      const lost = Promise.withResolvers<void>()
      replica._onDm = (serialized, ...rest) => {
        if (serialized.includes('"__r":"dm-ack"')) return lost.resolve()
        receive(serialized, ...rest)
      }
      const response = sender.send(recipient.id, 'move', { ack: true }).then((receipt) => receipt.response, String)
      await lost.promise
      await recipient.send(sender.id, 'after')
      const settled = await raceTimeout(response, 5_000, () => 'not within 5 s')
      await sender.leave()
      await recipient.leave()
      return settled
    })
  }

  // The server is gone, with the timers it set.
  die(): void {
    this.ctx.abort()
  }

  scheduledAlarm(): Promise<number | null> {
    return this.ctx.storage.getAlarm()
  }

  // A server watches every member's frames of a room, past its roster read: a member who joins later reaches it only by
  // the join's delivery.
  watchFrames(roomId: string): Promise<void> {
    return this.#inSession(async () => {
      const room = await Room.create(roomId)
      room.subscribeBinary((data) => this.#frames.push(data[0]!))
      await room.getParticipants()
    })
  }

  framesSeen(): number[] {
    return this.#frames
  }

  // A server's member joins a room and keeps a frame there.
  joinAndKeepFrame(roomId: string): Promise<void> {
    return this.#inSession(async () => {
      const member = await (await Room.get(roomId)).join()
      await member.publishBinary(new Uint8Array([7]), { retain: true })
    })
  }

  async #inSession<T>(probe: () => Promise<T>): Promise<T> {
    const runId = crypto.randomUUID()
    const outcome = Promise.withResolvers<T>()
    probeRuns.set(runId, () => probe().then(outcome.resolve, outcome.reject))
    const request = new Request('https://room.test/_telefunc', {
      method: 'POST',
      headers: { [TELEFUNC_BROADCAST_BUCKET_HEADER]: 'weur' },
      body: stringify({ file: PROBE_FILE, name: 'onProbe', args: [runId] }),
    })
    const response = await this.fetch!(request)
    probeRuns.delete(runId)
    if (!response.ok) throw new Error(`the probe's telefunction answered ${response.status}`)
    return outcome.promise
  }
}

// A session DO that records the Room frames it is handed, holding the one reading 'hold' until released; one told to
// refuse fails every handoff.
export class SessionDurableObject extends DurableObject {
  readonly #arrived: string[] = []
  readonly #held = Promise.withResolvers<void>()
  #refusing = false

  async telefuncRoomDeliver({ payload }: RoomSessionDeliveryRequest): Promise<void> {
    if (this.#refusing) throw new Error('this session refuses Room deliveries')
    const text = textDecoder.decode(payload)
    this.#arrived.push(text)
    if (text === 'hold') await this.#held.promise
  }

  refuse(): void {
    this.#refusing = true
  }

  release(): void {
    this.#held.resolve()
  }

  arrived(): string[] {
    return this.#arrived
  }
}

export class RoomProbeDurableObject extends RoomAuthority<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, env.TelefuncDurableObject)
  }

  scheduledAlarm(): Promise<number | null> {
    return this.ctx.storage.getAlarm()
  }

  fireAlarm(): Promise<void> {
    return this.alarm()
  }
}

type RpcMethods<T> = {
  [K in keyof T]: T[K] extends (...args: infer Args) => infer Result
    ? (...args: Args) => Promise<Awaited<Result>>
    : never
}

type Authority = RpcMethods<RoomProbeDurableObject>
type Session = RpcMethods<Pick<SessionDurableObject, 'refuse' | 'release' | 'arrived'>>

type BroadcastSession = RpcMethods<
  Pick<TelefuncProbeDurableObject, 'broadcastSubscribe' | 'broadcastPublish' | 'broadcastReceived'>
>

type CallbackSession = RpcMethods<Pick<TelefuncProbeDurableObject, 'callWithCallback' | 'leave'>>
type RoomServer = RpcMethods<Pick<TelefuncProbeDurableObject, 'createRoom' | 'metaKeyOrder' | 'lostDmAck' | 'die'>>
type LaneServer = RpcMethods<Pick<TelefuncProbeDurableObject, 'watchFrames' | 'framesSeen' | 'joinAndKeepFrame'>>
type TelefuncAuthority = RpcMethods<Pick<RoomProbeDurableObject, 'readHead' | 'scheduledAlarm'>>
type Env = Cloudflare.Env

const probes: Record<string, (env: Env, suffix: string) => Promise<unknown>> = {
  '/lost-target': lostTarget,
  '/pipelined-delivery': pipelinedDelivery,
  '/alarm-policy': alarmScheduling,
  '/route-renewal': routeRenewal,
  '/every-track': everyTrackRoutes,
  '/native-rpc': nativeRpcRoundTrip,
  '/large-retained': largeRetainedReplay,
  '/broadcast-sessions': broadcastAcrossSessions,
  '/refused-first-write': refusedFirstWrite,
  '/callback-timers': callbackTimers,
  '/auto-close': autoClose,
  '/meta-order': metaKeyOrder,
  '/lost-dm-ack': lostDmAck,
  '/join-opens-lane': joinOpensLane,
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const probe = probes[new URL(request.url).pathname]
    if (probe === undefined) return new Response(null, { status: 404 })
    try {
      return Response.json(await probe(env, crypto.randomUUID()))
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 })
    }
  },
}

function sessionOf(env: Env, suffix: string): DurableObjectId {
  return env.TelefuncDurableObject.idFromName(`session-${suffix}`)
}

function session(env: Env, suffix: string): Session {
  return env.TelefuncDurableObject.get(sessionOf(env, suffix)) as unknown as Session
}

function roomProbe(env: Env, suffix: string, name: string) {
  const roomId = `${name}-${suffix}`
  const inc = `${name}-inc-${suffix}`
  const authority = env.ROOM.get(env.ROOM.idFromName(roomId)) as unknown as Authority
  const lease = (role = 'lease') => `${name}-${role}-${suffix}`
  const open = (operation = `${name} open`) => openHead(authority, inc, operation)
  const join = async (sessionId: DurableObjectId, role = 'lease', laneKey = 'semantic') => {
    const registration = await authority.registerRoute({
      roomId,
      inc,
      laneKey,
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
    /** An inbox commit, the one kind whose delivery its committer awaits. */
    async commit(payload: string | number, operation: string) {
      const frame = typeof payload === 'string' ? textEncoder.encode(payload) : new Uint8Array([payload])
      return accepted(await authority.commitLane(inc, INBOX_LANE, frame), operation)
    },
    scheduledAlarm: () => authority.scheduledAlarm(),
    settle: (commit: AcceptedCommitWire) => authority.awaitDelivery(commit.deliveryToken!),
  }
}

async function lostTarget(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'lost-target')
  await probe.open()
  await session(env, suffix).refuse()
  await probe.join(sessionOf(env, suffix), 'lease', INBOX_LANE_KEY)
  const commit = await probe.commit(1, 'lost target')
  return { receivers: commit.receivers, settlement: await rejectionOf(probe.settle(commit), 'lost-target settlement') }
}

// The first frame is held at the session, so a later frame arrives only if it left without waiting for it.
async function pipelinedDelivery(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'pipelined')
  await probe.open()
  await probe.join(sessionOf(env, suffix), 'lease', INBOX_LANE_KEY)
  const frames = ['hold', ...Array.from({ length: 10 }, (_, index) => `frame-${index}`)]
  const commits = await Promise.all(frames.map((frame) => probe.commit(frame, `pipelined ${frame}`)))
  const whileHeld = await poll(
    () => session(env, suffix).arrived(),
    (arrived) => arrived.length === frames.length,
  )
  await session(env, suffix).release()
  return {
    whileHeld,
    settlements: await Promise.all(commits.map((commit) => rejectionOf(probe.settle(commit), 'pipelined settlement'))),
  }
}

// One session holds the camera lane of `m`, another its every-track lane, a third the camera's every-member lane.
async function everyTrackRoutes(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'every-track')
  await probe.open()
  for (const [role, laneKey] of [
    ['camera', 'binary:m:camera'],
    ['every-track', 'binary-all:m'],
    ['every-member', 'binary-track:camera'],
  ] as const) {
    await probe.join(sessionOf(env, `${role}-${suffix}`), role, laneKey)
  }
  const receivers: number[] = []
  for (const [member, track, text] of [
    ['m', 'camera', 'camera-1'],
    ['m', 'mic', 'mic-1'],
    ['other', 'camera', 'other'],
    ['m', 'camera', 'camera-2'],
  ] as const) {
    const frame = textEncoder.encode(text)
    const commit = accepted(await probe.authority.commitLane(probe.inc, { kind: 'binary', member, track }, frame), text)
    receivers.push(commit.receivers)
  }
  const arrived = (role: string, count: number) =>
    poll(
      () => session(env, `${role}-${suffix}`).arrived(),
      (frames) => frames.length >= count,
    )
  return {
    receivers,
    everyTrack: await arrived('every-track', 3),
    everyMember: await arrived('every-member', 3),
    camera: await arrived('camera', 2),
  }
}

async function alarmScheduling(env: Env, suffix: string) {
  const sessionId = sessionOf(env, suffix)
  const probe = roomProbe(env, suffix, 'alarm')
  const idle = await probe.scheduledAlarm()
  const head = await probe.open()
  await probe.join(sessionId)
  const afterRoute = (await probe.scheduledAlarm()) === null ? 'idle' : 'armed'
  await probe.authority.unsubscribeRoute({
    roomId: probe.roomId,
    inc: probe.inc,
    laneKey: 'semantic',
    sessionDoId: sessionId.toString(),
    leaseId: `alarm-lease-${suffix}`,
  })
  const afterUnsubscribe = await probe.scheduledAlarm()
  // A head naming another incarnation leaves the first one's generation orphaned.
  const next = { head: { currentInc: `${probe.inc}-next`, state: 'open' as const, config: head.config } }
  expectHead(await probe.authority.compareExchangeHead({ form: 'rev', rev: head.rev }, next), 'alarm reopen')
  const retry = await probe.scheduledAlarm()
  const afterReopen = retry === null ? 'idle' : 'armed'
  // The orphan's sweep is due: a later route registration doesn't push its retry out.
  await new Promise((resolve) => setTimeout(resolve, 20))
  const registration = await probe.authority.registerRoute({
    roomId: probe.roomId,
    inc: `${probe.inc}-next`,
    laneKey: 'semantic',
    sessionDoId: sessionId.toString(),
    leaseId: `alarm-next-lease-${suffix}`,
  })
  if (!('ok' in registration)) throw new Error(`route registration failed: ${registration.reason}`)
  const retryKept = (await probe.scheduledAlarm()) === retry
  await probe.authority.unsubscribeRoute({
    roomId: probe.roomId,
    inc: `${probe.inc}-next`,
    laneKey: 'semantic',
    sessionDoId: sessionId.toString(),
    leaseId: `alarm-next-lease-${suffix}`,
  })
  await probe.authority.fireAlarm()
  return { idle, afterRoute, afterUnsubscribe, afterReopen, retryKept, afterAlarm: await probe.scheduledAlarm() }
}

async function routeRenewal(env: Env, suffix: string) {
  const sessionId = sessionOf(env, suffix)
  const probe = roomProbe(env, suffix, 'renewal')
  await probe.open()
  await probe.join(sessionId)
  const route = { roomId: probe.roomId, inc: probe.inc, laneKey: 'semantic', sessionDoId: sessionId.toString() }
  return {
    live: await probe.authority.renewRoute({ ...route, leaseId: `renewal-lease-${suffix}` }),
    otherLease: await probe.authority.renewRoute({ ...route, leaseId: 'another-lease' }),
  }
}

// Two session DOs in one isolate, as local workerd runs them: both subscribe, one publishes.
async function broadcastAcrossSessions(env: Env, suffix: string) {
  const key = `broadcast-${suffix}`
  const session = (name: string) =>
    env.TELEFUNC.get(env.TELEFUNC.idFromName(`broadcast-session-${name}-${suffix}`)) as unknown as BroadcastSession
  const [a, b] = [session('a'), session('b')]
  await a.broadcastSubscribe(key)
  await b.broadcastSubscribe(key)
  await a.broadcastPublish(key, ['one', 'two', 'three'])
  return { a: await a.broadcastReceived(), b: await b.broadcastReceived() }
}

// Two session DOs in one isolate each take a call with a callback; a's page leaves, then b's. Reports, per session DO,
// how often the intervals its calls made fired while it held no callback, and after both pages left.
async function callbackTimers(env: Env, suffix: string) {
  const session = (name: string) =>
    env.TELEFUNC.get(env.TELEFUNC.idFromName(`callback-${name}-${suffix}`)) as unknown as CallbackSession
  const [a, b] = [session('a'), session('b')]
  const made = intervals.length
  const firedBy = (name: string) =>
    intervals
      .slice(made)
      .filter((interval) => interval.by === name)
      .reduce((fired, interval) => fired + interval.fired, 0)
  const scanPeriod = () => new Promise((resolve) => setTimeout(resolve, 5_500))
  const callA = await a.callWithCallback('a')
  const callB = await b.callWithCallback('b')
  await a.leave(callA)
  const aLeft = firedBy('a')
  await scanPeriod()
  const whileOnlyBHolds = { a: firedBy('a') - aLeft, made: intervals.slice(made).map(({ by }) => by) }
  await b.leave(callB)
  const bothLeft = { a: firedBy('a'), b: firedBy('b') }
  await scanPeriod()
  return {
    whileOnlyBHolds,
    afterBothLeft: { a: firedBy('a') - bothLeft.a, b: firedBy('b') - bothLeft.b },
  }
}

// Two servers each create a room nothing holds, closing 1 s on; one is gone at once. The live one closes its room as
// Room.close() does; the other's room lapses at its authority's alarm, with nothing left for the alarm after.
async function autoClose(env: Env, suffix: string) {
  const roomOf = (name: string) => {
    const roomId = `auto-close-${name}-${suffix}`
    const server = env.TELEFUNC.get(
      env.TELEFUNC.idFromName(`auto-close-server-${name}-${suffix}`),
    ) as unknown as RoomServer
    const authority = env.TELEFUNC.get(
      env.TELEFUNC.idFromName(`__telefunc_room__:${roomId}`),
    ) as unknown as TelefuncAuthority
    return { roomId, server, authority, state: async () => (await authority.readHead())?.state ?? 'absent' }
  }
  const [live, gone] = [roomOf('live'), roomOf('gone')]
  await live.server.createRoom(live.roomId, 1_000)
  await gone.server.createRoom(gone.roomId, 1_000)
  const alarmArmed = (await gone.authority.scheduledAlarm()) !== null
  await gone.server.die().catch(() => {})
  await new Promise((resolve) => setTimeout(resolve, 2_000))
  const afterTimeout = { live: await live.state(), gone: await gone.state() }
  await new Promise((resolve) => setTimeout(resolve, 5_000))
  return {
    alarmArmed,
    afterTimeout,
    afterLapse: await gone.state(),
    alarmAfterLapse: await gone.authority.scheduledAlarm(),
  }
}

async function metaKeyOrder(env: Env, suffix: string) {
  const server = env.TELEFUNC.get(env.TELEFUNC.idFromName(`meta-order-server-${suffix}`)) as unknown as RoomServer
  return await server.metaKeyOrder(`meta-order-${suffix}`)
}

async function lostDmAck(env: Env, suffix: string) {
  const server = env.TELEFUNC.get(env.TELEFUNC.idFromName(`lost-dm-ack-server-${suffix}`)) as unknown as RoomServer
  return await server.lostDmAck(`lost-dm-ack-${suffix}`)
}

// A server in one session watches every member's frames; a member joins from another session and keeps a frame. The
// join's delivery to the watching session opens that member's lane.
async function joinOpensLane(env: Env, suffix: string) {
  const roomId = `join-opens-lane-${suffix}`
  const server = (name: string) =>
    env.TELEFUNC.get(env.TELEFUNC.idFromName(`join-opens-lane-${name}-${suffix}`)) as unknown as LaneServer
  const [watcher, joiner] = [server('watcher'), server('joiner')]
  await watcher.watchFrames(roomId)
  await joiner.joinAndKeepFrame(roomId)
  return await poll(
    () => watcher.framesSeen(),
    (frames) => frames.length > 0,
  )
}

async function largeRetainedReplay(env: Env, suffix: string) {
  const probe = roomProbe(env, suffix, 'large-retained')
  await probe.open('large retained open')
  // A byte pattern whose period divides no chunk's length, so a chunk stored from the wrong bytes reads back unlike it.
  const payload = Uint8Array.from({ length: 25 * 1024 * 1024 }, (_, index) => index % 251)
  const lane = { kind: 'binary' as const, member: 'member', track: 'track' }
  accepted(await probe.authority.commitLane(probe.inc, lane, payload, { retain: true }), 'large retained')
  const retained = await probe.authority.readRetained(probe.inc, lane)
  const replayed = (retained as { payload?: unknown } | null)?.payload
  if (!(replayed instanceof Uint8Array)) throw new Error('large retained replay did not return native bytes')
  return { bytes: replayed.byteLength, firstMismatch: replayed.findIndex((byte, index) => byte !== index % 251) }
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
  const cellResult = await probe.authority.compareExchangeCells(probe.inc, {
    expect: [{ key: 'native', version: initialCells.versions.get('native') ?? null }],
    mutations: [{ key: 'native', bytes: new Uint8Array([0x44, 0x55]) }],
    counts: [{ key: 'count', delta: 1, create: true }],
  })
  if (typeof cellResult !== 'object') throw new Error(`native RPC cell write returned ${cellResult}`)
  const storedCells = await probe.authority.readCells(probe.inc, { keys: ['native'] })
  if ('staleInc' in storedCells) throw new Error('native RPC cell reread was stale')
  const stored = storedCells.cells.get('native')
  return {
    headConfig: [...opened.config],
    cell: stored === undefined ? null : [...stored],
    count: cellResult.committed.get('count')?.value,
    staleCell: await probe.authority.commitLane(probe.inc, { kind: 'semantic' }, new Uint8Array([1]), {
      requiredCellKeys: ['m:missing'],
    }),
  }
}

/** A fresh authority whose first SQL use is a write SQLite refuses keeps its tables for the next calls. */
async function refusedFirstWrite(env: Env, suffix: string) {
  const authority = env.ROOM.get(env.ROOM.idFromName(`refused-first-write-${suffix}`)) as unknown as Authority
  const refused = await rejectionOf(
    authority.directoryPut('x'.repeat(3 * 1024 * 1024), 'inc', Infinity, 0),
    'oversized id',
  )
  await authority.directoryPut('room', 'inc', Infinity, 0)
  return {
    refused: refused.includes('SQLITE_TOOBIG'),
    listed: (await authority.directoryList('')).entries.map(({ roomId }) => roomId),
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

function accepted(result: CommitWire, operation: string): AcceptedCommitWire {
  if ('stale' in result) throw new Error(`${operation} commit was stale`)
  return result
}

async function poll<T>(read: () => Promise<T>, done: (value: T) => boolean): Promise<T> {
  const deadline = Date.now() + CONTROL_HORIZON_MS
  for (;;) {
    const value = await read()
    if (done(value) || Date.now() >= deadline) return value
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

async function rejectionOf(promise: Promise<unknown>, label: string): Promise<string> {
  try {
    await raceTimeout(promise, CONTROL_HORIZON_MS, () => {
      throw new Error(`${label} did not settle within ${CONTROL_HORIZON_MS}ms`)
    })
    return 'resolved'
  } catch (error) {
    return error instanceof Error ? error.message : String(error)
  }
}
