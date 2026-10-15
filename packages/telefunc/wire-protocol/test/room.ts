export {
  decoder,
  semanticLane,
  HOLD_RECORD_KEYS,
  driver,
  memoryState,
  installRoomBackend,
  disposeRoomBackend,
  here,
  anotherServer,
  lapseSessionOf,
  attachPeer,
  EVERY_TRACK,
  NO_TRACK,
  yieldAfterEachReadAndCountWrites,
  subsOf,
  syncOf,
  controlOf,
  binaryLanes,
  mark,
  ran,
  timeOutsideGc,
  trackNames,
  spyGossip,
  tellAgain,
  applyPage,
  expire,
  leasesHeld,
  captureHeartbeats,
  countDemandLane,
  countBinarySubscriptions,
  wantsHeld,
  declareEach,
  stubIndexOf,
  replacerContext,
  declare,
  register,
  serve,
  joinThrough,
  createTail,
  relayed,
  rosterEnded,
  memberEvents,
  clientView,
  notices,
  semanticFrames,
  delayRosterRead,
  wideBinaryScenario,
  fakeClient,
  standaloneParticipant,
  newState,
  snapshot,
  pendingClientJoin,
  forceRoomGc,
  gcFixture,
  retainOnlyListedRemote,
  retainOnlyRevivedRemote,
  retainOnlyJoinedParticipant,
  retainOnlyCallbackRemote,
  retainOnlyDepartedParticipant,
  terminalSubscription,
  rejectedSubscription,
  captureOutcome,
  mockLaneSubscription,
  rejectLaneSubscriptions,
  failoverPoint,
  loseLaneFrames,
  holdLaneDelivery,
  delayDriverLane,
  delayLaneSubscription,
}
export type { Peer }

import { expect, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { TAG, decode, type BroadcastSubscriptions } from '../shared-ws.js'
import { createDeferred } from '../../utils/createDeferred.js'
import { ROOM_HEARTBEAT_INTERVAL_MS } from '../room/server/constants.js'
import { DEFAULT_TRACK } from '../room/binary.js'
import { type RoomSnapshotMetadata } from '../room/protocol.js'
import { type DemandEntry, type RoomDemandPage } from '../room/server/protocol.js'
import {
  HOLDS_CELL_KEY,
  JOINS_CELL_KEY,
  MEMBER_CELL_PREFIX,
  memberCellKey,
  sessionCellKey,
} from '../room/server/cells.js'
import type { ParticipantMeta } from '../room/types.js'
import { ClientRoom, ClientStandaloneParticipant } from '../room/client.js'
import { ClientBroadcast, type ClientChannel } from '../client/channel.js'
import { RoomModel, RoomState, type RoomStateView } from '../room/state.js'
import { Room } from '../room/server/statics.js'
import { ServerRoom } from '../room/server/room.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import type { LaneSubscription } from '../room/server/lane-subscription.js'
import { ResponseRoomGrants, RoomStubChannel } from '../room/server/stub.js'
import { type DemandReport, RoomDemand } from '../room/server/demand.js'
import { roomRemoteReviver } from '../room/response-client.js'
import type { InternalClientReviverContext, InternalServerReplacerContext } from '../types.js'
import type { ServerChannel } from '../server/channel.js'
import type { ChannelPublishInfo } from '../channel.js'
import { disposeBackend, getRoomBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import type { LaneId, RoomSubscriptionSource } from '../backend/room/contract.js'
import { encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from '../backend/room/lane-key.js'
import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionState,
} from '../backend/subscription.js'
import { GcRegistry } from '../gcRegistry.js'
import { wrapProxy } from '../wrapProxy.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const semanticLane = { kind: 'semantic' } as const satisfies LaneId
type SubscribedLane = RoomSubscriptionSource['lane']
const HOLD_RECORD_KEYS = [HOLDS_CELL_KEY, JOINS_CELL_KEY].join()
let driver: MemoryBackend
let memoryState: MemoryBackendState
/** `state`, a fresh memory store by default, installed as this process's backend: each spec runs it first. */
async function installRoomBackend(state = new MemoryBackendState()): Promise<void> {
  await disposeBackend()
  memoryState = state
  driver = new MemoryBackend({ state: memoryState })
  installBackend(() => driver)
}
/** Ends what a spec left: its fake timers and mocks, the other servers it started, and the backend. */
async function disposeRoomBackend(): Promise<void> {
  vi.useRealTimers()
  vi.restoreAllMocks()
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    for (const server of otherServers.splice(0)) await server.crash()
    await disposeBackend()
  } finally {
    report.mockRestore()
  }
}
/** A server running this code: its Room API, its stubs, and its connection to the store. */
type Server = {
  Room: typeof Room
  ServerRoom: typeof ServerRoom
  RoomStubChannel: typeof RoomStubChannel
  ResponseRoomGrants: typeof ResponseRoomGrants
  readonly driver: MemoryBackend
  backend(): ReturnType<typeof getRoomBackend>
  /** The server goes away: nothing it does reaches the store any more. */
  crash(): Promise<void>
}
const here: Server = {
  Room,
  ServerRoom,
  RoomStubChannel,
  ResponseRoomGrants,
  get driver() {
    return driver
  },
  backend: getRoomBackend,
  crash: disposeBackend,
}
const otherServers: Server[] = []
/** Another server on this spec's store: a module graph and process state of its own, so its own replicas and lanes. */
async function anotherServer(): Promise<Server> {
  const globals: Record<string, unknown> = {}
  vi.resetModules()
  vi.doMock('../../utils/getGlobalObject.js', () => ({
    getGlobalObject: (key: string, value: unknown) =>
      (globals[key] ??= typeof value === 'function' ? (value as () => unknown)() : value),
  }))
  try {
    const [statics, rooms, stubs, install, memory] = await Promise.all([
      import('../room/server/statics.js'),
      import('../room/server/room.js'),
      import('../room/server/stub.js'),
      import('../backend/install.js'),
      import('../backend/memory/backend.js'),
    ])
    let crashed = false
    const serverDriver = new Proxy(new memory.MemoryBackend({ state: memoryState }), {
      get(target, key) {
        const value = Reflect.get(target, key)
        if (typeof value !== 'function') return value
        return (...args: unknown[]) => {
          if (!crashed) return Reflect.apply(value, target, args)
          // A crashed server's calls, its handles' writes included, never reach the store.
          return value.constructor.name === 'AsyncGeneratorFunction'
            ? (async function* () {
                await new Promise(() => {})
              })()
            : new Promise(() => {})
        }
      },
    })
    install.installBackend(() => serverDriver)
    const server: Server = {
      Room: statics.Room,
      ServerRoom: rooms.ServerRoom,
      RoomStubChannel: stubs.RoomStubChannel,
      ResponseRoomGrants: stubs.ResponseRoomGrants,
      driver: serverDriver,
      backend: install.getRoomBackend,
      async crash() {
        await install.disposeBackend()
        crashed = true
      },
    }
    otherServers.push(server)
    return server
  } finally {
    vi.doUnmock('../../utils/getGlobalObject.js')
    vi.resetModules()
  }
}
function serverOf(room: Room | ServerRoom): Server {
  return otherServers.find((server) => room instanceof server.ServerRoom) ?? here
}
/** Lapses the session hosting the member, as its server's crash does once the session's lease runs out; returns the
 *  session's cell key. */
async function lapseSessionOf(room: ServerRoom, memberId: string): Promise<string> {
  const memberKey = memberCellKey(memberId)
  const member = await driver.readCells(room.id, room._incarnation.inc, { keys: [memberKey] })
  if ('staleInc' in member) throw new Error('unexpected stale generation')
  const key = sessionCellKey((parse(decoder.decode(member.cells.get(memberKey)!)) as { session: string }).session)
  const session = await driver.readCells(room.id, room._incarnation.inc, { keys: [key] })
  if ('staleInc' in session) throw new Error('unexpected stale generation')
  const lapsed = { key, bytes: encoder.encode(stringify({ until: session.now - 1 })) }
  const cx = { expect: [{ key, version: session.versions.get(key)! }], mutations: [lapsed] }
  expect(await driver.compareExchangeCells(room.id, room._incarnation.inc, cx)).toEqual({ committed: new Map() })
  return key
}
type Peer = ReturnType<typeof attachPeer>
function attachPeer(stub: ServerChannel, lastSeq?: number, broadcast?: BroadcastSubscriptions) {
  const frames: Uint8Array[] = []
  const replay = stub._replayBuffer!
  if (lastSeq !== undefined) {
    const missed = replay.getAfter(lastSeq)
    if (missed === null) throw new Error('The replay dropped a frame the peer lacks')
    frames.push(...missed)
  }
  const peer = new IndexedPeer(
    {
      send: (frame, onCommit) => {
        frames.push(frame)
        onCommit?.()
      },
      carry: (frame) => void frames.push(frame),
      bufferedAmount: () => 0,
      deliveredBeforeFrame: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    },
    7,
    replay,
  )
  stub._attachPeer(peer, { broadcast })
  return { peer, decoded: () => frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)) }
}
const EVERY_TRACK = { member: null, track: null, on: true }
const NO_TRACK = { member: null, track: null, on: false }
/** Every read of the driver yields once, so other calls' writes land between a read and the write after it; the
 *  returned function counts the head and cell writes since. */
function yieldAfterEachReadAndCountWrites(): () => number {
  for (const method of ['readHead', 'readCells'] as const) {
    const read = (driver[method] as (...args: unknown[]) => Promise<unknown>).bind(driver)
    vi.spyOn(driver, method).mockImplementation((async (...args: unknown[]) => {
      const result = await read(...args)
      await new Promise((resolve) => setTimeout(resolve, 0))
      return result
    }) as never)
  }
  const writes = [vi.spyOn(driver, 'compareExchangeHead'), vi.spyOn(driver, 'compareExchangeCells')]
  return () => writes.reduce((total, write) => total + write.mock.calls.length, 0)
}
function subsOf(room: Room | ServerRoom): {
  _semantic: LaneSubscription
  _binary: Map<string, LaneSubscription>
  _everyTrack: Map<string, LaneSubscription>
  _everyMember: Map<string, LaneSubscription>
  _inbox: Map<string, LaneSubscription>
  _dropped: Set<LaneSubscription>
  _demandLane: LaneSubscription
  memberOwned(member: string): void
  memberChanged(member: string): void
  replan(): Promise<void>
  binaryReady(): Promise<void>
} {
  return (room as ServerRoom)._replica.plan as unknown as ReturnType<typeof subsOf>
}
function syncOf(room: Room | ServerRoom): {
  refreshMembers(): Promise<void>
  _heartbeatTick(): Promise<void>
  ensureRoster(): Promise<void>
  reconcileAuthority(): Promise<void>
  readonly _heartbeatBusy: boolean
} {
  return (room as ServerRoom)._replica.sync as unknown as ReturnType<typeof syncOf>
}
/** The control lane the room's replica subscribes. */
function controlOf(room: Room | ServerRoom): LaneSubscription {
  return ((room as ServerRoom)._replica.plan as unknown as { _control: LaneSubscription })._control
}
/** The binary lanes a room subscribes, by their backend keys. */
function binaryLanes(room: ServerRoom): string[] {
  const subs = subsOf(room)
  const named = [...subs._binary.keys()].map((key) => {
    const [member, track] = key.split('\0') as [string, string]
    return encodeLaneKey({ kind: 'binary', member, track })
  })
  const fanIn = [
    ...[...subs._everyTrack.keys()].map(everyTrackLaneKey),
    ...[...subs._everyMember.keys()].map(everyMemberLaneKey),
  ]
  return [...fanIn, ...named].sort()
}
type Mark = { wall: number; cpu: NodeJS.CpuUsage }
const mark = (): Mark => ({ wall: performance.now(), cpu: process.cpuUsage() })
/** Milliseconds the process ran from `from` to `to`: wall time, but no more than its CPU time, which the machine's
 *  load, holding the process off a CPU, does not add to. */
function ran(from: Mark, to: Mark): number {
  const cpu = to.cpu.user - from.cpu.user + to.cpu.system - from.cpu.system
  return Math.min(to.wall - from.wall, cpu / 1000)
}
/** How long `run` took, less the garbage collections during it: their pauses grow with the live heap, not with what
 *  `run` does. */
async function timeOutsideGc(run: () => Promise<void>): Promise<number> {
  const collections: PerformanceEntry[] = []
  const observer = new PerformanceObserver((entries) => void collections.push(...entries.getEntries()))
  observer.observe({ entryTypes: ['gc'] })
  const started = performance.now()
  await run()
  const ended = performance.now()
  // A collection's entry arrives after it.
  await new Promise((resolve) => setTimeout(resolve, 0))
  observer.disconnect()
  const collecting = collections
    .filter((entry) => entry.startTime >= started && entry.startTime < ended)
    .reduce((total, entry) => total + entry.duration, 0)
  return ended - started - collecting
}
function trackNames(count: number): string[] {
  return Array.from({ length: count }, (_, i) => `t${i}`)
}
/** The room's demand report, and the entries its instance tells from now on. */
function spyGossip(room: Room | ServerRoom) {
  const report = (room as ServerRoom)._replica.report
  return { report, published: vi.spyOn(report as unknown as { _tell(entry: DemandEntry, on: boolean): void }, '_tell') }
}
/** The whole report told again, all at once. */
function tellAgain(report: DemandReport): void {
  for (const _ of report.all());
}
const pagesTold = new WeakMap<RoomDemand, Map<string, number>>()
/** `instance`'s next page of `wants` to `demand`. */
function applyPage(demand: RoomDemand, instance: string, wants: RoomDemandPage['wants']): void {
  let pages = pagesTold.get(demand)
  if (!pages) pagesTold.set(demand, (pages = new Map()))
  const page = (pages.get(instance) ?? 0) + 1
  pages.set(instance, page)
  demand.applyPage({ __r: 'demand', instance, page, wants })
}
/** One heartbeat's drop of the reports that lapsed, all at once. */
function expire(demand: RoomDemand): void {
  for (const _ of demand.expire());
}
/** The leases the demand holds on other processes' reports. */
function leasesHeld(demand: RoomDemand): number {
  const { _pairs, _members, _tracks } = demand as unknown as Record<string, Map<unknown, unknown>>
  return _pairs!.size + _members!.size + _tracks!.size
}
/** The room heartbeats started from now on; `fire()` runs each still running once, as one interval passing would. */
function captureHeartbeats(): { fire(): void } {
  const running = new Map<unknown, () => void>()
  const setInterval = globalThis.setInterval
  const clearInterval = globalThis.clearInterval
  vi.spyOn(globalThis, 'setInterval').mockImplementation(((callback: () => void, ms?: number) => {
    const timer = setInterval(callback, ms)
    if (ms === ROOM_HEARTBEAT_INTERVAL_MS) running.set(timer, callback)
    return timer
  }) as typeof globalThis.setInterval)
  vi.spyOn(globalThis, 'clearInterval').mockImplementation((timer) => {
    running.delete(timer)
    clearInterval(timer)
  })
  return { fire: () => [...running.values()].forEach((callback) => callback()) }
}
/** The room's demand-lane commits, and the pages this process's receivers took off the lane, from now on. */
function countDemandLane(roomId: string) {
  const backend = getRoomBackend()
  const commitLane = backend.commitLane.bind(backend)
  const subscribeLane = backend.subscribeLane.bind(backend)
  const counts = { commits: 0, parsed: 0, inFlight: 0 }
  vi.spyOn(backend, 'commitLane').mockImplementation((...args) => {
    const committed = commitLane(...args)
    if (args[0] !== roomId || args[2].kind !== 'demand') return committed
    counts.commits++
    counts.inFlight++
    void committed
      .then((result) => ('stale' in result ? undefined : result.delivery))
      .catch(() => {})
      .finally(() => counts.inFlight--)
    return committed
  })
  vi.spyOn(backend, 'subscribeLane').mockImplementation((id, inc, lane, receiver) =>
    subscribeLane(id, inc, lane, (payload, info) => {
      if (id === roomId && lane.kind === 'demand') counts.parsed++
      return receiver(payload, info)
    }),
  )
  return {
    get commits() {
      return counts.commits
    },
    get parsed() {
      return counts.parsed
    },
    reset() {
      counts.commits = counts.parsed = 0
    },
    /** No commit in flight for five timer turns in a row, each on time. */
    async quiet() {
      for (let quiet = 0; quiet < 5; ) {
        const turn = performance.now()
        await new Promise((resolve) => setTimeout(resolve, 20))
        quiet = counts.inFlight === 0 && performance.now() - turn < 30 ? quiet + 1 : 0
      }
    },
  }
}
/** The binary lanes opened on the backend from now on. */
function countBinarySubscriptions(on = here): SubscribedLane[] {
  const subscribed: SubscribedLane[] = []
  mockLaneSubscription(
    (lane) => lane.kind === 'binary' || lane.kind === 'binary-all' || lane.kind === 'binary-track',
    (subscribeLane, roomId, inc, lane, receiver) => {
      subscribed.push(lane)
      return subscribeLane(roomId, inc, lane, receiver)
    },
    on,
  )
  return subscribed
}
/** The member ids a stub's wants name, as it holds them and as the room's index does. */
function wantsHeld(room: ServerRoom, stub: RoomStubChannel) {
  const held = stub as unknown as { _textMemberWants: Set<string>; _binary: { members: Map<string, unknown> } }
  const index = (
    subsOf(room) as unknown as { _wants: { _textMembers: Map<string, unknown>; _members: Map<string, unknown> } }
  )._wants
  return {
    text: [...held._textMemberWants],
    binary: [...held._binary.members.keys()],
    indexedText: [...index._textMembers.keys()],
    indexedBinary: [...index._members.keys()],
  }
}
/** Declares a binary want for every track of each id, and a text want for each, in order. */
function declareEach(stub: RoomStubChannel, ids: string[]): void {
  for (const id of ids) declare(stub, { __r: 'sub-binary', member: id, track: null, on: true })
  declare(stub, { __r: 'sub-text', announce: false, members: Object.fromEntries(ids.map((id) => [id, true])) })
}
function stubIndexOf(room: ServerRoom): Map<string, RoomStubChannel> {
  return (room as unknown as { _stubOf: Map<string, RoomStubChannel> })._stubOf
}
function replacerContext(channels: ServerChannel[]): InternalServerReplacerContext {
  const states = new Map<symbol, unknown>()
  return {
    registerChannel: (channel: ServerChannel) => {
      channel._registerChannel()
      channels.push(channel)
    },
    validators: new Map(),
    responseState<T>(key: symbol, init: () => T): T {
      if (!states.has(key)) states.set(key, init())
      return states.get(key) as T
    },
  } as unknown as InternalServerReplacerContext
}
function declare(stub: RoomStubChannel, declaration: unknown): void {
  stub._onPeerMessage(stringify(declaration), 0)
}
function register(room: ServerRoom): RoomStubChannel {
  const { RoomStubChannel, ResponseRoomGrants } = serverOf(room)
  const stub = new RoomStubChannel(room, { grants: new ResponseRoomGrants() })
  stub._registerChannel()
  room._attachStub(stub)
  return stub
}
function serve(room: ServerRoom): { stub: RoomStubChannel; peer: Peer } {
  const stub = register(room)
  return { stub, peer: attachPeer(stub) }
}
/** A client's first join through its Room stub; returns the member id. */
async function joinThrough(stub: RoomStubChannel, meta: ParticipantMeta = {}): Promise<string> {
  return ((await stub._handleRequest({ __r: 'req-join', reqId: 0, meta, selfDelivery: true })) as { id: string }).id
}
async function createTail(id: string) {
  await Room.create(id)
  const source = await Room.get(id)
  return {
    member: await source.join(),
    tail: (await Room.get(id, { tail: true })) as ServerRoom,
  }
}
type RelayedEvent = {
  __r: string
  id?: string
  members?: Array<{ id: string; meta: unknown }>
  meta?: unknown
  data?: unknown
  ackId?: string
}
/** Every event a Room stub relayed to its client, in order. */
function relayed(peer: Peer): RelayedEvent[] {
  return peer.decoded().flatMap((frame) => (frame.tag === TAG.PUBLISH ? [parse(frame.text) as RelayedEvent] : []))
}
/** Whether the stub relayed a roster's last part. */
function rosterEnded(peer: Peer): boolean {
  return relayed(peer).some((event) => event.__r === 'roster' && !('more' in event))
}
function memberEvents(peer: Peer, id: string): RelayedEvent[] {
  return relayed(peer).filter((event) => event.id === id)
}
/** A client Room fed every event its stub relayed, in order. */
function clientView(peer: Peer, roomId: string): ClientRoom {
  const { client, emit } = fakeClient(roomId)
  for (const frame of peer.decoded()) if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
  return client
}
/** A participant stub's notices to its client. */
function notices(peer: Peer): Array<{ __r: string }> {
  return peer.decoded().flatMap((frame) => (frame.tag === TAG.TEXT ? [parse(frame.text) as { __r: string }] : []))
}
function semanticFrames(peer: Peer, kind: 'data' | 'announce'): unknown[] {
  return relayed(peer)
    .filter((event) => event.__r === kind)
    .map((event) => event.data)
}

function delayRosterRead(roomId: string, on = here): { started: Promise<void>; release: () => void } {
  const { driver } = on
  const readCellPages = driver.readCellPages.bind(driver)
  const roster = { started: createDeferred(), release: createDeferred() }
  vi.spyOn(driver, 'readCellPages').mockImplementation(async function* (candidateRoomId, inc, prefix) {
    if (candidateRoomId === roomId && prefix === MEMBER_CELL_PREFIX) {
      roster.started.resolve()
      await roster.release.promise
    }
    yield* readCellPages(candidateRoomId, inc, prefix)
  })
  return { started: roster.started.promise, release: roster.release.resolve }
}
async function wideBinaryScenario(id: string, retain: boolean, byte: number) {
  const serverRoom = (await Room.create(id)) as ServerRoom
  const camera = await serverRoom.join()
  const { stub, peer } = serve(serverRoom)
  const serverSeqs: number[] = []
  if (!retain) serverRoom.subscribeBinary((_data, info) => serverSeqs.push(info.seq))
  const generation = memoryState.rooms.get(id)!.gens.get(serverRoom._incarnation.inc)!
  const lane = encodeLaneKey({ kind: 'binary', member: camera.id, track: DEFAULT_TRACK })
  generation.memberOrders.set(camera.id, new Map([[lane, { seq: 0xffff_ffff, timestamp: 10 }]]))
  if (!retain) {
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
    await subsOf(serverRoom).binaryReady()
  }
  const receipt = await camera.publishBinary(new Uint8Array([byte]), retain ? { retain: true } : undefined)
  if (retain) {
    declare(stub, { __r: 'sub-binary', ...EVERY_TRACK })
    await subsOf(serverRoom).binaryReady()
  }
  await vi.waitFor(() => expect(peer.decoded().some((candidate) => candidate.tag === TAG.PUBLISH_BINARY)).toBe(true))
  const frame = peer
    .decoded()
    .filter((candidate) => candidate.tag === TAG.PUBLISH_BINARY)
    .at(-1)!
  const fake = createFakeStub()
  const client = new ClientRoom(fake.stub, snapshot(id))
  const clientSeqs: number[] = []
  client.subscribeBinary((_data, info) => clientSeqs.push(info.seq))
  fake.emitBinary(frame.data, { key: id, ...frame.info })
  return { receipt: receipt.seq, server: serverSeqs[0] ?? null, client: clientSeqs[0] }
}
function createFakeStub(options?: {
  send?: (message: unknown, options?: { ack?: boolean }) => Promise<unknown>
  wireDeclarations?: boolean[]
}): {
  stub: ClientBroadcast
  emitText(data: unknown, info: ChannelPublishInfo): void
  emitBinary(data: Uint8Array, info: ChannelPublishInfo): void
} {
  const text: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const binary: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  const stub = {
    _subscribed: { text: false, binary: false },
    _isClosed: false,
    _connection: {
      sendBroadcastSubscribe: () => options?.wireDeclarations?.push(true),
      sendBroadcastUnsubscribe: () => options?.wireDeclarations?.push(false),
    },
    _subscribeLocal: (kind: 'text' | 'binary', callback: never) => {
      const listeners: unknown[] = kind === 'text' ? text : binary
      listeners.push(callback)
      return () => listeners.splice(listeners.indexOf(callback), 1)
    },
    _setSubscribed: ClientBroadcast.prototype._setSubscribed,
    send: options?.send ?? (async () => undefined),
    _publishUnreported: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    _publishBinaryUnreported: async () => ({ key: 'fake', seq: 1, timestamp: 1 }),
    onClose: () => {},
  } as unknown as ClientBroadcast
  return {
    stub,
    emitText: (data, info) => text.forEach((callback) => callback(data, info)),
    emitBinary: (data, info) => binary.forEach((callback) => callback(data, info)),
  }
}
function fakeClient(
  roomId: string,
  options?: Parameters<typeof createFakeStub>[0],
  snapshotOverride?: Partial<RoomSnapshotMetadata>,
) {
  const fake = createFakeStub(options)
  return {
    fake,
    client: new ClientRoom(fake.stub, { ...snapshot(roomId), ...snapshotOverride }),
    emit: (data: unknown, seq = 1) => fake.emitText(data, { key: roomId, seq, timestamp: seq }),
  }
}
/** A client-held participant whose channel the test drives: `notify` delivers a stub notice. */
function standaloneParticipant(
  meta: ParticipantMeta,
  send?: () => Promise<unknown>,
  bufferLimit = getServerConfig().channel.bufferLimit,
) {
  let notify!: (notice: unknown) => unknown
  const channel = {
    listen: (cb: (notice: unknown) => unknown) => {
      notify = cb
    },
    onClose: () => {},
    send,
  } as unknown as ClientChannel
  const participant = new ClientStandaloneParticipant(channel, {
    channelId: 'channel',
    id: 'me',
    meta,
    selfDelivery: true,
    identity: null,
    bufferLimit,
  })
  return { participant, notify: (notice: unknown) => notify(notice) }
}
function newState(
  options: Partial<
    ConstructorParameters<typeof RoomModel>[0] & Pick<ConstructorParameters<typeof RoomState>[0], 'onCallbackError'>
  >,
): RoomState {
  const { onCallbackError = () => {}, ...model } = options
  return new RoomState({
    owner: {} as RoomStateView, // read only by the serializer
    model: new RoomModel({ roomId: 'state', meta: [], seed: { members: [] }, metaSeq: 0, onLeave: () => {}, ...model }),
    onListenersChanged: () => {},
    onCallbackError,
  })
}
function snapshot(roomId: string): RoomSnapshotMetadata {
  return {
    channelId: 'channel',
    roomId,
    meta: [],
    closed: false,
    count: 0,
    metaSeq: 0,
    bufferLimit: getServerConfig().channel.bufferLimit,
  }
}
async function pendingClientJoin(roomId: string, onSend?: (message: unknown) => unknown) {
  const id = crypto.randomUUID()
  const ack = createDeferred<{ id: string; joinedAt: number }>()
  const { fake, client, emit } = fakeClient(roomId, {
    send: async (message) => {
      const request = message as { __r?: string; reqId?: number }
      if (request.__r !== 'req-join') return await onSend?.(message)
      emit({ __r: 'joined', reqId: request.reqId, id }, 0)
      return await ack.promise
    },
  })
  const joining = client.join()
  await Promise.resolve()
  return { id, ack, fake, client, emit, joining }
}
async function forceRoomGc(): Promise<void> {
  for (let cycle = 0; cycle < 8; cycle++) {
    ;(globalThis as { gc(): void }).gc()
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}
function gcFixture(roomId: string, join = false) {
  const memberId = crypto.randomUUID()
  const fake = createFakeStub(
    join
      ? { send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined) }
      : {},
  )
  let closeCount = 0
  return {
    memberId,
    fake,
    target: new ClientRoom(fake.stub, snapshot(roomId)),
    registry: new GcRegistry(5),
    onClose: () => {
      closeCount++
    },
    closed: () => closeCount,
  }
}
type GcFixture = ReturnType<typeof gcFixture>
async function retainOnlyListedRemote({ target, fake, registry, onClose, memberId }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  fake.emitText(
    {
      __r: 'roster',
      members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0, identity: null }],
      part: 0,
    },
    { key: target.id, seq: 1, timestamp: 1 },
  )
  const [member] = await room.getParticipants()
  return { member: member!, room: new WeakRef(room) }
}
function retainOnlyRevivedRemote({ target, registry, onClose, memberId }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  const context = { shareLifecycle: () => {} } as unknown as InternalClientReviverContext
  const metadata = { room, id: memberId, meta: {}, joinedAt: 1, metaSeq: 0, identity: null }
  return { member: roomRemoteReviver.revive(metadata, context).value, room: new WeakRef(room) }
}
async function retainOnlyJoinedParticipant({ target, registry, onClose }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  return { member: await room.join(), room: new WeakRef(room) }
}
async function retainOnlyCallbackRemote({ target, fake, registry, onClose, memberId }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  let member: ReturnType<ClientRoom['getParticipant']> extends Promise<infer T> ? NonNullable<T> : never
  const stop = room.onJoin((joined) => {
    member = joined
  })
  fake.emitText({ __r: 'join', id: memberId, meta: {}, joinedAt: 1 }, { key: target.id, seq: 1, timestamp: 1 })
  stop()
  return { member: member!, room: new WeakRef(room) }
}
async function retainOnlyDepartedParticipant({ target, fake, registry, onClose, memberId }: GcFixture) {
  const room = wrapProxy(target)
  registry.register(room, onClose)
  const member = await room.join()
  fake.emitText({ __r: 'leave', id: memberId }, { key: target.id, seq: 1, timestamp: 1 })
  return { member, room: new WeakRef(room) }
}
function terminalSubscription(inner?: BackendSubscription): {
  subscription: BackendSubscription
  close(): Promise<void>
} {
  let state = inner?.state() ?? 'ready'
  const listeners = new Set<(next: SubscriptionState) => void>()
  let closed = false
  const subscription: BackendSubscription = {
    ready: inner?.ready ?? Promise.resolve(),
    state: () => state,
    onStateChange: (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    unsubscribe: async () => {
      if (closed) return
      closed = true
      state = 'closed'
      for (const listener of listeners) listener(state)
      listeners.clear()
      await inner?.unsubscribe()
    },
  }
  return { subscription, close: subscription.unsubscribe }
}
function rejectedSubscription(diagnostic: string): BackendSubscription {
  const ready = Promise.reject(new Error(diagnostic))
  void ready.catch(() => {})
  return {
    ready,
    state: () => 'closed',
    onStateChange: () => () => {},
    unsubscribe: async () => {},
  }
}
function captureOutcome<T>(promise: Promise<T>) {
  const outcome: { value: unknown } = { value: Symbol('pending') }
  void promise.then(
    (value) => {
      outcome.value = value
    },
    (error: unknown) => {
      outcome.value = error
    },
  )
  return outcome
}
function mockLaneSubscription(
  match: SubscribedLane['kind'] | ((lane: SubscribedLane) => boolean),
  replacement: (
    subscribeLane: ReturnType<typeof getRoomBackend>['subscribeLane'],
    roomId: string,
    inc: string,
    lane: SubscribedLane,
    receiver: BackendReceiver,
  ) => BackendSubscription,
  on = here,
) {
  const backend = on.backend()
  const subscribeLane = backend.subscribeLane.bind(backend)
  const matches = typeof match === 'function' ? match : (lane: SubscribedLane) => lane.kind === match
  vi.spyOn(backend, 'subscribeLane').mockImplementation((roomId, inc, lane, receiver) =>
    matches(lane)
      ? replacement(subscribeLane, roomId, inc, lane, receiver)
      : subscribeLane(roomId, inc, lane, receiver),
  )
  return backend
}
function rejectLaneSubscriptions(kind: SubscribedLane['kind'], diagnostic: string) {
  vi.useFakeTimers()
  const started = createDeferred()
  const backend = mockLaneSubscription(kind, () => {
    started.resolve()
    return rejectedSubscription(diagnostic)
  })
  vi.spyOn(console, 'error').mockImplementation(() => {})
  return { backend, started: started.promise }
}
/** Loses the frames picked on matching lanes opened from now on, as a lane that stays ready. */
/** Puts the room's generation back as it is now, its cells and its room-wide lanes' order, as a failover onto a master
 *  that missed every write after now leaves it. */
function failoverPoint(room: ServerRoom): () => void {
  const generation = memoryState.rooms.get(room.id)!.gens.get(room._incarnation.inc)!
  const version = generation.version
  const cells = new Map(generation.cells)
  const cellsByPrefix = new Map([...generation.cellsByPrefix].map(([prefix, keys]) => [prefix, new Set(keys)]))
  const order = new Map(generation.order)
  return () => {
    generation.version = version
    generation.cells.clear()
    for (const [key, cell] of cells) generation.cells.set(key, cell)
    generation.cellsByPrefix.clear()
    for (const [prefix, keys] of cellsByPrefix) generation.cellsByPrefix.set(prefix, keys)
    generation.order.clear()
    for (const [lane, info] of order) generation.order.set(lane, info)
  }
}
function loseLaneFrames(matches: (lane: SubscribedLane) => boolean, on = here) {
  let pick: (text: string) => boolean = () => false
  mockLaneSubscription(
    matches,
    (subscribeLane, roomId, inc, lane, receiver) =>
      subscribeLane(roomId, inc, lane, (payload, info) => {
        if (!pick(decoder.decode(payload))) receiver(payload, info)
      }),
    on,
  )
  return {
    next() {
      pick = () => ((pick = () => false), true)
    },
    where(picks: (text: string) => boolean) {
      pick = picks
    },
  }
}
/** Holds the frames delivered on matching lanes opened from now on, until released in delivery order. */
function holdLaneDelivery(matches: (lane: SubscribedLane) => boolean, on = here) {
  const held: Array<() => void | Promise<void>> = []
  let holding = true
  mockLaneSubscription(
    matches,
    (subscribeLane, roomId, inc, lane, receiver) =>
      subscribeLane(roomId, inc, lane, (payload, info) => {
        if (!holding) return receiver(payload, info)
        held.push(() => receiver(payload, info))
      }),
    on,
  )
  return {
    async release() {
      holding = false
      for (const deliver of held.splice(0)) await deliver()
    },
  }
}
/** Matching lanes' driver attempts receive only once released, as a Redis SUBSCRIBE still in flight. */
function delayDriverLane(matches: (lane: SubscribedLane) => boolean, on = here): () => void {
  const { driver } = on
  const bind = driver.subscriptions.bind.bind(driver.subscriptions)
  const releases: Array<() => void> = []
  let released = false
  vi.spyOn(driver.subscriptions, 'bind').mockImplementation((source) => {
    const binding = bind(source)
    if (!('lane' in source) || !matches(source.lane)) return binding
    return {
      ...binding,
      open: (receiver, localReceiverCount): SubscriptionAttempt => {
        // A planning pass opens a lane set a slice at a time, so some lanes of it can open after the release.
        if (released) return binding.open(receiver, localReceiverCount)
        let inner: SubscriptionAttempt | null = null
        let state: SubscriptionState = 'establishing'
        const listeners = new Set<(next: SubscriptionState) => void>()
        releases.push(() => {
          inner = binding.open(receiver, localReceiverCount)
          state = inner.state()
          for (const listener of listeners) listener(state)
        })
        return {
          state: () => state,
          onStateChange: (listener) => {
            listeners.add(listener)
            return () => listeners.delete(listener)
          },
          unsubscribe: async () => {
            state = 'closed'
            await inner?.unsubscribe()
          },
        }
      },
    }
  })
  return () => {
    released = true
    for (const release of releases.splice(0)) release()
  }
}
function delayLaneSubscription(matches: (lane: SubscribedLane) => boolean) {
  const started = createDeferred()
  let release!: () => Promise<void>
  mockLaneSubscription(matches, (subscribeLane, roomId, inc, lane, receiver) => {
    let inner: BackendSubscription | null = null
    let state: SubscriptionState = 'establishing'
    const readiness = createDeferred()
    const listeners = new Set<(next: SubscriptionState) => void>()
    started.resolve()
    release = async () => {
      inner = subscribeLane(roomId, inc, lane, receiver)
      await inner.ready
      state = 'ready'
      readiness.resolve()
      for (const listener of listeners) listener(state)
    }
    return {
      ready: readiness.promise,
      state: () => state,
      onStateChange: (listener) => {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      unsubscribe: async () => {
        state = 'closed'
        readiness.resolve()
        for (const listener of listeners) listener(state)
        await inner?.unsubscribe()
      },
    }
  })
  return { started: started.promise, release: () => release() }
}
