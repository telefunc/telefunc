// Node-side control plane for local workerd conformance. All Room SPI calls and receiver callbacks run
// in ConformanceSessionDurableObject through the production Cloudflare backend/session manager. This
// file holds only serializable command handles and immutable observation caches.

import { Miniflare } from 'miniflare'
import { ClientBroadcast } from '../../../client/channel.js'
import { CHANNEL_TRANSPORT } from '../../../constants.js'
import { ClientRoom } from '../../../room/client.js'
import type {
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  LaneReceiver,
  LaneSubscription,
  ReadinessState,
  RoomBackendSpi,
  RoomHead,
} from '../../spi.js'
import type {
  CellsWire,
  CommitWire,
  DropWire,
  GenerationWire,
  HeadCxWire,
  HeadNextWire,
  HeadWire,
  RegisterWire,
  RetainedWire,
} from '../../../server/adapter/cloudflare/room/do.js'
import { base64ToBytes, bytesToBase64 } from '../../../server/adapter/cloudflare/room/codec.js'
import { bindRemoteReceiver, pollRemoteReceiver, receiverDescriptor, unbindRemoteReceiver } from '../receiver.js'
import type { BackendFixture, BackendHarness } from '../harness.js'
import { bundleWorker } from './bundle.js'
import type { SessionRoomCommand, SessionRoomReply } from './commands.js'

const MAX_RETAINED_BYTES = 16 * 1024 * 1024

export type RoomStub = {
  [Symbol.dispose]?: () => void
  readHead(): Promise<HeadWire | null>
  compareExchangeHead(cx: HeadCx, next: HeadNextWire): Promise<HeadCxWire>
  readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsWire>
  compareExchangeCells(
    inc: string,
    revision: string,
    mutations: Array<{ key: string; set?: { bytesB64: string; ttlMs?: number } }>,
  ): Promise<CxResult>
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string },
  ): Promise<CommitWire>
  awaitDelivery(token: string): Promise<void>
  readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null>
  listRetained(inc: string): Promise<LaneId[]>
  deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void>
  captureRouteGeneration(
    inc: string,
    attemptId?: string | null,
    attemptCreatedAt?: number | null,
  ): Promise<GenerationWire>
  releaseRouteGenerationCapture(attemptId: string): Promise<void>
  countRouteGenerationCaptures(): Promise<number>
  registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string,
    captureAttemptId?: string | null,
    captureCreatedAt?: number | null,
  ): Promise<RegisterWire>
  renewRoute(
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken?: string | null,
    captureAttemptId?: string | null,
    captureCreatedAt?: number | null,
  ): Promise<{ ok: boolean; expiresAt?: number; terminal?: boolean }>
  unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void>
  listGenerations(): Promise<string[]>
  dropGeneration(inc: string): Promise<DropWire>
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }>
  telefuncRoomRunMaintenance(): Promise<{ prunedRoutes: number }>
  telefuncRoomSeedOrderWatermarkForTest(inc: string, lane: LaneId, seq: number, timestamp: number): Promise<void>
  telefuncRoomReconstructForTest(): Promise<void>
  telefuncRoomDropRetainedChunksForTest(): Promise<void>
}

type SessionStub = {
  [Symbol.dispose]?: () => void
  roomCommand(serialized: string): Promise<string>
  commitLaneB64(
    roomId: string,
    inc: string,
    lane: LaneId,
    payloadB64: string,
  ): Promise<
    { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string } | { stale: true }
  >
  deliveryStatus(
    token: string,
  ): Promise<{ state: 'pending' } | { state: 'resolved' } | { state: 'rejected'; error: string }>
  contextProbe(delayMs: number): Promise<boolean>
  createSubscription(
    subscriptionId: string,
    receiverId: string,
    roomId: string,
    inc: string,
    lane: LaneId,
    command: NonNullable<ReturnType<typeof receiverDescriptor>>['command'],
  ): Promise<{ ready: true; state: ReadinessState } | { ready: false; state: ReadinessState; error: string }>
  subscriptionState(subscriptionId: string): Promise<{ state: ReadinessState; events: ReadinessState[] }>
  unsubscribeSubscription(subscriptionId: string): Promise<string | null>
  pollReceiver(
    subscriptionId: string,
    receiverId: string,
  ): Promise<Array<{ payload: string; seq: number; timestamp: number; source?: 'seed' | 'live' }>>
  releaseReceiver(subscriptionId: string, receiverId: string): Promise<void>
  seedReceiver(subscriptionId: string, receiverId: string): Promise<void>
  forceRenewalFailures(count: number): Promise<void>
  forceEstablishmentFailures(count: number): Promise<void>
  forcePostCommitEstablishmentFailures(count: number): Promise<void>
  registrationLeaseHistory(): Promise<string[]>
  forceGenerationCaptureFailures(count: number): Promise<void>
  forceInvalidationFailures(count: number): Promise<void>
  forceUnsubscribeFailures(count: number): Promise<void>
  sharedBackendOwnershipProbe(roomId: string, inc: string, delayMs: number): Promise<string>
  clearOwnershipProbes(): Promise<void>
  missingBindingSubscriptionProbe(roomId: string, inc: string): Promise<string>
  resetSessionEpoch(): Promise<void>
  advanceRenewalTimers(ms: number): Promise<void>
  disposeBackend(): Promise<void>
}

type RecoverySessionStub = {
  [Symbol.dispose]?: () => void
  prepareRecoveryChannel(channelId: string, roomId: string, inc: string): Promise<void>
  forceProductionRecoveryEpoch(): Promise<{
    ownedByCurrentManager: boolean
    before: ReadinessState | 'absent'
    after: ReadinessState | 'absent'
  }>
  teardownRecoveryChannel(): Promise<void>
  recoveryObservation(): Promise<{
    roomId: string
    inc: string
    wants: number
    readyEpochs: number
    payloads: string[]
    errors: string[]
    state: ReadinessState | 'absent'
  }>
}

type RoomNamespace = { idFromName(name: string): unknown; get(id: unknown): RoomStub }
type SessionNamespace = {
  idFromName(name: string): { toString(): string }
  idFromString(id: string): unknown
  get(id: unknown): SessionStub
}

type Shared = {
  mf: Miniflare
  url: URL
  rooms: RoomNamespace
  sessions: SessionNamespace
  recoveryRooms: RoomNamespace
  recoverySessions: {
    idFromName(name: string): unknown
    get(id: unknown): RecoverySessionStub
  }
}
let sharedPromise: Promise<Shared> | null = null
const lowLevelRoomStubs = new Set<RoomStub>()

let clockValue = 0
let clockPush: Promise<void> = Promise.resolve()

async function getShared(): Promise<Shared> {
  if (sharedPromise !== null) return sharedPromise
  sharedPromise = (async () => {
    const script = await bundleWorker()
    const mf = new Miniflare({
      modules: true,
      script,
      compatibilityDate: '2025-01-01',
      compatibilityFlags: ['nodejs_compat'],
      durableObjects: {
        ROOM: { className: 'TelefuncRoomDurableObject', useSQLite: true },
        TelefuncDurableObject: { className: 'ConformanceSessionDurableObject' },
        RecoveryRoom: { className: 'RecoveryTelefuncRoomDurableObject', useSQLite: true },
        RecoverySession: { className: 'RecoveryTelefuncDurableObject' },
      },
      kvNamespaces: ['RECOVERY_KV'],
      bindings: { TELEFUNC_ROOM_ALARM_INTERVAL_MS: '500' },
    })
    const url = await mf.ready
    const readiness = await mf.dispatchFetch('http://telefunc-room/clock/get')
    if (!readiness.ok || (await readiness.text()) !== String(clockValue)) {
      await mf.dispose()
      throw new Error('Cloudflare conformance worker failed its readiness probe')
    }
    return {
      mf,
      url,
      rooms: (await mf.getDurableObjectNamespace('ROOM')) as unknown as RoomNamespace,
      sessions: (await mf.getDurableObjectNamespace('TelefuncDurableObject')) as unknown as SessionNamespace,
      recoveryRooms: (await mf.getDurableObjectNamespace('RecoveryRoom')) as unknown as RoomNamespace,
      recoverySessions: (await mf.getDurableObjectNamespace(
        'RecoverySession',
      )) as unknown as Shared['recoverySessions'],
    }
  })()
  return sharedPromise
}

function setClock(value: number): void {
  clockValue = value
  clockPush = clockPush.then(async () => {
    const shared = await getShared()
    await shared.mf.dispatchFetch(`http://telefunc-room/clock/set?v=${value}`)
  })
}

export async function flushCloudflareAuthorityClock(): Promise<void> {
  await clockPush
}

export async function disposeSharedMiniflare(): Promise<void> {
  const pending = sharedPromise
  sharedPromise = null
  disposeCloudflareRoomStubs()
  if (pending === null) return
  try {
    await bounded((await pending).mf.dispose(), 'Cloudflare conformance Miniflare disposal')
  } catch {
    // already disposed
  }
}

export async function cloudflareRoomStub(roomId: string): Promise<RoomStub> {
  const shared = await getShared()
  await clockPush
  const stub = shared.rooms.get(shared.rooms.idFromName(roomId))
  lowLevelRoomStubs.add(stub)
  return stub
}

export async function cloudflareSessionId(seed: string): Promise<string> {
  return (await getShared()).sessions.idFromName(seed).toString()
}

export function disposeCloudflareRoomStubs(): void {
  for (const stub of lowLevelRoomStubs) stub[Symbol.dispose]?.()
  lowLevelRoomStubs.clear()
}

export async function runCloudflareSocketRecoverySchedule(wake: 'message' | 'delivery'): Promise<{
  close: { code: number; reason: string }
  observation: Awaited<ReturnType<RecoverySessionStub['recoveryObservation']>>
  oldDelivery: 'not-attempted' | 'rejected'
}> {
  const shared = await getShared()
  await clockPush
  const roomId = `recovery-${wake}-${crypto.randomUUID()}`
  const inc = crypto.randomUUID()
  const room = shared.recoveryRooms.get(shared.recoveryRooms.idFromName(roomId))
  const opened = await room.compareExchangeHead(
    { expect: 'absent' },
    { head: { currentInc: inc, state: 'open', configB64: '' } },
  )
  if (!('ok' in opened)) throw new Error(`failed to open recovery room: ${JSON.stringify(opened)}`)

  // Ask the same production routing helper, inside workerd with the same request metadata, which shard
  // recoveryTelefunc.serve() will select. This prevents an isolate-global ChannelMux from concealing a
  // test that accidentally prepares one Durable Object while the real socket lands on another.
  const sessionName = await fetch(new URL('/recovery/session-name', shared.url)).then(async (response) =>
    response.text(),
  )
  const session = shared.recoverySessions.get(shared.recoverySessions.idFromName(sessionName))
  const channelId = crypto.randomUUID()
  await session.prepareRecoveryChannel(channelId, roomId, inc)

  const NativeWebSocket = globalThis.WebSocket
  const closes: Array<{ code: number; reason: string }> = []
  const ObservedWebSocket = new Proxy(NativeWebSocket, {
    construct(target, args, newTarget) {
      const socket = Reflect.construct(target, args, newTarget) as WebSocket
      socket.addEventListener('close', (event) => closes.push({ code: event.code, reason: event.reason }))
      return socket
    },
  })
  Object.defineProperty(globalThis, 'WebSocket', { configurable: true, writable: true, value: ObservedWebSocket })

  const telefuncUrl = new URL('/_telefunc', shared.url).href
  const broadcast = new ClientBroadcast({
    channelId,
    key: `room:${roomId}`,
    transports: [CHANNEL_TRANSPORT.WS],
    telefuncUrl,
    connectionKey: channelId,
  })
  const clientRoom = new ClientRoom(broadcast, {
    channelId,
    roomId,
    meta: {},
    closed: false,
    count: 0,
    stamp: { at: 0, by: '' },
  })
  clientRoom.subscribeBinary(() => {}, { track: 'camera' })

  try {
    try {
      await waitForRecovery(async () => (await session.recoveryObservation()).readyEpochs === 1)
    } catch (error) {
      throw new Error(
        `initial Cloudflare recovery Room want did not become ready: ${JSON.stringify(await session.recoveryObservation())}`,
        { cause: error },
      )
    }
    const reset = await session.forceProductionRecoveryEpoch()
    if (!reset.ownedByCurrentManager || reset.before !== 'ready' || reset.after !== 'closed') {
      throw new Error(`production recovery reset missed the socket-owning Room manager: ${JSON.stringify(reset)}`)
    }

    let oldDelivery: 'not-attempted' | 'rejected' = 'not-attempted'
    if (wake === 'delivery') {
      const stale = await room.commitLane(roomId, inc, { kind: 'semantic' }, new TextEncoder().encode('stale'))
      if (!('deliveryToken' in stale)) throw new Error('stale recovery commit was not accepted')
      let rejected = false
      try {
        await room.awaitDelivery(stale.deliveryToken)
      } catch {
        rejected = true
      }
      if (!rejected) {
        throw new Error(
          `old recovery lease unexpectedly delivered: ${JSON.stringify({ reset, stale, closes, observation: await session.recoveryObservation() })}`,
        )
      }
      oldDelivery = 'rejected'
    }

    await waitForRecovery(() => Promise.resolve(closes.some((entry) => entry.code === 1012)))
    await waitForRecovery(async () => {
      const observation = await session.recoveryObservation()
      return observation.readyEpochs >= 2 && observation.wants >= 2 && observation.state === 'ready'
    })

    const fresh = await room.commitLane(roomId, inc, { kind: 'semantic' }, new TextEncoder().encode(`fresh-${wake}`))
    if (!('deliveryToken' in fresh)) throw new Error('fresh recovery commit was not accepted')
    await room.awaitDelivery(fresh.deliveryToken)
    const observation = await session.recoveryObservation()
    const close = closes.find((entry) => entry.code === 1012)
    if (close === undefined) throw new Error('recovery socket did not close with 1012')
    return { close, observation, oldDelivery }
  } finally {
    broadcast.abort()
    await session.teardownRecoveryChannel()
    Object.defineProperty(globalThis, 'WebSocket', { configurable: true, writable: true, value: NativeWebSocket })
    session[Symbol.dispose]?.()
    room[Symbol.dispose]?.()
  }
}

async function waitForRecovery(predicate: () => Promise<boolean>, timeoutMs: number = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error('Cloudflare socket recovery did not settle before timeout')
    await new Promise<void>((resolve) => setTimeout(resolve, 50))
  }
}

function headFromWire(wire: HeadWire): RoomHead {
  const head: RoomHead = {
    rev: wire.rev,
    currentInc: wire.currentInc,
    state: wire.state,
    config: base64ToBytes(wire.configB64),
  }
  if (wire.closeLease !== undefined) head.closeLease = { ...wire.closeLease }
  return head
}

function nextToWire(next: HeadNext): HeadNextWire {
  if ('delete' in next) return next
  const head: Extract<HeadNextWire, { head: unknown }>['head'] = {
    currentInc: next.head.currentInc,
    state: next.head.state,
    configB64: bytesToBase64(next.head.config),
  }
  if (next.head.closeLease !== undefined) head.closeLease = { ...next.head.closeLease }
  return next.ttlMs === undefined ? { head } : { head, ttlMs: next.ttlMs }
}

function parseRoomReply<T>(serialized: string): T {
  const reply = JSON.parse(serialized) as SessionRoomReply
  if (!reply.ok) throw new Error(reply.error)
  return reply.value as T
}

async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const watchdog = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} exceeded 5 seconds`)), 5_000)
  })
  try {
    return await Promise.race([promise, watchdog])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

type SubscriptionControl = {
  id: string
  receiver: LaneReceiver
  state: ReadinessState
  listeners: Set<(state: ReadinessState) => void>
}

class CloudflareConformanceBackend implements RoomBackendSpi {
  readonly spiVersion = 1 as const
  readonly capabilities = {
    receivers: 'global' as const,
    maxRetainedPayloadBytes: MAX_RETAINED_BYTES,
    clusterSafe: false,
    directory: true,
  }
  readonly #session: SessionStub
  readonly #subscriptions = new Map<string, SubscriptionControl>()
  readonly #deliverySettlements = new Map<string, Promise<void>>()
  #disposed = false
  #controlPush: Promise<void> = Promise.resolve()

  constructor(session: SessionStub) {
    this.#session = session
  }

  async #preflight(): Promise<void> {
    if (this.#disposed) throw new Error('CloudflareRoomBackend: used after dispose()')
    await bounded(
      Promise.all([clockPush, this.#controlPush]).then(() => undefined),
      'Cloudflare conformance preflight',
    )
  }

  async readHead(roomId: string) {
    await this.#preflight()
    const wire = await this.#command<{ head: HeadWire } | null>({ kind: 'read-head', roomId })
    return wire === null ? null : { head: headFromWire(wire.head) }
  }
  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext) {
    await this.#preflight()
    const wire = await this.#command<HeadCxWire>({ kind: 'compare-exchange-head', roomId, cx, next: nextToWire(next) })
    if ('conflict' in wire)
      return { conflict: true as const, current: wire.current === null ? null : headFromWire(wire.current) }
    return 'deleted' in wire
      ? { ok: true as const, deleted: true as const }
      : { ok: true as const, head: headFromWire(wire.head) }
  }
  async readCells(roomId: string, inc: string, sel: { keys: string[] } | { prefix: string }) {
    await this.#preflight()
    const wire = await this.#command<CellsWire>({ kind: 'read-cells', roomId, inc, selection: sel })
    if ('staleInc' in wire) return wire
    return { revision: wire.revision, cells: new Map(wire.cells.map(([key, value]) => [key, base64ToBytes(value)])) }
  }
  async compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]) {
    await this.#preflight()
    return this.#command<CxResult>({
      kind: 'compare-exchange-cells',
      roomId,
      inc,
      revision,
      mutations: mutations.map((mutation) =>
        mutation.set === undefined
          ? { key: mutation.key }
          : { key: mutation.key, set: { bytesB64: bytesToBase64(mutation.set.bytes), ttlMs: mutation.set.ttlMs } },
      ),
    })
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string },
  ): Promise<CommitResult> {
    await this.#preflight()
    const result = await this.#command<
      { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string } | { stale: true }
    >({ kind: 'commit-lane', roomId, inc, lane, payloadB64: bytesToBase64(payload), options: opts })
    if ('stale' in result) return result
    const delivery = this.#orderedDelivery(roomId, inc, lane, result.deliveryToken, async () => {
      await this.#pollReceivers()
      await this.#refreshStates()
    })
    void delivery.catch(() => {})
    return { accepted: true, seq: result.seq, timestamp: result.timestamp, receivers: result.receivers, delivery }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId) {
    await this.#preflight()
    const wire = await this.#command<RetainedWire | null>({ kind: 'read-retained', roomId, inc, lane })
    return wire === null ? null : { payload: base64ToBytes(wire.payloadB64), seq: wire.seq, timestamp: wire.timestamp }
  }
  async listRetained(roomId: string, inc: string) {
    await this.#preflight()
    return this.#command<LaneId[]>({ kind: 'list-retained', roomId, inc })
  }
  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }) {
    await this.#preflight()
    await this.#command<null>({ kind: 'delete-retained', roomId, inc, lane, options: opts })
  }

  async runOrderMaintenance(roomId: string): Promise<void> {
    await this.#preflight()
    await this.#command<null>({ kind: 'run-order-maintenance', roomId })
  }

  async reconstructOrderAuthority(roomId: string): Promise<void> {
    await this.#preflight()
    await this.#command<null>({ kind: 'reconstruct-order-authority', roomId })
  }

  async seedOrderWatermark(roomId: string, inc: string, lane: LaneId, seq: number, timestamp: number): Promise<void> {
    await this.#preflight()
    await this.#command<null>({ kind: 'seed-order-watermark', roomId, inc, lane, seq, timestamp })
  }

  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver): LaneSubscription {
    if (this.#disposed) throw new Error('CloudflareRoomBackend: used after dispose()')
    const descriptor = receiverDescriptor(receiver)
    if (descriptor === undefined) {
      throw new Error('Cloudflare conformance forbids Node receiver callbacks; use a receiver command')
    }
    const id = crypto.randomUUID()
    const control: SubscriptionControl = { id, receiver, state: 'establishing', listeners: new Set() }
    this.#subscriptions.set(id, control)
    bindRemoteReceiver(receiver, id, {
      poll: () => this.#session.pollReceiver(id, descriptor.id),
      release: () => this.#session.releaseReceiver(id, descriptor.id),
      seed: () => this.#session.seedReceiver(id, descriptor.id),
    })
    const ready = this.#preflight()
      .then(() => this.#session.createSubscription(id, descriptor.id, roomId, inc, lane, descriptor.command))
      .then((result) => {
        control.state = result.state
        if (!result.ready) throw new Error(result.error)
      })
    void ready.catch(() => {})
    return {
      ready,
      state: () => control.state,
      onStateChange: (listener) => {
        control.listeners.add(listener)
        return () => control.listeners.delete(listener)
      },
      unsubscribe: async () => {
        if (control.state === 'closed') return
        control.state = 'closed'
        for (const listener of [...control.listeners]) listener('closed')
        await this.#preflight()
        try {
          const error = await this.#session.unsubscribeSubscription(id)
          if (error !== null) throw new Error(error)
        } finally {
          this.#subscriptions.delete(id)
          unbindRemoteReceiver(receiver, id)
        }
      },
    }
  }

  async listGenerations(roomId: string) {
    await this.#preflight()
    return this.#command<string[]>({ kind: 'list-generations', roomId })
  }
  async dropGeneration(roomId: string, inc: string) {
    await this.#preflight()
    await this.#command<null>({ kind: 'drop-generation', roomId, inc })
    for (const key of this.#deliverySettlements.keys()) {
      const [candidateRoomId, candidateInc] = JSON.parse(key) as [string, string, string]
      if (candidateRoomId === roomId && candidateInc === inc) this.#deliverySettlements.delete(key)
    }
    await this.#refreshStates()
  }
  async directoryPut(roomId: string, incTag: string) {
    await this.#preflight()
    await this.#command<null>({ kind: 'directory-put', roomId, incTag })
  }
  async directoryDelete(roomId: string, incTag: string) {
    await this.#preflight()
    await this.#command<null>({ kind: 'directory-delete', roomId, incTag })
  }
  async directoryList(prefix: string, cursor?: string) {
    await this.#preflight()
    return this.#command<{ entries: { roomId: string; incTag: string }[]; cursor?: string }>({
      kind: 'directory-list',
      prefix,
      cursor,
    })
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    await bounded(
      Promise.allSettled([...this.#deliverySettlements.values()]).then(() => undefined),
      'Cloudflare conformance delivery drain',
    )
    await bounded(this.#session.disposeBackend(), 'Cloudflare conformance backend disposal')
    this.#session[Symbol.dispose]?.()
    this.#subscriptions.clear()
    this.#deliverySettlements.clear()
  }

  forceRenewalFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceRenewalFailures(count))
  }
  forceEstablishmentFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceEstablishmentFailures(count))
  }
  forcePostCommitEstablishmentFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forcePostCommitEstablishmentFailures(count))
  }
  async registrationLeaseHistory(): Promise<string[]> {
    await this.#controlPush
    return this.#session.registrationLeaseHistory()
  }
  forceGenerationCaptureFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceGenerationCaptureFailures(count))
  }
  forceInvalidationFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceInvalidationFailures(count))
  }
  forceUnsubscribeFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceUnsubscribeFailures(count))
  }
  async resetSessionEpoch(): Promise<void> {
    await this.#preflight()
    await this.#session.resetSessionEpoch()
    for (const control of this.#subscriptions.values()) {
      control.state = 'closed'
      for (const listener of [...control.listeners]) listener('closed')
    }
    this.#subscriptions.clear()
    this.#deliverySettlements.clear()
  }
  async advanceRenewalTimers(ms: number): Promise<void> {
    await this.#controlPush
    await this.#session.advanceRenewalTimers(ms)
    await this.#refreshStates()
  }

  async #pollReceivers(): Promise<void> {
    await Promise.all([...this.#subscriptions.values()].map((entry) => pollRemoteReceiver(entry.receiver)))
  }

  async #refreshStates(): Promise<void> {
    for (const control of this.#subscriptions.values()) {
      const update = await this.#session.subscriptionState(control.id)
      control.state = update.state
      for (const event of update.events) for (const listener of [...control.listeners]) listener(event)
    }
  }

  async commitFromSession(roomId: string, inc: string, lane: LaneId, payload: Uint8Array): Promise<CommitResult> {
    await this.#preflight()
    const result = await this.#session.commitLaneB64(roomId, inc, lane, bytesToBase64(payload))
    if ('stale' in result) return result
    const delivery = this.#orderedDelivery(roomId, inc, lane, result.deliveryToken, async () => {
      await this.#pollReceivers()
      await this.#refreshStates()
    })
    void delivery.catch(() => {})
    return { accepted: true, seq: result.seq, timestamp: result.timestamp, receivers: result.receivers, delivery }
  }

  probeContext(delayMs: number): Promise<boolean> {
    return this.#session.contextProbe(delayMs)
  }

  probeSharedBackendOwnership(roomId: string, inc: string, delayMs: number): Promise<string> {
    return this.#session.sharedBackendOwnershipProbe(roomId, inc, delayMs)
  }

  clearOwnershipProbes(): Promise<void> {
    return this.#session.clearOwnershipProbes()
  }

  missingBindingSubscriptionProbe(roomId: string, inc: string): Promise<string> {
    return this.#session.missingBindingSubscriptionProbe(roomId, inc)
  }

  async #command<T>(command: SessionRoomCommand): Promise<T> {
    const result = parseRoomReply<T>(
      await bounded(
        this.#session.roomCommand(JSON.stringify(command)),
        `Cloudflare conformance command '${command.kind}'`,
      ),
    )
    return result
  }

  async #awaitDelivery(token: string): Promise<void> {
    for (;;) {
      // Yield outside workerd before every observation so a burst of Node-side status queries cannot
      // starve the worker-owned delivery callback that changes the status.
      await new Promise<void>((resolve) => setTimeout(resolve, 50))
      const status = await this.#session.deliveryStatus(token)
      if (status.state === 'resolved') return
      if (status.state === 'rejected') throw new Error(status.error)
    }
  }

  #orderedDelivery(
    roomId: string,
    inc: string,
    lane: LaneId,
    token: string,
    observe: () => Promise<void>,
  ): Promise<void> {
    const key = JSON.stringify([roomId, inc, lane])
    const attempt = this.#awaitDelivery(token)
    void attempt.catch(() => {})
    const previous = this.#deliverySettlements.get(key) ?? Promise.resolve()
    const delivery = previous.then(async () => {
      try {
        await attempt
      } finally {
        await observe()
      }
    })
    const tail = delivery.then(
      () => undefined,
      () => undefined,
    )
    this.#deliverySettlements.set(key, tail)
    void tail.then(() => {
      if (this.#deliverySettlements.get(key) === tail) this.#deliverySettlements.delete(key)
    })
    return delivery
  }
}

export function cloudflareRenewalControls(backend: RoomBackendSpi): {
  forceFailures(count: number): void
  forceEstablishmentFailures(count: number): void
  forcePostCommitEstablishmentFailures(count: number): void
  registrationLeaseHistory(): Promise<string[]>
  forceGenerationCaptureFailures(count: number): void
  forceInvalidationFailures(count: number): void
  forceUnsubscribeFailures(count: number): void
  resetSessionEpoch(): Promise<void>
  advance(ms: number): Promise<void>
} {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return {
    forceFailures: (count) => backend.forceRenewalFailures(count),
    forceEstablishmentFailures: (count) => backend.forceEstablishmentFailures(count),
    forcePostCommitEstablishmentFailures: (count) => backend.forcePostCommitEstablishmentFailures(count),
    registrationLeaseHistory: () => backend.registrationLeaseHistory(),
    forceGenerationCaptureFailures: (count) => backend.forceGenerationCaptureFailures(count),
    forceInvalidationFailures: (count) => backend.forceInvalidationFailures(count),
    forceUnsubscribeFailures: (count) => backend.forceUnsubscribeFailures(count),
    resetSessionEpoch: () => backend.resetSessionEpoch(),
    advance: (ms) => backend.advanceRenewalTimers(ms),
  }
}

export async function cloudflareCommitFromSession(
  backend: RoomBackendSpi,
  roomId: string,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
): Promise<CommitResult> {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return backend.commitFromSession(roomId, inc, lane, payload)
}

export function cloudflareContextProbe(backend: RoomBackendSpi, delayMs: number): Promise<boolean> {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return backend.probeContext(delayMs)
}

export function cloudflareSharedBackendOwnershipProbe(
  backend: RoomBackendSpi,
  roomId: string,
  inc: string,
  delayMs: number,
): Promise<string> {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return backend.probeSharedBackendOwnership(roomId, inc, delayMs)
}

export function cloudflareClearOwnershipProbes(backend: RoomBackendSpi): Promise<void> {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return backend.clearOwnershipProbes()
}

export function cloudflareMissingBindingSubscriptionProbe(
  backend: RoomBackendSpi,
  roomId: string,
  inc: string,
): Promise<string> {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return backend.missingBindingSubscriptionProbe(roomId, inc)
}

export const cloudflareHarness: BackendHarness = {
  name: 'cloudflare',
  async create(): Promise<BackendFixture> {
    const shared = await getShared()
    setClock(Date.now())
    await bounded(clockPush, 'Cloudflare conformance initial clock push')
    const id = shared.sessions.idFromName(`conformance:${crypto.randomUUID()}`)
    const backend = new CloudflareConformanceBackend(shared.sessions.get(id))
    return {
      backend,
      traces: { handoffAwaitsReceiver: true, perTargetFailure: true, cxAppliesSynchronously: false },
      expectedReceivers: { twoLocalSubscriptionsSameLane: 1, oneLocalSubscriptionAfterSiblingDetach: 1 },
      authorityNow: () => clockValue,
      advanceAuthority: (ms) => setClock(clockValue + ms),
      orderControl: {
        setAuthority: (now) => setClock(now),
        runMaintenance: (roomId) => backend.runOrderMaintenance(roomId),
        reconstructBackend: (roomId) => backend.reconstructOrderAuthority(roomId),
        seedWatermark: (roomId, inc, lane, seq, timestamp) =>
          backend.seedOrderWatermark(roomId, inc, lane, seq, timestamp),
      },
      concurrentHeadCxBarrier: async <T>(first: () => Promise<T>, second: () => Promise<T>) => {
        const firstPromise = first()
        const secondPromise = second()
        return Promise.all([firstPromise, secondPromise])
      },
      dispose: () => backend.dispose(),
    }
  },
}
