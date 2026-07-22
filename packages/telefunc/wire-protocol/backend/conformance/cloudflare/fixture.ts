// Node-side control plane for local workerd conformance. All Room SPI calls and receiver callbacks run
// in ConformanceSessionDurableObject through the production Cloudflare backend/session manager. This
// file holds only serializable command handles and immutable observation caches.

import { Miniflare } from 'miniflare'
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
import { bindRemoteReceiver, pollRemoteReceiver, receiverDescriptor } from '../receiver.js'
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
    opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string },
  ): Promise<CommitWire>
  awaitDelivery(token: string): Promise<void>
  readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null>
  listRetained(inc: string): Promise<LaneId[]>
  deleteRetainedLane(inc: string, lane?: LaneId): Promise<void>
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
  unsubscribeSubscription(subscriptionId: string): Promise<void>
  pollReceiver(
    subscriptionId: string,
    receiverId: string,
  ): Promise<Array<{ payload: string; seq: number; timestamp: number; source?: 'seed' | 'live' }>>
  releaseReceiver(subscriptionId: string, receiverId: string): Promise<void>
  seedReceiver(subscriptionId: string, receiverId: string): Promise<void>
  forceRenewalFailures(count: number): Promise<void>
  forceEstablishmentFailures(count: number): Promise<void>
  forceGenerationCaptureFailures(count: number): Promise<void>
  forceInvalidationFailures(count: number): Promise<void>
  resetSessionEpoch(): Promise<void>
  advanceRenewalTimers(ms: number): Promise<void>
  disposeBackend(): Promise<void>
}

type RoomNamespace = { idFromName(name: string): unknown; get(id: unknown): RoomStub }
type SessionNamespace = {
  idFromName(name: string): { toString(): string }
  idFromString(id: string): unknown
  get(id: unknown): SessionStub
}

type Shared = { mf: Miniflare; rooms: RoomNamespace; sessions: SessionNamespace }
let sharedPromise: Promise<Shared> | null = null
const lowLevelRoomStubs = new Set<RoomStub>()

let clockValue = 0
let clockPush: Promise<void> = Promise.resolve()

async function getShared(): Promise<Shared> {
  if (sharedPromise !== null) return sharedPromise
  sharedPromise = (async () => {
    const mf = new Miniflare({
      modules: true,
      script: await bundleWorker(),
      compatibilityDate: '2025-01-01',
      compatibilityFlags: ['nodejs_compat'],
      durableObjects: {
        ROOM: { className: 'TelefuncRoomDurableObject', useSQLite: true },
        TelefuncDurableObject: { className: 'ConformanceSessionDurableObject' },
      },
      bindings: { TELEFUNC_ROOM_ALARM_INTERVAL_MS: '500' },
    })
    return {
      mf,
      rooms: (await mf.getDurableObjectNamespace('ROOM')) as unknown as RoomNamespace,
      sessions: (await mf.getDurableObjectNamespace('TelefuncDurableObject')) as unknown as SessionNamespace,
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
    await (await pending).mf.dispose()
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
    await Promise.all([clockPush, this.#controlPush])
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
    opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string },
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
  async deleteRetained(roomId: string, inc: string, lane?: LaneId) {
    await this.#preflight()
    await this.#command<null>({ kind: 'delete-retained', roomId, inc, lane })
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
    bindRemoteReceiver(receiver, {
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
        await this.#session.unsubscribeSubscription(id)
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
    await this.#session.disposeBackend()
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
  forceGenerationCaptureFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceGenerationCaptureFailures(count))
  }
  forceInvalidationFailures(count: number): void {
    this.#controlPush = this.#controlPush.then(() => this.#session.forceInvalidationFailures(count))
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

  async #command<T>(command: SessionRoomCommand): Promise<T> {
    return parseRoomReply<T>(await this.#session.roomCommand(JSON.stringify(command)))
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
  forceGenerationCaptureFailures(count: number): void
  forceInvalidationFailures(count: number): void
  resetSessionEpoch(): Promise<void>
  advance(ms: number): Promise<void>
} {
  if (!(backend instanceof CloudflareConformanceBackend)) throw new Error('expected Cloudflare conformance backend')
  return {
    forceFailures: (count) => backend.forceRenewalFailures(count),
    forceEstablishmentFailures: (count) => backend.forceEstablishmentFailures(count),
    forceGenerationCaptureFailures: (count) => backend.forceGenerationCaptureFailures(count),
    forceInvalidationFailures: (count) => backend.forceInvalidationFailures(count),
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

export const cloudflareHarness: BackendHarness = {
  name: 'cloudflare',
  async create(): Promise<BackendFixture> {
    const shared = await getShared()
    setClock(Date.now())
    await clockPush
    const id = shared.sessions.idFromName(`conformance:${crypto.randomUUID()}`)
    const backend = new CloudflareConformanceBackend(shared.sessions.get(id))
    return {
      backend,
      traces: { handoffAwaitsReceiver: true, perTargetFailure: true, cxAppliesSynchronously: false },
      expectedReceivers: { twoLocalSubscriptionsSameLane: 1, oneLocalSubscriptionAfterSiblingDetach: 1 },
      authorityNow: () => clockValue,
      advanceAuthority: (ms) => setClock(clockValue + ms),
      concurrentHeadCxBarrier: async <T>(first: () => Promise<T>, second: () => Promise<T>) => {
        const firstPromise = first()
        const secondPromise = second()
        return Promise.all([firstPromise, secondPromise])
      },
      dispose: () => backend.dispose(),
    }
  },
}
