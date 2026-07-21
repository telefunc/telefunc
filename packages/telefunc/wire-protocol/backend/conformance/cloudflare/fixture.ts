// The local-workerd conformance fixture: a `RoomBackendSpi` facade in Node that drives the real
// `TelefuncRoomDurableObject` running inside Miniflare/workerd (real DO SQLite), relays delivery back to
// the test's receiver closures through a Node service binding, and controls the DO's authority clock
// through the DO's own `:now` seam. Registered into the conformance harness so every I1–I13 scenario runs
// against workerd with identical outcomes.
//
// NOT compiled by tsc (the cloudflare conformance dir is excluded); imports Miniflare + esbuild, which are
// Node-only test tooling.

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
  RoomBackendSpi,
  RoomHead,
} from '../../spi.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from '../../../server/adapter/cloudflare/room/codec.js'
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
import {
  CloudflareLaneSubscription,
  CloudflareLaneSubscriptionMultiplexer,
  realSubscriptionScheduler,
  type SubscriptionScheduler,
} from '../../../server/adapter/cloudflare/room/subscription.js'
import type {
  SubscriberDeliveryRequest,
  SubscriberRouteIdentity,
} from '../../../server/adapter/cloudflare/room/subscriber.js'
import type { BackendFixture, BackendHarness } from '../harness.js'
import { bundleWorker } from './bundle.js'

const DIRECTORY_DO_NAME = '__telefunc_room_directory__'
const MAX_RETAINED_BYTES = 16 * 1024 * 1024

// A DO stub's RPC surface, as this facade uses it.
type RoomStub = {
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
  captureRouteGeneration(inc: string, attemptId?: string | null): Promise<GenerationWire>
  releaseRouteGenerationCapture(attemptId: string): Promise<void>
  registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriber: string,
    leaseId: string,
    bucket: string | null,
    expectedGenerationToken: string,
  ): Promise<RegisterWire>
  renewRoute(
    inc: string,
    laneKey: string,
    subscriber: string,
    leaseId: string,
    expectedGenerationToken?: string | null,
  ): Promise<{ ok: boolean; expiresAt?: number; terminal?: boolean }>
  unsubscribeRoute(inc: string, laneKey: string, subscriber: string, leaseId: string): Promise<void>
  listGenerations(): Promise<string[]>
  dropGeneration(inc: string): Promise<DropWire>
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }>
  runJanitor(): Promise<{ prunedRoutes: number }>
}

export type { RoomStub }
type Namespace = { idFromName(name: string): unknown; get(id: unknown): RoomStub }
type SubscriberStub = {
  [Symbol.dispose]?: () => void
  installRoute(identity: SubscriberRouteIdentity, leaseId: string): Promise<void>
  uninstallRoute(identity: SubscriberRouteIdentity, leaseId: string): Promise<void>
  failUninstalls(count: number): Promise<void>
  holdDeliveries(): Promise<void>
  releaseDeliveries(): Promise<void>
  waitForDelivery(): Promise<void>
  holdProbes(): Promise<void>
  releaseProbes(): Promise<void>
  waitForProbe(): Promise<void>
  telefuncBroadcastDeliver(request: SubscriberDeliveryRequest): Promise<void>
}
type SubscriberNamespace = { idFromName(name: string): unknown; get(id: unknown): SubscriberStub }

// ── shared per-process runtime (one Miniflare, reused across the file's tests) ──

type Shared = {
  mf: Miniflare
  ns: Namespace
  subscriberNs: SubscriberNamespace
}
let sharedPromise: Promise<Shared> | null = null
// Fully-scoped identity → live Node receiver; the service binding is only the subscriber DO's local relay.
const receivers = new Map<string, Set<LaneReceiver>>()
const lowLevelStubs = new Set<RoomStub>()
const lowLevelSubscriberStubs = new Set<SubscriberStub>()

function disposeRoomStub(stub: RoomStub): void {
  stub[Symbol.dispose]?.()
}

// The controlled authority clock: a logical number kept identical on both sides (this is what makes the
// exact `until === authorityNow() + durationMs` assertions hold). advanceAuthority is synchronous, so the
// push to workerd is chained and every RPC awaits the pending push first.
let clockValue = 0
let clockPush: Promise<void> = Promise.resolve()

async function pushClock(value: number, mf: Miniflare): Promise<void> {
  await mf.dispatchFetch(`http://telefunc-room/clock/set?v=${value}`)
}

function setClock(value: number): void {
  clockValue = value
  clockPush = clockPush.then(async () => {
    const shared = await getShared()
    await pushClock(value, shared.mf)
  })
}

// The handoff to one subscriber: invoke its live local receiver and, since the receiver is typed
// `=> void`, await a thenable return (the stalling-receiver trace). A throw rejects THIS frame's delivery
// (per-target failure). A vanished receiver (unsubscribed mid-flight) is skipped, at-most-once. This is
// the subscriber-isolate side of the CF handoff: the room DO's fan-out RPC target, collapsed here to the
// in-process receiver the same way production would dispatch inside the subscriber DO.
function receiverKey(identity: SubscriberRouteIdentity): string {
  return JSON.stringify([identity.roomId, identity.inc, identity.laneKey, identity.subscriber])
}

type DeliveryRelayWire = SubscriberRouteIdentity & {
  payloadB64: string
  seq: number
  timestamp: number
}

async function deliveryRelayHandler(request: Request): Promise<Response> {
  const wire = (await request.json()) as DeliveryRelayWire
  const local = receivers.get(receiverKey(wire))
  if (local === undefined) return new Response('gone', { status: 410 })
  const payload = base64ToBytes(wire.payloadB64)
  const attempts = [...local].map(async (receiver) => {
    const result = receiver(new Uint8Array(payload), { seq: wire.seq, timestamp: wire.timestamp }) as unknown
    if (result !== null && typeof result === 'object' && typeof (result as { then?: unknown }).then === 'function') {
      await result
    }
  })
  try {
    await Promise.all(attempts)
    return new Response('ok')
  } catch {
    return new Response('receiver-threw', { status: 500 })
  }
}

function addReceiver(identity: SubscriberRouteIdentity, receiver: LaneReceiver): void {
  const key = receiverKey(identity)
  let local = receivers.get(key)
  if (local === undefined) {
    local = new Set<LaneReceiver>()
    receivers.set(key, local)
  }
  local.add(receiver)
}

function removeReceiver(identity: SubscriberRouteIdentity, receiver?: LaneReceiver): boolean {
  const key = receiverKey(identity)
  const local = receivers.get(key)
  if (local === undefined) return true
  if (receiver === undefined) local.clear()
  else local.delete(receiver)
  if (local.size > 0) return false
  receivers.delete(key)
  return true
}

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
        TELEFUNC_ROOM_SUBSCRIBER: { className: 'TelefuncRoomSubscriberDurableObject' },
      },
      bindings: { TELEFUNC_ROOM_ALARM_INTERVAL_MS: '500' },
      serviceBindings: {
        TELEFUNC_ROOM_DELIVERY_RELAY: deliveryRelayHandler,
      },
    })
    const ns = (await mf.getDurableObjectNamespace('ROOM')) as unknown as Namespace
    const subscriberNs = (await mf.getDurableObjectNamespace(
      'TELEFUNC_ROOM_SUBSCRIBER',
    )) as unknown as SubscriberNamespace
    return { mf, ns, subscriberNs }
  })()
  return sharedPromise
}

export async function disposeSharedMiniflare(): Promise<void> {
  const pending = sharedPromise
  sharedPromise = null
  receivers.clear()
  for (const stub of lowLevelStubs) disposeRoomStub(stub)
  lowLevelStubs.clear()
  for (const stub of lowLevelSubscriberStubs) stub[Symbol.dispose]?.()
  lowLevelSubscriberStubs.clear()
  if (pending === null) return
  try {
    const shared = await pending
    await shared.mf.dispose()
  } catch {
    // already gone
  }
}

// ── low-level primitives for the CF-specific scenarios (route lease lifecycle, eviction, barrier race) ──
// These reach past the SPI facade to drive the route table and delivery chain directly, which is where the
// CF-specific properties (readiness-ordering §2.3/§3) live.

export async function cloudflareRoomStub(roomId: string): Promise<RoomStub> {
  const shared = await getShared()
  await clockPush
  const stub = shared.ns.get(shared.ns.idFromName(roomId))
  lowLevelStubs.add(stub)
  return stub
}

export function disposeCloudflareRoomStubs(): void {
  for (const stub of lowLevelStubs) disposeRoomStub(stub)
  lowLevelStubs.clear()
  for (const stub of lowLevelSubscriberStubs) stub[Symbol.dispose]?.()
  lowLevelSubscriberStubs.clear()
}

async function subscriberStub(subscriber: string): Promise<SubscriberStub> {
  const shared = await getShared()
  const stub = shared.subscriberNs.get(shared.subscriberNs.idFromName(subscriber))
  lowLevelSubscriberStubs.add(stub)
  return stub
}

export async function installReceiver(
  roomId: string,
  inc: string,
  laneKey: string,
  subscriber: string,
  leaseId: string,
  receiver: LaneReceiver,
): Promise<void> {
  const identity = { roomId, inc, laneKey, subscriber }
  addReceiver(identity, receiver)
  await (await subscriberStub(subscriber)).installRoute(identity, leaseId)
}

export async function evictReceiver(
  roomId: string,
  inc: string,
  laneKey: string,
  subscriber: string,
  leaseId: string,
): Promise<void> {
  const identity = { roomId, inc, laneKey, subscriber }
  removeReceiver(identity)
  await (await subscriberStub(subscriber)).uninstallRoute(identity, leaseId)
}

export async function holdReceiverDeliveries(subscriber: string): Promise<void> {
  await (await subscriberStub(subscriber)).holdDeliveries()
}

export async function releaseReceiverDeliveries(subscriber: string): Promise<void> {
  await (await subscriberStub(subscriber)).releaseDeliveries()
}

export async function waitForReceiverDelivery(subscriber: string): Promise<void> {
  await (await subscriberStub(subscriber)).waitForDelivery()
}

export async function holdReceiverProbes(subscriber: string): Promise<void> {
  await (await subscriberStub(subscriber)).holdProbes()
}

export async function releaseReceiverProbes(subscriber: string): Promise<void> {
  await (await subscriberStub(subscriber)).releaseProbes()
}

export async function waitForReceiverProbe(subscriber: string): Promise<void> {
  await (await subscriberStub(subscriber)).waitForProbe()
}

export async function failReceiverUninstalls(subscriber: string, count: number): Promise<void> {
  await (await subscriberStub(subscriber)).failUninstalls(count)
}

export async function flushCloudflareAuthorityClock(): Promise<void> {
  await clockPush
}

// ── head marshalling ──

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
  if ('delete' in next) return { delete: true }
  const head: Extract<HeadNextWire, { head: unknown }>['head'] = {
    currentInc: next.head.currentInc,
    state: next.head.state,
    configB64: bytesToBase64(next.head.config),
  }
  if (next.head.closeLease !== undefined) head.closeLease = { ...next.head.closeLease }
  return next.ttlMs === undefined ? { head } : { head, ttlMs: next.ttlMs }
}

// ── the readiness lifecycle, facade side ──

class ManualRenewalScheduler implements SubscriptionScheduler {
  #now = 0
  #sequence = 0
  readonly #tasks = new Map<number, { at: number; task: () => Promise<void> }>()

  schedule(delayMs: number, task: () => Promise<void>): () => void {
    const id = ++this.#sequence
    this.#tasks.set(id, { at: this.#now + delayMs, task })
    return () => this.#tasks.delete(id)
  }

  async advance(ms: number): Promise<void> {
    const target = this.#now + ms
    for (;;) {
      let next: { id: number; at: number; task: () => Promise<void> } | null = null
      for (const [id, entry] of this.#tasks) {
        if (entry.at <= target && (next === null || entry.at < next.at || (entry.at === next.at && id < next.id))) {
          next = { id, ...entry }
        }
      }
      if (next === null) break
      this.#tasks.delete(next.id)
      this.#now = next.at
      await next.task()
    }
    this.#now = target
  }

  clear(): void {
    this.#tasks.clear()
  }
}

// ── the backend facade ──

class CloudflareRoomBackend implements RoomBackendSpi {
  readonly spiVersion = 1 as const
  readonly capabilities = {
    receivers: 'global' as const,
    maxRetainedPayloadBytes: MAX_RETAINED_BYTES,
    clusterSafe: false,
    directory: true,
  }

  readonly #ns: Namespace
  readonly #subscriberNs: SubscriberNamespace
  readonly #renewalScheduler: SubscriptionScheduler
  readonly #representative = crypto.randomUUID()
  readonly #stubs = new Map<string, RoomStub>()
  readonly #subscriberStubs = new Map<string, SubscriberStub>()
  #disposed = false
  #forcedRenewalFailures = 0
  #forcedEstablishmentFailures = 0
  #forcedGenerationCaptureFailures = 0
  // Fully-scoped local ownership; one shared route lifecycle sits behind each local callback mux.
  // Delivery chains themselves live only in the room DO.
  readonly #muxes = new Map<string, { identity: SubscriberRouteIdentity; mux: CloudflareLaneSubscriptionMultiplexer }>()

  constructor(
    ns: Namespace,
    subscriberNs: SubscriberNamespace,
    renewalScheduler: SubscriptionScheduler = realSubscriptionScheduler,
  ) {
    this.#ns = ns
    this.#subscriberNs = subscriberNs
    this.#renewalScheduler = renewalScheduler
  }

  forceRenewalFailures(count: number): void {
    this.#forcedRenewalFailures = count
  }

  forceEstablishmentFailures(count: number): void {
    this.#forcedEstablishmentFailures = count
  }

  forceGenerationCaptureFailures(count: number): void {
    this.#forcedGenerationCaptureFailures = count
  }

  async advanceRenewalTimers(ms: number): Promise<void> {
    if (!(this.#renewalScheduler instanceof ManualRenewalScheduler)) {
      throw new Error('advanceRenewalTimers requires the conformance scheduler')
    }
    await this.#renewalScheduler.advance(ms)
  }

  #assertLive(): void {
    if (this.#disposed) throw new Error('CloudflareRoomBackend: used after dispose()')
  }

  #stub(roomId: string): RoomStub {
    let stub = this.#stubs.get(roomId)
    if (stub === undefined) {
      stub = this.#ns.get(this.#ns.idFromName(roomId))
      this.#stubs.set(roomId, stub)
    }
    return stub
  }

  #subscriberName(roomId: string): string {
    return `room:${encodeURIComponent(roomId)}:isolate:${this.#representative}`
  }

  #subscriberStub(subscriber: string): SubscriberStub {
    let stub = this.#subscriberStubs.get(subscriber)
    if (stub === undefined) {
      stub = this.#subscriberNs.get(this.#subscriberNs.idFromName(subscriber))
      this.#subscriberStubs.set(subscriber, stub)
    }
    return stub
  }

  #directory(): RoomStub {
    return this.#ns.get(this.#ns.idFromName(DIRECTORY_DO_NAME))
  }

  async #preflight(): Promise<void> {
    this.#assertLive()
    await clockPush
  }

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    await this.#preflight()
    const wire = await this.#stub(roomId).readHead()
    return wire === null ? null : { head: headFromWire(wire) }
  }

  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    await this.#preflight()
    const wire = await this.#stub(roomId).compareExchangeHead(cx, nextToWire(next))
    if ('error' in wire) throw new Error(wire.error)
    if ('conflict' in wire)
      return { conflict: true, current: wire.current === null ? null : headFromWire(wire.current) }
    if ('deleted' in wire) return { ok: true, deleted: true }
    return { ok: true, head: headFromWire(wire.head) }
  }

  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    await this.#preflight()
    const wire = await this.#stub(roomId).readCells(inc, sel)
    if ('staleInc' in wire) return { staleInc: true }
    const cells = new Map<string, Uint8Array>()
    for (const [key, b64] of wire.cells) cells.set(key, base64ToBytes(b64))
    return { revision: wire.revision, cells }
  }

  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    await this.#preflight()
    const wire = mutations.map((mutation) =>
      mutation.set === undefined
        ? { key: mutation.key }
        : { key: mutation.key, set: { bytesB64: bytesToBase64(mutation.set.bytes), ttlMs: mutation.set.ttlMs } },
    )
    return this.#stub(roomId).compareExchangeCells(inc, revision, wire)
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string },
  ): Promise<CommitResult> {
    await this.#preflight()
    const stub = this.#stub(roomId)
    const wire = await stub.commitLane(roomId, inc, lane, payload, opts)
    if ('error' in wire) throw new Error(wire.error)
    if ('stale' in wire) return { stale: true }
    // Start observing the authority-hosted attempt immediately; the caller may await the returned promise
    // later without leaving an unobserved token in the room DO.
    const delivery = stub.awaitDelivery(wire.deliveryToken)
    // Mark the rejection observed so a delivery the caller awaits only later (a failed handoff) is never a
    // spurious unhandled rejection; the caller still sees it when it awaits (promises fan out to handlers).
    void delivery.catch(() => {})
    return { accepted: true, seq: wire.seq, timestamp: wire.timestamp, receivers: wire.receivers, delivery }
  }

  async readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null> {
    await this.#preflight()
    const wire = await this.#stub(roomId).readRetained(inc, lane)
    return wire === null ? null : { payload: base64ToBytes(wire.payloadB64), seq: wire.seq, timestamp: wire.timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    await this.#preflight()
    return this.#stub(roomId).listRetained(inc)
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId): Promise<void> {
    await this.#preflight()
    await this.#stub(roomId).deleteRetainedLane(inc, lane)
  }

  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver): LaneSubscription {
    this.#assertLive()
    const laneKey = laneKeyOf(lane)
    const subscriber = this.#subscriberName(roomId)
    const identity: SubscriberRouteIdentity = { roomId, inc, laneKey, subscriber }
    const muxKey = receiverKey(identity)
    const existing = this.#muxes.get(muxKey)
    addReceiver(identity, receiver)
    if (existing !== undefined) {
      return existing.mux.attach(() => {
        removeReceiver(identity, receiver)
      })
    }

    let leaseId = crypto.randomUUID()
    let generationToken: string | null = null
    const generationCaptureAttemptId = crypto.randomUUID()
    let mux!: CloudflareLaneSubscriptionMultiplexer
    const sharedSubscription = new CloudflareLaneSubscription(
      {
        establish: async () => {
          await clockPush
          let captured: GenerationWire
          try {
            captured = await this.#stub(roomId).captureRouteGeneration(inc, generationCaptureAttemptId)
            if (this.#forcedGenerationCaptureFailures > 0) {
              this.#forcedGenerationCaptureFailures -= 1
              throw new Error('forced generation-capture transport failure')
            }
          } catch (error) {
            return {
              ready: false,
              // Transport loss is recoverable even before the first token response. A structured
              // authority rejection/token mismatch below remains terminal.
              retryable: true,
              reason: (error as Error).message,
            }
          }
          if ('rejected' in captured) {
            return { ready: false, retryable: false, reason: captured.reason }
          }
          if (generationToken === null) generationToken = captured.generationToken
          if (generationToken !== captured.generationToken) {
            return { ready: false, retryable: false, reason: `generation '${inc}' was invalidated` }
          }
          const attemptLeaseId = crypto.randomUUID()
          leaseId = attemptLeaseId
          // The subscriber isolate installs its local mux identity BEFORE any room RPC.
          await this.#subscriberStub(subscriber).installRoute(identity, attemptLeaseId)
          if (this.#forcedEstablishmentFailures > 0) {
            this.#forcedEstablishmentFailures -= 1
            throw new Error('forced establishment transport failure')
          }
          const result = await this.#stub(roomId).registerRoute(
            roomId,
            inc,
            laneKey,
            subscriber,
            attemptLeaseId,
            null,
            generationToken,
          )
          if ('ok' in result) {
            return { ready: true }
          }
          return {
            ready: false,
            retryable: result.terminal !== true && result.reason.includes('not addressable'),
            reason: result.reason,
          }
        },
        renew: async () => {
          const renewalLeaseId = leaseId
          const renewalGenerationToken = generationToken
          await clockPush
          if (this.#forcedRenewalFailures > 0) {
            this.#forcedRenewalFailures -= 1
            return false
          }
          const result = await this.#stub(roomId).renewRoute(
            inc,
            laneKey,
            subscriber,
            renewalLeaseId,
            renewalGenerationToken,
          )
          return {
            renewed: result.ok,
            terminal: result.terminal,
            reason: result.terminal === true ? `generation '${inc}' was invalidated` : undefined,
          }
        },
        validate: async () => {
          const validationLeaseId = leaseId
          const validationGenerationToken = generationToken
          await clockPush
          const result = await this.#stub(roomId).renewRoute(
            inc,
            laneKey,
            subscriber,
            validationLeaseId,
            validationGenerationToken,
          )
          return {
            renewed: result.ok,
            terminal: result.terminal,
            reason: result.terminal === true ? `generation '${inc}' was invalidated` : undefined,
          }
        },
        unsubscribe: async () => {
          const retiringLeaseId = leaseId
          await clockPush
          let failure: unknown
          try {
            await this.#stub(roomId).unsubscribeRoute(inc, laneKey, subscriber, retiringLeaseId)
          } catch (error) {
            failure = error
          }
          try {
            await this.#subscriberStub(subscriber).uninstallRoute(identity, retiringLeaseId)
          } catch (error) {
            failure ??= error
          }
          try {
            await this.#stub(roomId).releaseRouteGenerationCapture(generationCaptureAttemptId)
          } catch {
            // The lifecycle is already terminal; an idempotency-fence cleanup transport failure is
            // hygiene-only and must not replace the route teardown result.
          }
          if (failure !== undefined) throw failure
        },
        closed: () => {
          const closedLeaseId = leaseId
          mux.lifecycleClosed(new Error('shared subscription lifecycle closed'))
          void this.#subscriberStub(subscriber)
            .uninstallRoute(identity, closedLeaseId)
            .catch(() => {})
          void this.#stub(roomId)
            .releaseRouteGenerationCapture(generationCaptureAttemptId)
            .catch(() => {})
        },
      },
      { scheduler: this.#renewalScheduler, jitter: () => 0.5 },
    )
    mux = new CloudflareLaneSubscriptionMultiplexer(sharedSubscription, () => {
      if (this.#muxes.get(muxKey)?.mux === mux) this.#muxes.delete(muxKey)
    })
    this.#muxes.set(muxKey, { identity, mux })
    const attachment = mux.attach(() => {
      removeReceiver(identity, receiver)
    })
    sharedSubscription.start()
    return attachment
  }

  async listGenerations(roomId: string): Promise<string[]> {
    await this.#preflight()
    return this.#stub(roomId).listGenerations()
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    await this.#preflight()
    const wire = await this.#stub(roomId).dropGeneration(inc)
    if ('error' in wire) throw new Error(wire.error)
    // The dropped incarnation's delivery chains never continue into a recreation.
    // Close the local subscriptions whose generation just vanished — their channel is terminal.
    for (const [laneKey, subscriber] of wire.droppedSubscribers) {
      for (const entry of this.#muxes.values()) {
        if (
          entry.identity.roomId === roomId &&
          entry.identity.inc === inc &&
          entry.identity.laneKey === laneKey &&
          entry.identity.subscriber === subscriber
        ) {
          entry.mux.generationDropped()
        }
      }
    }
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    await this.#preflight()
    await this.#directory().directoryPut(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    await this.#preflight()
    await this.#directory().directoryDelete(roomId, incTag)
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    await this.#preflight()
    return this.#directory().directoryList(prefix, cursor)
  }

  async dispose(): Promise<void> {
    this.#disposed = true
    for (const entry of this.#muxes.values()) {
      entry.mux.backendDisposed()
    }
    this.#muxes.clear()
    if (this.#renewalScheduler instanceof ManualRenewalScheduler) this.#renewalScheduler.clear()
    for (const stub of this.#stubs.values()) disposeRoomStub(stub)
    this.#stubs.clear()
    for (const stub of this.#subscriberStubs.values()) stub[Symbol.dispose]?.()
    this.#subscriberStubs.clear()
  }
}

export function cloudflareRenewalControls(backend: RoomBackendSpi): {
  forceFailures(count: number): void
  forceEstablishmentFailures(count: number): void
  forceGenerationCaptureFailures(count: number): void
  advance(ms: number): Promise<void>
} {
  if (!(backend instanceof CloudflareRoomBackend)) throw new Error('expected the Cloudflare conformance backend')
  return {
    forceFailures: (count) => backend.forceRenewalFailures(count),
    forceEstablishmentFailures: (count) => backend.forceEstablishmentFailures(count),
    forceGenerationCaptureFailures: (count) => backend.forceGenerationCaptureFailures(count),
    advance: (ms) => backend.advanceRenewalTimers(ms),
  }
}

// ── harness registration ──

export const cloudflareHarness: BackendHarness = {
  name: 'cloudflare',
  async create(): Promise<BackendFixture> {
    const shared = await getShared()
    // Authority time STARTS aligned with the caller clock and diverges only through advanceAuthority —
    // load-bearing for the I13 killers (an epoch offset by years would certify a clock-skew mutation
    // against the wrong scenario).
    setClock(Date.now())
    await clockPush
    const backend = new CloudflareRoomBackend(shared.ns, shared.subscriberNs, new ManualRenewalScheduler())
    return {
      backend,
      // Cloudflare RPCs the target (relayed here to the receiver closure) and awaits it, so the handoff
      // extends to the receiver and a throw is visible on that frame. CX application is genuinely async
      // (an RPC hop), so the barrier-forced I13(c) variant is carried via concurrentHeadCxBarrier.
      traces: { handoffAwaitsReceiver: true, perTargetFailure: true, cxAppliesSynchronously: false },
      expectedReceivers: { twoLocalSubscriptionsSameLane: 1, oneLocalSubscriptionAfterSiblingDetach: 1 },
      authorityNow: () => clockValue,
      advanceAuthority: (ms: number) => setClock(clockValue + ms),
      // Both head-CX requests are issued back-to-back with no await between, so both are in flight before
      // either is awaited; the room DO applies concurrently-arriving RPCs in issue (FIFO) order, which is
      // the linearization this forces (verified: a back-to-back CAS pair always resolves first-issued
      // wins). Releasing in the given order is therefore issuing in that order.
      concurrentHeadCxBarrier: async <T>(first: () => Promise<T>, second: () => Promise<T>): Promise<[T, T]> => {
        const firstPromise = first()
        const secondPromise = second()
        return Promise.all([firstPromise, secondPromise])
      },
      dispose: () => backend.dispose(),
    }
  },
}
