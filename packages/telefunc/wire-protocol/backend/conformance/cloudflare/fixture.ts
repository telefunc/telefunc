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
  ReadinessState,
  RoomBackendSpi,
  RoomHead,
} from '../../spi.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from '../../../server/adapter/cloudflare/room/codec.js'
import type {
  CellsWire,
  CommitWire,
  DropWire,
  HeadCxWire,
  HeadNextWire,
  HeadWire,
  RegisterWire,
  RetainedWire,
} from '../../../server/adapter/cloudflare/room/do.js'
import { Fanout } from '../../../server/adapter/cloudflare/room/fanout.js'
import type { BackendFixture, BackendHarness } from '../harness.js'
import { bundleWorker } from './bundle.js'

const DIRECTORY_DO_NAME = '__telefunc_room_directory__'
const MAX_RETAINED_BYTES = 16 * 1024 * 1024

// A DO stub's RPC surface, as this facade uses it.
type RoomStub = {
  readHead(): Promise<HeadWire | null>
  compareExchangeHead(cx: HeadCx, next: HeadNextWire): Promise<HeadCxWire>
  readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsWire>
  compareExchangeCells(inc: string, revision: string, mutations: Array<{ key: string; set?: { bytesB64: string; ttlMs?: number } }>): Promise<CxResult>
  commitLane(inc: string, lane: LaneId, payload: Uint8Array, opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string }): Promise<CommitWire>
  readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null>
  listRetained(inc: string): Promise<LaneId[]>
  deleteRetainedLane(inc: string, lane?: LaneId): Promise<void>
  registerRoute(inc: string, laneKey: string, subscriber: string, leaseId: string, bucket: string | null): Promise<RegisterWire>
  renewRoute(inc: string, laneKey: string, subscriber: string, leaseId: string): Promise<{ ok: boolean; expiresAt?: number }>
  unsubscribeRoute(inc: string, laneKey: string, subscriber: string, leaseId: string): Promise<void>
  listGenerations(): Promise<string[]>
  dropGeneration(inc: string): Promise<DropWire>
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(prefix: string, cursor?: string): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }>
  runJanitor(): Promise<{ prunedRoutes: number }>
}

type Namespace = { idFromName(name: string): unknown; get(id: unknown): RoomStub }

// ── shared per-process runtime (one Miniflare, reused across the file's tests) ──

type Shared = {
  mf: Miniflare
  ns: Namespace
}
let sharedPromise: Promise<Shared> | null = null
// subscriberName → the live Node receiver, so the DELIVER service binding can invoke it and await it.
const receivers = new Map<string, LaneReceiver>()

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
async function deliverToReceiver(subscriber: string, frame: Uint8Array, info: { seq: number; timestamp: number }): Promise<void> {
  const receiver = receivers.get(subscriber)
  if (receiver === undefined) return
  const result = receiver(new Uint8Array(frame), { seq: info.seq, timestamp: info.timestamp }) as unknown
  if (result !== null && typeof result === 'object' && typeof (result as { then?: unknown }).then === 'function') {
    await result
  }
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
      durableObjects: { ROOM: { className: 'TelefuncRoomDurableObject', useSQLite: true } },
    })
    const ns = (await mf.getDurableObjectNamespace('ROOM')) as unknown as Namespace
    return { mf, ns }
  })()
  return sharedPromise
}

export async function disposeSharedMiniflare(): Promise<void> {
  const pending = sharedPromise
  sharedPromise = null
  receivers.clear()
  if (pending === null) return
  try {
    const shared = await pending
    await shared.mf.dispose()
  } catch {
    // already gone
  }
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

class CfLaneSubscription implements LaneSubscription {
  readonly ready: Promise<void>
  #state: ReadinessState = 'establishing'
  #settle!: { resolve: () => void; reject: (err: unknown) => void }
  #settled = false
  readonly #listeners = new Set<(state: ReadinessState) => void>()
  readonly #onUnsubscribe: () => Promise<void>

  constructor(onUnsubscribe: () => Promise<void>) {
    this.#onUnsubscribe = onUnsubscribe
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  state(): ReadinessState {
    return this.#state
  }

  onStateChange(cb: (state: ReadinessState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.#state === 'closed') return
    await this.#onUnsubscribe()
    this.#transition('closed')
  }

  // Initial establishment resolves/rejects `ready` WITHOUT notifying onStateChange — the initial
  // transition belongs to the promise, later transitions (loss/re-establish/drop) to the listeners. This
  // matches the memory reference's observable, where establishment runs before any listener attaches.
  establish(): void {
    if (this.#settled) return
    this.#settled = true
    this.#state = 'ready'
    this.#settle.resolve()
  }

  failEstablishment(reason: string): void {
    if (this.#settled) return
    this.#settled = true
    this.#state = 'closed'
    this.#settle.reject(new Error(reason))
  }

  generationDropped(): void {
    this.#transition('closed')
  }

  #transition(state: ReadinessState): void {
    if (this.#state === state) return
    this.#state = state
    for (const cb of this.#listeners) cb(state)
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
  #disposed = false
  // The ephemeral delivery chains, driven in the subscriber isolate (this facade) over the acceptance-time
  // route snapshot the DO returns — one Fanout per room so the (inc, laneKey) chains stay isolated. This
  // is the SAME fanout.ts algorithm the production room DO uses; only its host differs (the workerd
  // service-binding relay serializes a stalled handoff across lanes, so the chain runs here instead).
  readonly #fanouts = new Map<string, Fanout>()
  // subscriberName → its live subscription, so a dropped generation can close the right ones.
  readonly #subs = new Map<string, { inc: string; laneKey: string; sub: CfLaneSubscription }>()

  constructor(ns: Namespace) {
    this.#ns = ns
  }

  #fanoutFor(roomId: string): Fanout {
    let fanout = this.#fanouts.get(roomId)
    if (fanout === undefined) {
      fanout = new Fanout(deliverToReceiver)
      this.#fanouts.set(roomId, fanout)
    }
    return fanout
  }

  #assertLive(): void {
    if (this.#disposed) throw new Error('CloudflareRoomBackend: used after dispose()')
  }

  #stub(roomId: string): RoomStub {
    return this.#ns.get(this.#ns.idFromName(roomId))
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
  ): Promise<{ ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }> {
    await this.#preflight()
    const wire = await this.#stub(roomId).compareExchangeHead(cx, nextToWire(next))
    if ('error' in wire) throw new Error(wire.error)
    if ('conflict' in wire) return { conflict: true, current: wire.current === null ? null : headFromWire(wire.current) }
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

  async compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
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
    const wire = await this.#stub(roomId).commitLane(inc, lane, payload, opts)
    if ('error' in wire) throw new Error(wire.error)
    if ('stale' in wire) return { stale: true }
    // Acceptance is done and the target snapshot is fixed; the ordered at-most-once chain runs here over
    // the current payload. `delivery` rejects only on this frame's own handoff failure and its promises
    // settle in commit order because the chain is settlement-gated.
    const fanout = this.#fanoutFor(roomId)
    const token = fanout.enqueue(inc, laneKeyOf(lane), wire.targets, payload, { seq: wire.seq, timestamp: wire.timestamp })
    const delivery = fanout.await(token)
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
    const subscriber = crypto.randomUUID()
    const leaseId = crypto.randomUUID()
    // The subscriber isolate installs its local receiver BEFORE any RPC (readiness-ordering §2.3 step 1).
    receivers.set(subscriber, receiver)
    const sub = new CfLaneSubscription(async () => {
      await clockPush
      await this.#stub(roomId).unsubscribeRoute(inc, laneKey, subscriber, leaseId)
      receivers.delete(subscriber)
      this.#subs.delete(subscriber)
    })
    this.#subs.set(subscriber, { inc, laneKey, sub })
    void (async () => {
      try {
        await clockPush
        const result = await this.#stub(roomId).registerRoute(inc, laneKey, subscriber, leaseId, null)
        if ('ok' in result) {
          sub.establish()
        } else {
          receivers.delete(subscriber)
          this.#subs.delete(subscriber)
          sub.failEstablishment(result.reason)
        }
      } catch (error) {
        receivers.delete(subscriber)
        this.#subs.delete(subscriber)
        sub.failEstablishment((error as Error).message)
      }
    })()
    return sub
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
    this.#fanoutFor(roomId).clearIncarnation(inc)
    // Close the local subscriptions whose generation just vanished — their channel is terminal.
    for (const [, subscriber] of wire.droppedSubscribers) {
      const entry = this.#subs.get(subscriber)
      if (entry !== undefined) {
        entry.sub.generationDropped()
        receivers.delete(subscriber)
        this.#subs.delete(subscriber)
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
    const backend = new CloudflareRoomBackend(shared.ns)
    return {
      backend,
      // Cloudflare RPCs the target (relayed here to the receiver closure) and awaits it, so the handoff
      // extends to the receiver and a throw is visible on that frame. CX application is genuinely async
      // (an RPC hop), so the barrier-forced I13(c) variant is carried via concurrentHeadCxBarrier.
      traces: { handoffAwaitsReceiver: true, perTargetFailure: true, cxAppliesSynchronously: false },
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
