// The backend registry the conformance scenarios are parameterized over. Every installed backend must
// produce identical outcomes for every scenario; the only permitted divergence is a `trace`, and a trace
// exists only where the contract itself declares the property per-backend rather than common.
//
// Redis and Cloudflare fixtures append themselves here; the scenario modules never learn a backend name.

import { MemoryBackend, MemoryBackendState } from '../memory/backend.js'
import type { LaneId, BackendSpi } from '../spi.js'

export type BackendTraces = {
  // Does the backend's handoff attempt extend to the receiver callback's own completion? Memory dispatches
  // to local callbacks and Cloudflare RPCs the target directly, so both do; Redis hands off to the broker
  // and can never observe a receiver. spi.md I5 states this is NOT a cross-backend guarantee, so the
  // scenarios that need a slow target are gated on it rather than asserted everywhere.
  handoffAwaitsReceiver: boolean
  // Is a failing target visible on that frame's `delivery` promise? Same clause: per-backend visibility,
  // never a false common guarantee (Redis exposes only the PUBLISH reply).
  perTargetFailure: boolean
  // Does a head CX apply atomically-synchronously — compare and store both complete before the returned
  // promise exists? On such a backend no two CXs can ever be in flight at once, so the two serial
  // linearizations of I13(c) exhaust its schedule space and asserting both IS the race. A backend with
  // genuinely ASYNCHRONOUS CX application must set this false, supply `concurrentHeadCxBarrier`, and run
  // the barrier-forced variant (spi.md I13 race-realization note, ratified 2026-07-20).
  cxAppliesSynchronously: boolean
}

export type BackendFixture = {
  backend: BackendSpi
  traces: BackendTraces
  // Exact authority-target counts for scenarios whose physical target granularity is backend-specific.
  // These are mappings, never lower bounds or skips: memory counts callbacks, Redis connections, and CF
  // routed session-shard Durable Objects.
  expectedReceivers: {
    twoLocalSubscriptionsSameLane: number
    oneLocalSubscriptionAfterSiblingDetach: number
  }
  // Optional exact result for a node-local authority. The fixture must resolve the concrete authority
  // and target owners for this run; accepting a set would let a constant result masquerade as correct.
  allowedReceiverCountsAtAuthority?(roomId: string, inc: string, lane: LaneId, globalFallback: number): Promise<number>
  // Authority time as the BACKEND sees it — never the caller's clock. Lease expiry, commit preconditions
  // and TTLs are all resolved against this, so the scenarios drive it explicitly instead of waiting.
  authorityNow(): number
  advanceAuthority(ms: number): void
  // REQUIRED iff traces.cxAppliesSynchronously is false. Queues both head-CX requests before either
  // reaches the backend, asserts both are pending, then releases them in the given order — the only way
  // to realize I13(c)'s race on a backend whose CX application is genuinely asynchronous. W2b/W2c carry
  // this obligation; the conformance suite fails loudly if an async backend registers without it.
  concurrentHeadCxBarrier?: <T>(first: () => Promise<T>, second: () => Promise<T>) => Promise<[T, T]>
  // Test-only, production-backed controls for the ordering boundary cases that cannot be reached by
  // billions of public commits. Implementations manipulate the real backend's authoritative store; no
  // production backend exposes a test method or a second public constructor.
  orderControl: {
    setAuthority(now: number): void
    runMaintenance(roomId: string): Promise<void>
    reconstructBackend(roomId: string): Promise<void>
    seedWatermark(roomId: string, inc: string, lane: LaneId, seq: number, timestamp: number): Promise<void>
  }
  dispose(): Promise<void>
}

export async function expectedReceiverCount(
  fixture: BackendFixture,
  roomId: string,
  inc: string,
  lane: LaneId,
  globalFallback: number,
): Promise<number> {
  return fixture.allowedReceiverCountsAtAuthority === undefined
    ? globalFallback
    : await fixture.allowedReceiverCountsAtAuthority(roomId, inc, lane, globalFallback)
}

export type BackendHarness = {
  name: string
  create(): Promise<BackendFixture>
}

function orderDomainKey(lane: LaneId): string {
  switch (lane.kind) {
    case 'semantic':
      return 'semantic'
    case 'control':
      return 'control'
    case 'binary':
      return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
    case 'inbox':
      return `inbox:${encodeURIComponent(lane.member)}`
  }
}

export const memoryHarness: BackendHarness = {
  name: 'memory',
  async create(): Promise<BackendFixture> {
    // Authority time STARTS aligned with the caller clock and diverges only through advanceAuthority.
    // That alignment is load-bearing for the I13 killers: an epoch offset by years would make a backend
    // that wrongly consults the caller clock fail the wrong scenario (a takeover would look permanently
    // expired), which would certify the mutation gate against the wrong invariant.
    let clock = Date.now()
    const state = new MemoryBackendState()
    const facades: MemoryBackend[] = []
    const createFacade = (): MemoryBackend => {
      const backend = new MemoryBackend({ authorityNow: () => clock, state })
      facades.push(backend)
      return backend
    }
    const fixture: BackendFixture = {
      backend: createFacade(),
      traces: { handoffAwaitsReceiver: true, perTargetFailure: true, cxAppliesSynchronously: true },
      expectedReceivers: { twoLocalSubscriptionsSameLane: 2, oneLocalSubscriptionAfterSiblingDetach: 1 },
      authorityNow: () => clock,
      advanceAuthority: (ms) => {
        clock += ms
      },
      orderControl: {
        setAuthority: (now) => {
          clock = now
        },
        runMaintenance: async (roomId) => {
          await fixture.backend.listGenerations(roomId)
        },
        reconstructBackend: async () => {
          fixture.backend = createFacade()
        },
        seedWatermark: async (roomId, inc, lane, seq, timestamp) => {
          const generation = state.rooms.get(roomId)?.gens.get(inc)
          if (generation === undefined) throw new Error('memory boundary control: generation is absent')
          generation.order.set(orderDomainKey(lane), { seq, timestamp })
        },
      },
      dispose: async () => {
        await Promise.all(facades.map((backend) => backend.dispose()))
      },
    }
    return fixture
  },
}

export const installedBackends: BackendHarness[] = [memoryHarness]
