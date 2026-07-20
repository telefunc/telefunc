// The backend registry the conformance scenarios are parameterized over. Every installed backend must
// produce identical outcomes for every scenario; the only permitted divergence is a `trace`, and a trace
// exists only where the contract itself declares the property per-backend rather than common.
//
// Redis and Cloudflare fixtures append themselves here; the scenario modules never learn a backend name.

import { MemoryRoomBackend } from '../memory/backend.js'
import type { RoomBackendSpi } from '../spi.js'

export type BackendTraces = {
  // Does the backend's handoff attempt extend to the receiver callback's own completion? Memory dispatches
  // to local callbacks and Cloudflare RPCs the target directly, so both do; Redis hands off to the broker
  // and can never observe a receiver. spi.md I5 states this is NOT a cross-backend guarantee, so the
  // scenarios that need a slow target are gated on it rather than asserted everywhere.
  handoffAwaitsReceiver: boolean
  // Is a failing target visible on that frame's `delivery` promise? Same clause: per-backend visibility,
  // never a false common guarantee (Redis exposes only the PUBLISH reply).
  perTargetFailure: boolean
}

export type BackendFixture = {
  backend: RoomBackendSpi
  traces: BackendTraces
  // Authority time as the BACKEND sees it — never the caller's clock. Lease expiry, commit preconditions
  // and TTLs are all resolved against this, so the scenarios drive it explicitly instead of waiting.
  authorityNow(): number
  advanceAuthority(ms: number): void
  dispose(): Promise<void>
}

export type BackendHarness = {
  name: string
  create(): Promise<BackendFixture>
}

export const memoryHarness: BackendHarness = {
  name: 'memory',
  async create(): Promise<BackendFixture> {
    // Authority time STARTS aligned with the caller clock and diverges only through advanceAuthority.
    // That alignment is load-bearing for the I13 killers: an epoch offset by years would make a backend
    // that wrongly consults the caller clock fail the wrong scenario (a takeover would look permanently
    // expired), which would certify the mutation gate against the wrong invariant.
    let clock = Date.now()
    const backend = new MemoryRoomBackend({ authorityNow: () => clock })
    return {
      backend,
      traces: { handoffAwaitsReceiver: true, perTargetFailure: true },
      authorityNow: () => clock,
      advanceAuthority: (ms) => {
        clock += ms
      },
      dispose: () => backend.dispose(),
    }
  },
}

export const installedBackends: BackendHarness[] = [memoryHarness]
