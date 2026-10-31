export {
  installBackend,
  configureBroadcastTransport,
  getBroadcastBackend,
  followBroadcastPlane,
  unfollowBroadcastPlane,
  backendPlane,
  disposeBackend,
}
export type { BackendDriver }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend, BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import { MemoryBackend } from './memory/backend.js'
import { SubscriptionManager, type PlaneSubscriptions } from './SubscriptionManager.js'
import type { SubscriptionDriver } from './subscription.js'
import { assertUsage } from '../../utils/assert.js'
import { getOrCreate } from '../../utils/getOrCreate.js'

/** A backend: Broadcast's driver, and the driver of each other plane it serves. */
type BackendDriver = BroadcastDriver & {
  /** The planes this backend serves, by extension key; each is built on its first `backendPlane()` call, given the
   *  `kit` the extension passes it. */
  readonly planes?: { readonly [key: symbol]: (kit: never) => unknown }
}

type Installed = {
  readonly driver: BackendDriver
  readonly key: readonly unknown[]
  /** The memory backend a process falls back to when it installed none. */
  readonly fallback: boolean
  /** The planes built so far, by extension key. */
  readonly planes: Map<symbol, BackendPlane>
  /** Built on first Broadcast use; `null` while a transport override owns the plane. */
  broadcast: BroadcastBackend | null
}

/** An extension's supervised plane; `disposeBackend()` stops it. */
type BackendPlane = { dispose(): Promise<void> }

type BroadcastOverride = { transport: BroadcastTransport; backend?: BroadcastBackend }

/** A consumer of the Broadcast plane that moves to the plane a transport replaces it with. */
type BroadcastPlaneFollower = { planeReplaced(): void }

const state = getGlobalObject<{
  installed: Installed | null
  broadcastOverride?: BroadcastOverride
  planeFollowers: Set<BroadcastPlaneFollower>
}>('wire-protocol/backend/install.ts', () => ({ installed: null, planeFollowers: new Set() }))

const FALLBACK_KEY = [Symbol.for('telefunc.memoryBackend')]

/** Installs the process's one backend and returns its driver. Re-evaluating an entry with the same `key` returns the
 *  installed driver; the key names the factory's configuration, so that driver is the one the factory would build. */
function installBackend<Driver extends BackendDriver>(factory: () => Driver, key: readonly unknown[]): Driver {
  const installed = state.installed
  if (installed === null) return install(factory, key, false).driver as Driver
  if (sameKey(installed.key, key)) return installed.driver as Driver
  assertUsage(
    !installed.fallback,
    'Install the backend (for example with installRedis()) before its first use: an earlier use already started the in-memory backend',
  )
  assertUsage(false, 'Install one backend per process: a different backend is already installed')
}

/** Installs the public broadcast-only override without displacing the full backend's other planes. */
function configureBroadcastTransport(transport: BroadcastTransport): void {
  const previous = state.broadcastOverride
  if (previous?.transport === transport) return
  state.broadcastOverride = { transport }
  const installed = state.installed
  const retired = installed?.broadcast
  if (installed) installed.broadcast = null
  // Live subscriptions move to the new plane before the retired ones stop.
  for (const follower of [...state.planeFollowers]) follower.planeReplaced()
  void previous?.backend?.dispose()
  void retired?.dispose()
}

/** Has `follower.planeReplaced()` called, once the new plane is in effect, each time a transport replaces the plane. */
function followBroadcastPlane(follower: BroadcastPlaneFollower): void {
  state.planeFollowers.add(follower)
}

function unfollowBroadcastPlane(follower: BroadcastPlaneFollower): void {
  state.planeFollowers.delete(follower)
}

function getBroadcastBackend(): BroadcastBackend {
  const override = state.broadcastOverride
  if (override) {
    return (override.backend ??= superviseBroadcastDriver(createBroadcastTransportDriver(override.transport)))
  }
  const installed = currentBackend()
  return (installed.broadcast ??= superviseBroadcastDriver(installed.driver))
}

/** The plane `key` of the installed backend, built once per installed backend: `supervise` wraps the driver the
 *  backend serves, built with the extension's `kit`, or the in-memory one `fallback` builds when the process installed
 *  none, given the driver's subscriptions keyed by `sourceKey`, whose deliveries carry `payloadKind`. */
function backendPlane<
  Source,
  Driver extends { readonly subscriptions: SubscriptionDriver<Source> },
  Plane extends BackendPlane,
>(
  key: symbol,
  {
    fallback,
    supervise,
    kit,
    sourceKey,
    payloadKind,
  }: {
    fallback: () => Driver
    supervise: (driver: Driver, subscriptions: PlaneSubscriptions<Source>) => Plane
    kit?: unknown
    sourceKey: (source: Source) => string
    payloadKind: (source: Source) => 'text' | 'binary'
  },
): Plane {
  assertUsage(
    !(state.broadcastOverride && (state.installed?.fallback ?? true)),
    'config.broadcast.transport configures Broadcast only, and this call needs a full backend: install the Redis backend or use the Cloudflare adapter.',
  )
  const installed = currentBackend()
  return getOrCreate(installed.planes, key, () => {
    let driver: Driver
    if (installed.fallback) driver = fallback()
    else {
      const build = installed.driver.planes?.[key]
      assertUsage(build, `The installed backend doesn't serve ${String(key)}, which this call needs`)
      driver = build(kit as never) as Driver
    }
    return supervise(driver, new SubscriptionManager({ driver: driver.subscriptions, sourceKey, payloadKind }))
  }) as Plane
}

/** For tests: forgets the backend, the transport override and the plane's followers, and stops their subscriptions. */
async function disposeBackend(): Promise<void> {
  const installed = state.installed
  const overridePlane = state.broadcastOverride?.backend
  state.installed = null
  delete state.broadcastOverride
  state.planeFollowers.clear()
  const planes = [...(installed?.planes.values() ?? [])]
  await Promise.all([
    overridePlane?.dispose(),
    installed?.broadcast?.dispose(),
    ...planes.map((plane) => plane.dispose()),
  ])
}

function install(factory: () => BackendDriver, key: readonly unknown[], fallback: boolean): Installed {
  const driver = factory()
  const installed: Installed = {
    driver,
    key,
    fallback,
    planes: new Map(),
    broadcast: null,
  }
  state.installed = installed
  return installed
}

function currentBackend(): Installed {
  return state.installed ?? install(() => new MemoryBackend(), FALLBACK_KEY, true)
}

function sameKey(a: readonly unknown[], b: readonly unknown[]): boolean {
  return a.length === b.length && a.every((value, i) => Object.is(value, b[i]))
}
