export {
  installBackend,
  configureBroadcastTransport,
  getBroadcastBackend,
  followBroadcastPlane,
  unfollowBroadcastPlane,
  backendPlane,
  disposeBackend,
}
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend } from './broadcast/contract.js'
import type { Backend, Subscriber, Unsubscribe } from './driver.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import { MemoryBackend } from './memory/MemoryBackend.js'
import { Subscriptions } from './Subscriptions.js'
import { assertUsage } from '../../utils/assert.js'
import { getOrCreate } from '../../utils/getOrCreate.js'

type Installed = {
  readonly backend: Backend
  readonly key: readonly unknown[]
  /** The memory backend a process falls back to when it installed none. */
  readonly fallback: boolean
  /** The planes built so far, by extension key. */
  readonly planes: Map<symbol, BackendPlane>
}

/** An extension's supervised plane; `disposeBackend()` stops it. */
type BackendPlane = { dispose(): Promise<void> }

/** A consumer of the Broadcast plane that moves to the plane a transport replaces it with. */
type BroadcastPlaneFollower = { planeReplaced(): void }

const state = getGlobalObject<{
  installed: Installed | null
  /** The public broadcast-only override: Broadcast goes through it instead of the installed backend. */
  transport?: BroadcastTransport
  /** Built on first Broadcast use, from the transport or else the installed backend; `null` again once either changes. */
  broadcast: BroadcastBackend | null
  planeFollowers: Set<BroadcastPlaneFollower>
}>('wire-protocol/backend/install.ts', () => ({ installed: null, broadcast: null, planeFollowers: new Set() }))

const FALLBACK_KEY = [Symbol.for('telefunc.memoryBackend')]

/** Installs the process's one backend and returns it. Re-evaluating an entry with the same `key` returns the installed
 *  one; the key names the factory's configuration, so that backend is the one the factory would build. */
function installBackend<B extends Backend>(factory: () => B, key: readonly unknown[]): B {
  const installed = state.installed
  if (installed === null) return install(factory, key, false).backend as B
  if (sameKey(installed.key, key)) return installed.backend as B
  assertUsage(
    !installed.fallback,
    'Install the backend (for example with installRedis()) before its first use: an earlier use already started the in-memory backend',
  )
  assertUsage(false, 'Install one backend per process: a different backend is already installed')
}

/** Installs the public broadcast-only override without displacing the full backend's other planes. */
function configureBroadcastTransport(transport: BroadcastTransport): void {
  if (state.transport === transport) return
  state.transport = transport
  const retired = state.broadcast
  state.broadcast = null
  // Live subscriptions move to the new plane before the retired ones stop.
  for (const follower of [...state.planeFollowers]) follower.planeReplaced()
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
  return (state.broadcast ??= superviseBroadcastDriver(
    state.transport ? createBroadcastTransportDriver(state.transport) : currentBackend().backend,
  ))
}

/** The plane `key` of the installed backend, built once per installed backend: `supervise` wraps the driver the
 *  backend serves, built with the extension's `kit`, or the in-memory one `fallback` builds when the process installed
 *  none, given the driver's subscriptions, shared by source and host. */
function backendPlane<
  Source,
  Driver extends { subscribe(source: Source, subscriber: Subscriber): Unsubscribe },
  Plane extends BackendPlane,
>(
  key: symbol,
  {
    fallback,
    supervise,
    kit,
  }: {
    fallback: () => Driver
    supervise: (driver: Driver, subscriptions: Subscriptions<Source>) => Plane
    kit?: unknown
  },
): Plane {
  assertUsage(
    !(state.transport && (state.installed?.fallback ?? true)),
    'config.broadcast.transport configures Broadcast only, and this call needs a full backend: install the Redis backend or use the Cloudflare adapter.',
  )
  const installed = currentBackend()
  return getOrCreate(installed.planes, key, () => {
    let driver: Driver
    if (installed.fallback) driver = fallback()
    else {
      const build = installed.backend.planes?.[key]
      assertUsage(build, `The installed backend doesn't serve ${String(key)}, which this call needs`)
      driver = build(kit as never) as Driver
    }
    return supervise(driver, new Subscriptions({ driver }))
  }) as Plane
}

/** For tests: forgets the backend, the transport override and the plane's followers, and stops their subscriptions. */
async function disposeBackend(): Promise<void> {
  const installed = state.installed
  const broadcast = state.broadcast
  state.installed = null
  state.broadcast = null
  delete state.transport
  state.planeFollowers.clear()
  const planes = [...(installed?.planes.values() ?? [])]
  await Promise.all([broadcast?.dispose(), ...planes.map((plane) => plane.dispose())])
}

function install(factory: () => Backend, key: readonly unknown[], fallback: boolean): Installed {
  const installed: Installed = {
    backend: factory(),
    key,
    fallback,
    planes: new Map(),
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
