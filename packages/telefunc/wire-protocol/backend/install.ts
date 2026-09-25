export { installBackend, configureBroadcastTransport, getBroadcastBackend, getRoomBackend, disposeBackend }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend, BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import { MemoryBackend } from './memory/backend.js'
import type { RoomBackend, RoomDriver } from './room/contract.js'
import { superviseRoomDriver } from './room/supervise.js'
import { assertUsage } from '../../utils/assert.js'

/** A backend: one driver for both planes. */
type BackendDriver = BroadcastDriver & RoomDriver

type Installed = {
  readonly driver: BackendDriver
  readonly key: readonly unknown[]
  /** The memory backend a process falls back to when it installed none. */
  readonly fallback: boolean
  readonly room: RoomBackend
  /** `null` while a Broadcast transport override owns the Broadcast plane. */
  broadcast: BroadcastBackend | null
}

type BroadcastOverride = { transport: BroadcastTransport; backend?: BroadcastBackend }

const state = getGlobalObject<{ installed: Installed | null; broadcastOverride?: BroadcastOverride }>(
  'wire-protocol/backend/install.ts',
  () => ({ installed: null }),
)

const FALLBACK_KEY = [Symbol('telefunc.memoryBackend')]

/** Installs the process's one backend and returns its driver. Re-evaluating an entry with the same `key` returns the
 *  installed driver; the key names the factory's configuration, so that driver is the one the factory would build. */
function installBackend<Driver extends BackendDriver>(
  factory: () => Driver,
  key: readonly unknown[] = [factory],
): Driver {
  const installed = state.installed
  if (installed === null) return install(factory, key, false).driver as Driver
  if (sameKey(installed.key, key)) return installed.driver as Driver
  assertUsage(
    !installed.fallback,
    'Install the backend (for example with installRedis()) before the first Broadcast or Room use: an earlier use already started the in-memory backend',
  )
  assertUsage(false, 'Install one backend per process: a different backend is already installed')
}

/** Installs the public broadcast-only override without displacing the full backend's Room plane. */
function configureBroadcastTransport(transport: BroadcastTransport): void {
  const previous = state.broadcastOverride
  if (previous?.transport === transport) return
  state.broadcastOverride = { transport }
  if (previous?.backend) void previous.backend.dispose()
  const installed = state.installed
  if (installed === null) return
  if (installed.broadcast) void installed.broadcast.dispose()
  installed.broadcast = null
}

function getBroadcastBackend(): BroadcastBackend {
  const override = state.broadcastOverride
  if (override)
    return (override.backend ??= superviseBroadcastDriver(createBroadcastTransportDriver(override.transport)))
  const installed = currentBackend()
  return (installed.broadcast ??= superviseBroadcastDriver(installed.driver))
}

function getRoomBackend(): RoomBackend {
  assertUsage(
    !(state.broadcastOverride && (state.installed?.fallback ?? true)),
    'config.broadcast.transport configures Broadcast only. Room requires a full backend; install the Redis backend or use the Cloudflare adapter.',
  )
  return currentBackend().room
}

/** For tests: forgets the backend and the transport's plane, and stops their subscriptions. */
async function disposeBackend(): Promise<void> {
  const installed = state.installed
  const overridePlane = state.broadcastOverride?.backend
  state.installed = null
  if (state.broadcastOverride) delete state.broadcastOverride.backend
  await Promise.all([overridePlane?.dispose(), installed?.broadcast?.dispose(), installed?.room.dispose()])
}

function install(factory: () => BackendDriver, key: readonly unknown[], fallback: boolean): Installed {
  const driver = factory()
  const installed: Installed = {
    driver,
    key,
    fallback,
    room: superviseRoomDriver(driver),
    broadcast: state.broadcastOverride ? null : superviseBroadcastDriver(driver),
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
