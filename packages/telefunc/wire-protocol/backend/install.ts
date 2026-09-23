export type { BackendDriver }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend, BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import { MemoryBackend } from './memory/backend.js'
import type { RoomBackend, RoomDriver } from './room/contract.js'
import { superviseRoomDriver } from './room/supervise.js'
import { assertUsage } from '../../utils/assert.js'

/** A backend: one driver for both planes, which disposes itself. */
type BackendDriver = BroadcastDriver & RoomDriver & { dispose(): Promise<void> }

type Installed = {
  readonly driver: BackendDriver
  readonly key: readonly unknown[]
  /** The memory backend a process falls back to when it installed none. */
  readonly fallback: boolean
  readonly room: RoomBackend
  /** `null` while a Broadcast transport override owns the Broadcast plane. */
  broadcast: BroadcastBackend | null
  retiredBroadcast?: Promise<void>
}

type BroadcastOverride = { transport: BroadcastTransport; backend?: BroadcastBackend }

type BackendState =
  | { phase: 'empty' }
  | { phase: 'ready'; installed: Installed }
  | { phase: 'disposing'; promise: Promise<void> }

const state = getGlobalObject<{ current: BackendState; broadcastOverride?: BroadcastOverride }>(
  'wire-protocol/backend/install.ts',
  () => ({ current: { phase: 'empty' } }),
)

const DISPOSING_ERROR = 'telefunc/backend: the backend is still disposing and cannot be acquired or installed yet'
const REPLACEMENT_ERROR = 'telefunc/backend: a backend is already active; dispose it before installing another'
const FALLBACK_KEY = [Symbol('telefunc.memoryBackend')]

/** Installs the process's one backend and returns its driver. Re-evaluating an entry with the same `key` returns the
 *  installed driver; the key names the factory's configuration, so that driver is the one the factory would build. */
export function installBackend<Driver extends BackendDriver>(
  factory: () => Driver,
  key: readonly unknown[] = [factory],
): Driver {
  const current = state.current
  if (current.phase !== 'ready') return install(factory, key, false).driver as Driver
  if (!sameKey(current.installed.key, key)) throw new Error(REPLACEMENT_ERROR)
  return current.installed.driver as Driver
}

/** Installs the public broadcast-only override without displacing the full backend's Room plane. */
export function configureBroadcastTransport(transport: BroadcastTransport): void {
  const previous = state.broadcastOverride
  if (previous?.transport === transport) return
  state.broadcastOverride = { transport }
  if (previous?.backend) void previous.backend.dispose()
  if (state.current.phase !== 'ready') return
  const installed = state.current.installed
  if (installed.broadcast) installed.retiredBroadcast = installed.broadcast.dispose()
  installed.broadcast = null
}

export function getBroadcastBackend(): BroadcastBackend {
  if (state.current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  const override = state.broadcastOverride
  if (override)
    return (override.backend ??= superviseBroadcastDriver(createBroadcastTransportDriver(override.transport)))
  const installed = currentBackend()
  return (installed.broadcast ??= superviseBroadcastDriver(installed.driver))
}

export function getRoomBackend(): RoomBackend {
  const current = state.current
  const fallbackOnly = current.phase === 'empty' || (current.phase === 'ready' && current.installed.fallback)
  assertUsage(
    !(state.broadcastOverride && fallbackOnly),
    'config.broadcast.transport configures Broadcast only. Room requires a full backend; install the Redis backend or use the Cloudflare adapter.',
  )
  return currentBackend().room
}

/** Disposes the canonical backend behind one shared promise, blocking acquisition until settlement. */
export function disposeBackend(): Promise<void> {
  const current = state.current
  const override = state.broadcastOverride
  if (current.phase === 'empty' && !override?.backend) return Promise.resolve()
  if (current.phase === 'disposing') return current.promise

  const overrideDisposal = override?.backend?.dispose() ?? Promise.resolve()
  if (override) delete override.backend
  const fullDisposal = current.phase === 'ready' ? disposeInstalled(current.installed) : Promise.resolve()
  const promise = Promise.all([overrideDisposal, fullDisposal]).then(() => {})
  const disposing: BackendState = { phase: 'disposing', promise }
  state.current = disposing
  const clear = () => {
    if (state.current === disposing) state.current = { phase: 'empty' }
  }
  void promise.then(clear, clear)
  return promise
}

function install(factory: () => BackendDriver, key: readonly unknown[], fallback: boolean): Installed {
  if (state.current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  const driver = factory()
  const installed: Installed = {
    driver,
    key,
    fallback,
    room: superviseRoomDriver(driver),
    broadcast: state.broadcastOverride ? null : superviseBroadcastDriver(driver),
  }
  state.current = { phase: 'ready', installed }
  return installed
}

function currentBackend(): Installed {
  const current = state.current
  if (current.phase === 'ready') return current.installed
  return install(() => new MemoryBackend(), FALLBACK_KEY, true)
}

async function disposeInstalled(installed: Installed): Promise<void> {
  await Promise.all([installed.broadcast?.dispose(), installed.retiredBroadcast, installed.room.dispose()])
  installed.broadcast = null
  await installed.driver.dispose()
}

function sameKey(a: readonly unknown[], b: readonly unknown[]): boolean {
  return a.length === b.length && a.every((value, i) => Object.is(value, b[i]))
}
