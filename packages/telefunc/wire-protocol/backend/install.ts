import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend, BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import { BACKEND_SPI_VERSION, type BackendDriverPair } from './driver-pair.js'
import { createMemoryBackendPair } from './memory/backend.js'
import type { RoomBackend, RoomDriver } from './room/contract.js'
import { superviseRoomDriver } from './room/supervise.js'
import { assertUsage } from '../../utils/assert.js'

export type BackendFactory = () => BackendDriverPair

type ManagedBackendPair = {
  readonly room: RoomBackend
  getBroadcast(): BroadcastBackend
  suspendBroadcast(): Promise<void>
  dispose(): Promise<void>
}

type BroadcastOverride = { transport: BroadcastTransport; backend?: BroadcastBackend }

type BackendState =
  | { phase: 'empty' }
  | { phase: 'installing' }
  | {
      phase: 'ready'
      pair: BackendDriverPair
      backend: ManagedBackendPair
      selection: 'memory' | 'default' | 'explicit'
      defaultIdentity?: unknown
    }
  | { phase: 'disposing'; promise: Promise<void> }

type BackendStore = { current: BackendState; broadcastOverride?: BroadcastOverride }

const state = getGlobalObject<BackendStore>('wire-protocol/backend/install.ts', () => ({ current: { phase: 'empty' } }))

const INSTALLING_ERROR = 'telefunc/backend: the backend is still installing; retry after installation settles'
const DISPOSING_ERROR = 'telefunc/backend: the backend is still disposing and cannot be acquired or installed yet'
const REPLACEMENT_ERROR = 'telefunc/backend: a backend is already active; dispose it before installing another'

/** Lazily installs one backend per isolate; repeated entry evaluation reuses the canonical instance. */
export function installBackend(factory: BackendFactory): void {
  if (state.current.phase === 'ready' && state.current.selection === 'explicit') return
  selectBackend(factory, 'explicit')
}

/** Installs an environment default above memory but below explicit selection. `identity` deduplicates
 * repeated wrapper evaluation without constructing a candidate. */
export function setDefaultBackend(factory: BackendFactory, identity: unknown = factory): void {
  const current = state.current
  if (current.phase === 'ready' && current.selection === 'explicit') return
  if (current.phase === 'ready' && current.selection === 'default' && Object.is(current.defaultIdentity, identity))
    return
  if (current.phase === 'ready') throw new Error(REPLACEMENT_ERROR)
  selectBackend(factory, 'default', identity)
}

/** Installs the public broadcast-only override without displacing the full backend's Room plane. */
export function configureBroadcastTransport(transport: BroadcastTransport): void {
  const previous = state.broadcastOverride
  if (previous?.transport === transport) return
  state.broadcastOverride = { transport }
  if (previous?.backend) void previous.backend.dispose()
  if (state.current.phase === 'ready') void state.current.backend.suspendBroadcast()
}

function selectBackend(
  factory: BackendFactory,
  selection: 'memory' | 'default' | 'explicit',
  defaultIdentity?: unknown,
): ManagedBackendPair {
  const current = state.current
  if (current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  if (typeof factory !== 'function') throw new Error('telefunc/backend: installBackend() requires a backend factory')

  state.current = { phase: 'installing' }
  let pair: BackendDriverPair
  try {
    pair = factory()
    assertBackendDriverPair(pair)
  } catch (error) {
    state.current = current.phase === 'ready' ? current : { phase: 'empty' }
    throw error
  }

  if (current.phase === 'ready' && Object.is(pair, current.pair)) {
    state.current = { ...current, selection, ...(selection === 'default' ? { defaultIdentity } : {}) }
    return current.backend
  }

  if (current.phase === 'ready') {
    state.current = current
    void Promise.resolve(pair.dispose()).catch((error) =>
      console.error('telefunc/backend: rejected backend disposal failed', error),
    )
    throw new Error(REPLACEMENT_ERROR)
  }
  const backend = superviseBackendPair(pair)
  state.current = {
    phase: 'ready',
    pair,
    backend,
    selection,
    ...(selection === 'default' ? { defaultIdentity } : {}),
  }
  return backend
}

export function getBroadcastBackend(): BroadcastBackend {
  if (state.current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (state.current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  const override = state.broadcastOverride
  if (override)
    return (override.backend ??= superviseBroadcastDriver(createBroadcastTransportDriver(override.transport)))
  return getBackendPair().getBroadcast()
}

export function getRoomBackend(): RoomBackend {
  if (
    state.broadcastOverride &&
    (state.current.phase === 'empty' || (state.current.phase === 'ready' && state.current.selection === 'memory'))
  ) {
    assertUsage(
      false,
      'config.broadcast.transport configures Broadcast only. Room requires a full backend; install the Redis backend or use the Cloudflare adapter.',
    )
  }
  return getBackendPair().room
}

function getBackendPair(): ManagedBackendPair {
  const current = state.current
  if (current.phase === 'ready') return current.backend
  return selectBackend(createMemoryBackendPair, 'memory')
}

/** Disposes the canonical backend behind one shared promise, blocking acquisition until settlement. */
export function disposeBackend(): Promise<void> {
  const current = state.current
  const override = state.broadcastOverride
  if (current.phase === 'empty' && !override?.backend) return Promise.resolve()
  if (current.phase === 'installing') return Promise.reject(new Error(INSTALLING_ERROR))
  if (current.phase === 'disposing') return current.promise

  const overrideDisposal = override?.backend?.dispose() ?? Promise.resolve()
  if (override) delete override.backend
  const fullDisposal = current.phase === 'ready' ? current.backend.dispose() : Promise.resolve()
  const promise = Promise.all([overrideDisposal, fullDisposal]).then(() => {})
  const disposing: Extract<BackendState, { phase: 'disposing' }> = { phase: 'disposing', promise }
  state.current = disposing
  const clear = () => clearDisposalPhase(disposing)
  void promise.then(clear, clear)
  return promise
}

function superviseBackendPair(pair: BackendDriverPair): ManagedBackendPair {
  const drivers = resolveBackendDrivers(pair)
  let broadcast = state.broadcastOverride ? null : superviseBroadcastDriver(drivers.broadcast)
  let broadcastRetirement: Promise<void> | undefined
  const room = superviseRoomDriver(drivers.room)
  let disposal: Promise<void> | undefined
  return {
    room,
    getBroadcast: () => (broadcast ??= superviseBroadcastDriver(drivers.broadcast)),
    suspendBroadcast: () => {
      const active = broadcast
      broadcast = null
      if (active) broadcastRetirement = active.dispose()
      return broadcastRetirement ?? Promise.resolve()
    },
    dispose: () =>
      (disposal ??= Promise.all([broadcast?.dispose(), broadcastRetirement, room.dispose()]).then(() => {
        broadcast = null
        return pair.dispose()
      })),
  }
}

function resolveBackendDrivers(pair: BackendDriverPair): { broadcast: BroadcastDriver; room: RoomDriver } {
  return pair.driver ? { broadcast: pair.driver, room: pair.driver } : { broadcast: pair.broadcast, room: pair.room }
}

function clearDisposalPhase(disposal: Extract<BackendState, { phase: 'disposing' }>): void {
  if (state.current === disposal) state.current = { phase: 'empty' }
}

const ROOM_METHODS = [
  'readHead',
  'compareExchangeHead',
  'readCells',
  'compareExchangeCells',
  'commitLane',
  'readRetained',
  'listRetained',
  'deleteRetained',
  'dropGeneration',
  'directoryPut',
  'directoryDelete',
  'directoryList',
] as const

function assertBackendDriverPair(pair: BackendDriverPair): void {
  if (pair === null || typeof pair !== 'object')
    throw new Error('telefunc/backend: invalid backend pair; expected an object')
  if (pair.spiVersion !== BACKEND_SPI_VERSION) {
    throw new Error(
      `telefunc/backend: incompatible backend spiVersion ${String(pair.spiVersion)}; expected ${BACKEND_SPI_VERSION}`,
    )
  }
  const drivers = resolveBackendDrivers(pair)
  assertDriver(drivers.broadcast, 'broadcast', ['publish'])
  assertDriver(drivers.room, 'room', ROOM_METHODS)
  assertMethod(pair, 'dispose')
}

function assertDriver(driver: BroadcastDriver | RoomDriver, plane: string, methods: readonly string[]): void {
  if (driver === null || typeof driver !== 'object') {
    throw new Error(`telefunc/backend: invalid ${plane} driver; expected an object`)
  }
  for (const method of methods) assertMethod(driver, method)
  if (driver.subscriptions === null || typeof driver.subscriptions !== 'object') {
    throw new Error(`telefunc/backend: invalid ${plane} subscriptions; expected an object`)
  }
  assertMethod(driver.subscriptions, 'bind')
}

function assertMethod(owner: object, method: string): void {
  if (typeof (owner as unknown as Record<string, unknown>)[method] !== 'function') {
    throw new Error(`telefunc/backend: invalid backend; missing required method "${method}"`)
  }
}
