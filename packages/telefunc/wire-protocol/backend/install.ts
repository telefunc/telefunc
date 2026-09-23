import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend, BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { createBroadcastTransportDriver, type BroadcastTransport } from './broadcast/transport.js'
import type { BackendDriverPair } from './driver-pair.js'
import { createMemoryBackendPair } from './memory/backend.js'
import type { RoomBackend, RoomDriver } from './room/contract.js'
import { superviseRoomDriver } from './room/supervise.js'
import { assert, assertUsage } from '../../utils/assert.js'

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
  | { phase: 'ready'; backend: ManagedBackendPair; identity: unknown }
  | { phase: 'disposing'; promise: Promise<void> }

type BackendStore = { current: BackendState; installing: boolean; broadcastOverride?: BroadcastOverride }

const state = getGlobalObject<BackendStore>('wire-protocol/backend/install.ts', () => ({
  current: { phase: 'empty' },
  installing: false,
}))

const DISPOSING_ERROR = 'telefunc/backend: the backend is still disposing and cannot be acquired or installed yet'
const REPLACEMENT_ERROR = 'telefunc/backend: a backend is already active; dispose it before installing another'

/** Installs the process's one backend; re-evaluating the same entry (same `identity`) is a no-op. */
export function installBackend(factory: BackendFactory, identity: unknown = factory): void {
  const current = state.current
  if (current.phase === 'ready' && Object.is(current.identity, identity)) return
  if (current.phase === 'ready') throw new Error(REPLACEMENT_ERROR)
  selectBackend(factory, identity)
}

/** Installs the public broadcast-only override without displacing the full backend's Room plane. */
export function configureBroadcastTransport(transport: BroadcastTransport): void {
  const previous = state.broadcastOverride
  if (previous?.transport === transport) return
  state.broadcastOverride = { transport }
  if (previous?.backend) void previous.backend.dispose()
  if (state.current.phase === 'ready') void state.current.backend.suspendBroadcast()
}

function selectBackend(factory: BackendFactory, identity: unknown): ManagedBackendPair {
  if (state.current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  assert(!state.installing) // a backend factory never reaches back into the backend
  state.installing = true
  let pair: BackendDriverPair
  try {
    pair = factory()
  } finally {
    state.installing = false
  }
  assertBackendDriverPair(pair)
  const backend = superviseBackendPair(pair)
  state.current = { phase: 'ready', backend, identity }
  return backend
}

export function getBroadcastBackend(): BroadcastBackend {
  if (state.current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  const override = state.broadcastOverride
  if (override)
    return (override.backend ??= superviseBroadcastDriver(createBroadcastTransportDriver(override.transport)))
  return getBackendPair().getBroadcast()
}

export function getRoomBackend(): RoomBackend {
  if (
    state.broadcastOverride &&
    (state.current.phase === 'empty' ||
      (state.current.phase === 'ready' && state.current.identity === createMemoryBackendPair))
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
  return selectBackend(createMemoryBackendPair, createMemoryBackendPair)
}

/** Disposes the canonical backend behind one shared promise, blocking acquisition until settlement. */
export function disposeBackend(): Promise<void> {
  const current = state.current
  const override = state.broadcastOverride
  if (current.phase === 'empty' && !override?.backend) return Promise.resolve()
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
  const { driver } = pair
  let broadcast = state.broadcastOverride ? null : superviseBroadcastDriver(driver)
  let broadcastRetirement: Promise<void> | undefined
  const room = superviseRoomDriver(driver)
  let disposal: Promise<void> | undefined
  return {
    room,
    getBroadcast: () => (broadcast ??= superviseBroadcastDriver(driver)),
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
  assertDriver(pair.driver, ['publish', ...ROOM_METHODS])
  assertMethod(pair, 'dispose')
}

function assertDriver(driver: BroadcastDriver & RoomDriver, methods: readonly string[]): void {
  if (driver === null || typeof driver !== 'object') {
    throw new Error('telefunc/backend: invalid backend driver; expected an object')
  }
  for (const method of methods) assertMethod(driver, method)
  if (driver.subscriptions === null || typeof driver.subscriptions !== 'object') {
    throw new Error('telefunc/backend: invalid backend subscriptions; expected an object')
  }
  assertMethod(driver.subscriptions, 'bind')
}

function assertMethod(owner: object, method: string): void {
  if (typeof (owner as unknown as Record<string, unknown>)[method] !== 'function') {
    throw new Error(`telefunc/backend: invalid backend; missing required method "${method}"`)
  }
}
