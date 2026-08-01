import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { BroadcastBackend, BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import { BACKEND_SPI_VERSION, type BackendDriverPair } from './driver-pair.js'
import { createMemoryBackendPair } from './memory/backend.js'
import type { RoomBackend, RoomDriver } from './room/contract.js'
import { superviseRoomDriver } from './room/supervise.js'

export type BackendFactory = () => BackendDriverPair

type ManagedBackendPair = {
  readonly broadcast: BroadcastBackend
  readonly room: RoomBackend
  dispose(): Promise<void>
}

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

type BackendStore = { current: BackendState; retiredDisposals?: Set<Promise<void>> }

const state = getGlobalObject<BackendStore>('wire-protocol/backend/install.ts', () => ({ current: { phase: 'empty' } }))
const retiredDisposals = (state.retiredDisposals ??= new Set())

const INSTALLING_ERROR = 'telefunc/backend: the backend is still installing; retry after installation settles'
const DISPOSING_ERROR = 'telefunc/backend: the backend is still disposing and cannot be acquired or installed yet'

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
  selectBackend(factory, 'default', identity)
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

  if (current.phase === 'ready') retireBackend(current.backend)
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
  return getBackendPair().broadcast
}

export function getRoomBackend(): RoomBackend {
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
  if (current.phase === 'empty' && retiredDisposals.size === 0) return Promise.resolve()
  if (current.phase === 'installing') return Promise.reject(new Error(INSTALLING_ERROR))
  if (current.phase === 'disposing') return current.promise

  const disposals = [...retiredDisposals]
  if (current.phase === 'ready') disposals.push(Promise.resolve().then(() => current.backend.dispose()))
  const promise = settleDisposals(disposals)
  const disposing: Extract<BackendState, { phase: 'disposing' }> = { phase: 'disposing', promise }
  state.current = disposing
  const clear = () => clearDisposalPhase(disposing)
  void promise.then(clear, clear)
  return promise
}

function superviseBackendPair(pair: BackendDriverPair): ManagedBackendPair {
  const broadcast = superviseBroadcastDriver(pair.broadcast)
  const room = superviseRoomDriver(pair.room)
  let disposal: Promise<void> | undefined
  return {
    broadcast,
    room,
    dispose: () => (disposal ??= Promise.all([broadcast.dispose(), room.dispose()]).then(() => pair.dispose())),
  }
}

function retireBackend(backend: ManagedBackendPair): void {
  const disposal = Promise.resolve().then(() => backend.dispose())
  retiredDisposals.add(disposal)
  void disposal.then(
    () => retiredDisposals.delete(disposal),
    (error) => {
      retiredDisposals.delete(disposal)
      console.error('telefunc/backend: replaced backend disposal failed', error)
    },
  )
}

async function settleDisposals(disposals: Promise<void>[]): Promise<void> {
  const errors = (await Promise.allSettled(disposals)).flatMap((result) =>
    result.status === 'rejected' ? [result.reason] : [],
  )
  if (errors.length === 1) throw errors[0]
  if (errors.length > 1) throw new AggregateError(errors, 'telefunc/backend: multiple backend disposals failed')
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
  assertDriver(pair.broadcast, 'broadcast', ['publish'])
  assertDriver(pair.room, 'room', ROOM_METHODS)
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
