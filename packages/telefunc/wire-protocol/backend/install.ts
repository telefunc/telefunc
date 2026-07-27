import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { MemoryRoomBackend } from './memory/backend.js'
import { ROOM_SPI_VERSION, type RoomBackendSpi } from './spi.js'

export type RoomBackendFactory = () => RoomBackendSpi

type RoomBackendState =
  | { phase: 'empty'; retired: WeakSet<object> }
  | { phase: 'installing'; retired: WeakSet<object> }
  | {
      phase: 'ready'
      backend: RoomBackendSpi
      selection: 'memory' | 'default' | 'explicit'
      defaultIdentity?: unknown
      retired: WeakSet<object>
    }
  | {
      phase: 'disposing'
      backend: RoomBackendSpi
      promise: Promise<void>
      invokingBackendDisposeSynchronously: boolean
      retired: WeakSet<object>
    }

type RoomBackendStore = { current: RoomBackendState }

const state = getGlobalObject<RoomBackendStore>('wire-protocol/backend/install.ts', () => ({
  current: { phase: 'empty', retired: new WeakSet<object>() },
}))

const INSTALLING_ERROR = 'telefunc/backend: the Room backend is still installing; retry after installation settles'
const DISPOSING_ERROR = 'telefunc/backend: the Room backend is still disposing and cannot be acquired or installed yet'
const REENTRANT_DISPOSE_ERROR =
  'telefunc/backend: backend.dispose() must not call disposeRoomBackend() during its synchronous invocation'

/**
 * Installs the per-isolate Room backend once. The factory is deliberately lazy: repeated entry-module
 * evaluation (such as HMR) returns the canonical backend without opening another backend connection.
 */
export function installRoomBackend(factory: RoomBackendFactory): RoomBackendSpi {
  const current = state.current
  if (current.phase === 'ready' && current.selection === 'explicit') return current.backend
  return selectRoomBackend(factory, 'explicit')
}

/**
 * Internal integration seam for environment packages. A default outranks the lazy in-memory fallback,
 * while an explicit install always wins regardless of call order. `identity` lets repeated wrapper
 * evaluation remain connection-idempotent without constructing a candidate backend merely to compare it.
 */
export function setDefaultRoomBackend(factory: RoomBackendFactory, identity: unknown = factory): RoomBackendSpi {
  const current = state.current
  if (current.phase === 'ready' && current.selection === 'explicit') return current.backend
  if (current.phase === 'ready' && current.selection === 'default' && Object.is(current.defaultIdentity, identity)) {
    return current.backend
  }
  return selectRoomBackend(factory, 'default', identity)
}

function selectRoomBackend(
  factory: RoomBackendFactory,
  selection: 'default' | 'explicit',
  defaultIdentity?: unknown,
): RoomBackendSpi {
  const current = state.current
  if (current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  if (typeof factory !== 'function') {
    throw new Error('telefunc/backend: installRoomBackend() requires a backend factory')
  }

  const retired = current.retired
  state.current = { phase: 'installing', retired }
  let backend: RoomBackendSpi
  try {
    backend = factory()
    assertRoomBackend(backend)
    if (retired.has(backend)) {
      throw new Error('telefunc/backend: a Room backend instance cannot be reinstalled after disposal has begun')
    }
  } catch (error) {
    state.current = current.phase === 'ready' ? current : { phase: 'empty', retired }
    throw error
  }

  if (current.phase === 'ready') {
    retired.add(current.backend)
    // Replacement is deliberately synchronous at the ownership boundary: every bundled default marks
    // itself retired and detaches its live callbacks before dispose() returns its completion promise.
    // The setup API therefore stays synchronous while resource settlement continues in the background.
    void current.backend.dispose().catch(() => {})
  }
  state.current = {
    phase: 'ready',
    backend,
    selection,
    ...(selection === 'default' ? { defaultIdentity } : {}),
    retired,
  }
  return backend
}

/**
 * Returns the backend for the current installation generation.
 *
 * Callers must not retain or use this reference across `disposeRoomBackend()`. Disposal is a barrier:
 * W5 must acquire a fresh reference through this seam after it settles.
 */
export function getRoomBackend(): RoomBackendSpi {
  const current = state.current
  if (current.phase === 'ready') return current.backend
  if (current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (current.phase === 'disposing') throw new Error(DISPOSING_ERROR)

  const backend = new MemoryRoomBackend()
  assertRoomBackend(backend)
  state.current = { phase: 'ready', backend, selection: 'memory', retired: current.retired }
  return backend
}

/**
 * Owns disposal of the canonical backend. Once this begins, acquisition is blocked until settlement.
 * Backends must not call this global seam from inside `backend.dispose()`: synchronous reentry rejects
 * rather than reporting a false completed shutdown. After that call stack unwinds, ordinary callers
 * share the one canonical disposal promise.
 */
export function disposeRoomBackend(): Promise<void> {
  const current = state.current
  if (current.phase === 'empty') return Promise.resolve()
  if (current.phase === 'installing') return Promise.reject(new Error(INSTALLING_ERROR))
  if (current.phase === 'disposing') {
    return current.invokingBackendDisposeSynchronously
      ? Promise.reject(new Error(REENTRANT_DISPOSE_ERROR))
      : current.promise
  }

  const { backend, retired } = current
  retired.add(backend)
  const deferred = createDeferred<void>()
  const disposing: Extract<RoomBackendState, { phase: 'disposing' }> = {
    phase: 'disposing',
    backend,
    promise: deferred.promise,
    invokingBackendDisposeSynchronously: false,
    retired,
  }
  state.current = disposing
  deferred.promise.then(
    () => clearDisposalPhase(disposing),
    () => clearDisposalPhase(disposing),
  )

  try {
    disposing.invokingBackendDisposeSynchronously = true
    Promise.resolve(backend.dispose()).then(deferred.resolve, deferred.reject)
    disposing.invokingBackendDisposeSynchronously = false
  } catch (error) {
    disposing.invokingBackendDisposeSynchronously = false
    deferred.reject(error)
  }
  return deferred.promise
}

function clearDisposalPhase(disposal: Extract<RoomBackendState, { phase: 'disposing' }>): void {
  if (state.current === disposal) state.current = { phase: 'empty', retired: disposal.retired }
}

function createDeferred<T>(): { promise: Promise<T>; resolve(value: T): void; reject(reason: unknown): void } {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

const REQUIRED_METHODS = [
  'readHead',
  'compareExchangeHead',
  'readCells',
  'compareExchangeCells',
  'commitLane',
  'readRetained',
  'listRetained',
  'deleteRetained',
  'subscribeLane',
  'listGenerations',
  'dropGeneration',
  'dispose',
] as const

const DIRECTORY_METHODS = ['directoryPut', 'directoryDelete', 'directoryList'] as const

function assertRoomBackend(backend: RoomBackendSpi): void {
  if (backend === null || typeof backend !== 'object') {
    throw new Error('telefunc/backend: invalid Room backend; expected an object')
  }
  if (backend.spiVersion !== ROOM_SPI_VERSION) {
    throw new Error(
      `telefunc/backend: incompatible Room backend spiVersion ${String(backend.spiVersion)}; expected ${ROOM_SPI_VERSION}`,
    )
  }
  for (const method of REQUIRED_METHODS) assertMethod(backend, method)

  const capabilities = backend.capabilities
  if (capabilities === null || typeof capabilities !== 'object') {
    throw new Error('telefunc/backend: invalid Room backend capabilities; expected an object')
  }
  if (capabilities.receivers !== 'global' && capabilities.receivers !== 'node-local') {
    throw new Error(
      'telefunc/backend: Room backend capabilities.receivers must be "global" or "node-local" (not "none")',
    )
  }
  if (!Number.isFinite(capabilities.maxRetainedPayloadBytes) || capabilities.maxRetainedPayloadBytes < 0) {
    throw new Error(
      'telefunc/backend: Room backend capabilities.maxRetainedPayloadBytes must be a finite non-negative number',
    )
  }
  if (typeof capabilities.clusterSafe !== 'boolean') {
    throw new Error('telefunc/backend: Room backend capabilities.clusterSafe must be a boolean')
  }
  if (typeof capabilities.directory !== 'boolean') {
    throw new Error('telefunc/backend: Room backend capabilities.directory must be a boolean')
  }
  if (capabilities.directory) {
    for (const method of DIRECTORY_METHODS) assertMethod(backend, method)
  }
}

function assertMethod(backend: RoomBackendSpi, method: string): void {
  if (typeof (backend as unknown as Record<string, unknown>)[method] !== 'function') {
    throw new Error(`telefunc/backend: invalid Room backend; missing required method "${method}"`)
  }
}
