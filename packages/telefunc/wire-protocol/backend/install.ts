import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { ROOM_SPI_VERSION, type RoomBackendSpi } from './spi.js'

export type RoomBackendFactory = () => RoomBackendSpi

type RoomBackendState =
  | { phase: 'empty'; retired: WeakSet<object> }
  | { phase: 'installing'; retired: WeakSet<object> }
  | { phase: 'ready'; backend: RoomBackendSpi; retired: WeakSet<object> }
  | {
      phase: 'disposing'
      backend: RoomBackendSpi
      promise: Promise<void>
      invokingBackendDispose: boolean
      retired: WeakSet<object>
    }

type RoomBackendStore = { current: RoomBackendState }

const state = getGlobalObject<RoomBackendStore>('wire-protocol/backend/install.ts', () => ({
  current: { phase: 'empty', retired: new WeakSet<object>() },
}))

const INSTALLING_ERROR = 'telefunc/backend: the Room backend is still installing; retry after installation settles'
const DISPOSING_ERROR = 'telefunc/backend: the Room backend is still disposing and cannot be acquired or installed yet'

/**
 * Installs the per-isolate Room backend once. The factory is deliberately lazy: repeated entry-module
 * evaluation (such as HMR) returns the canonical backend without opening another backend connection.
 */
export function installRoomBackend(factory: RoomBackendFactory): RoomBackendSpi {
  const current = state.current
  if (current.phase === 'ready') return current.backend
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
    state.current = { phase: 'empty', retired }
    throw error
  }

  state.current = { phase: 'ready', backend, retired }
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
  throw new Error('telefunc/backend: no Room backend is installed; call installRoomBackend(...) first')
}

/**
 * Owns disposal of the canonical backend. Once this begins, acquisition is blocked until settlement.
 * A direct synchronous reentrant call from `backend.dispose()` is a resolved no-op so a backend cannot
 * await its own shutdown; all external callers share the one canonical disposal promise.
 */
export function disposeRoomBackend(): Promise<void> {
  const current = state.current
  if (current.phase === 'empty') return Promise.resolve()
  if (current.phase === 'installing') return Promise.reject(new Error(INSTALLING_ERROR))
  if (current.phase === 'disposing') {
    return current.invokingBackendDispose ? Promise.resolve() : current.promise
  }

  const { backend, retired } = current
  retired.add(backend)
  const deferred = createDeferred<void>()
  const disposing: Extract<RoomBackendState, { phase: 'disposing' }> = {
    phase: 'disposing',
    backend,
    promise: deferred.promise,
    invokingBackendDispose: false,
    retired,
  }
  state.current = disposing
  deferred.promise.then(
    () => clearDisposalPhase(disposing),
    () => clearDisposalPhase(disposing),
  )

  try {
    disposing.invokingBackendDispose = true
    Promise.resolve(backend.dispose()).then(deferred.resolve, deferred.reject)
    disposing.invokingBackendDispose = false
  } catch (error) {
    disposing.invokingBackendDispose = false
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
