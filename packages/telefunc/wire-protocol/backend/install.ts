import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { MemoryBackend } from './memory/backend.js'
import { BACKEND_SPI_VERSION, type BackendDriver, type BackendSpi } from './spi.js'
import { superviseBackend } from './supervised-backend.js'

export type BackendFactory = () => BackendDriver

type BackendState =
  | { phase: 'empty' }
  | { phase: 'installing' }
  | {
      phase: 'ready'
      driver: BackendDriver
      backend: BackendSpi
      selection: 'memory' | 'default' | 'explicit'
      defaultIdentity?: unknown
    }
  | { phase: 'disposing'; promise: Promise<void> }

type BackendStore = { current: BackendState }

const state = getGlobalObject<BackendStore>('wire-protocol/backend/install.ts', () => ({
  current: { phase: 'empty' },
}))

const INSTALLING_ERROR = 'telefunc/backend: the backend is still installing; retry after installation settles'
const DISPOSING_ERROR = 'telefunc/backend: the backend is still disposing and cannot be acquired or installed yet'

/**
 * Installs the per-isolate backend once. The factory is deliberately lazy: repeated entry-module
 * evaluation (such as HMR) returns the canonical backend without opening another backend connection.
 */
export function installBackend(factory: BackendFactory): BackendSpi {
  const current = state.current
  if (current.phase === 'ready' && current.selection === 'explicit') return current.backend
  return selectBackend(factory, 'explicit')
}

/**
 * Internal integration seam for environment packages. A default outranks the lazy in-memory fallback,
 * while an explicit install always wins regardless of call order. `identity` lets repeated wrapper
 * evaluation remain connection-idempotent without constructing a candidate backend merely to compare it.
 */
export function setDefaultBackend(factory: BackendFactory, identity: unknown = factory): BackendSpi {
  const current = state.current
  if (current.phase === 'ready' && current.selection === 'explicit') return current.backend
  if (current.phase === 'ready' && current.selection === 'default' && Object.is(current.defaultIdentity, identity)) {
    return current.backend
  }
  return selectBackend(factory, 'default', identity)
}

function selectBackend(
  factory: BackendFactory,
  selection: 'default' | 'explicit',
  defaultIdentity?: unknown,
): BackendSpi {
  const current = state.current
  if (current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  if (typeof factory !== 'function') {
    throw new Error('telefunc/backend: installBackend() requires a backend factory')
  }

  state.current = { phase: 'installing' }
  let driver: BackendDriver
  try {
    driver = factory()
    assertBackendDriver(driver)
  } catch (error) {
    state.current = current.phase === 'ready' ? current : { phase: 'empty' }
    throw error
  }

  if (current.phase === 'ready' && Object.is(driver, current.driver)) {
    state.current = current
    return current.backend
  }

  if (current.phase === 'ready') {
    void current.backend.dispose().catch(() => {})
  }
  const backend = superviseBackend(driver)
  state.current = {
    phase: 'ready',
    driver,
    backend,
    selection,
    ...(selection === 'default' ? { defaultIdentity } : {}),
  }
  return backend
}

/**
 * Returns the backend for the current installation generation.
 *
 * Callers must not retain or use this reference across `disposeBackend()`. Disposal is a barrier:
 * W5 must acquire a fresh reference through this seam after it settles.
 */
export function getBackend(): BackendSpi {
  const current = state.current
  if (current.phase === 'ready') return current.backend
  if (current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (current.phase === 'disposing') throw new Error(DISPOSING_ERROR)

  // The implicit default is deliberately composed through the exact same supervision boundary as an
  // installed backend. Memory cannot become a second, silently divergent subscription mechanism.
  const driver = new MemoryBackend()
  assertBackendDriver(driver)
  const backend = superviseBackend(driver)
  state.current = { phase: 'ready', driver, backend, selection: 'memory' }
  return backend
}

/**
 * Owns disposal of the canonical backend. Once this begins, acquisition is blocked until settlement
 * and ordinary callers share the one canonical disposal promise.
 */
export function disposeBackend(): Promise<void> {
  const current = state.current
  if (current.phase === 'empty') return Promise.resolve()
  if (current.phase === 'installing') return Promise.reject(new Error(INSTALLING_ERROR))
  if (current.phase === 'disposing') return current.promise

  const promise = Promise.resolve().then(() => current.backend.dispose())
  const disposing: Extract<BackendState, { phase: 'disposing' }> = { phase: 'disposing', promise }
  state.current = disposing
  promise.then(
    () => clearDisposalPhase(disposing),
    () => clearDisposalPhase(disposing),
  )
  return promise
}

function clearDisposalPhase(disposal: Extract<BackendState, { phase: 'disposing' }>): void {
  if (state.current === disposal) state.current = { phase: 'empty' }
}

const REQUIRED_METHODS = [
  'publish',
  'readHead',
  'compareExchangeHead',
  'readCells',
  'compareExchangeCells',
  'commitLane',
  'readRetained',
  'listRetained',
  'deleteRetained',
  'listGenerations',
  'dropGeneration',
  'directoryPut',
  'directoryDelete',
  'directoryList',
  'dispose',
] as const

function assertBackendDriver(backend: BackendDriver): void {
  if (backend === null || typeof backend !== 'object') {
    throw new Error('telefunc/backend: invalid backend; expected an object')
  }
  if (backend.spiVersion !== BACKEND_SPI_VERSION) {
    throw new Error(
      `telefunc/backend: incompatible backend spiVersion ${String(backend.spiVersion)}; expected ${BACKEND_SPI_VERSION}`,
    )
  }
  for (const method of REQUIRED_METHODS) assertMethod(backend, method)
  if (backend.subscriptions === null || typeof backend.subscriptions !== 'object') {
    throw new Error('telefunc/backend: invalid backend subscriptions; expected an object')
  }
  assertMethod(backend.subscriptions, 'bind')

  const capabilities = backend.capabilities
  if (capabilities === null || typeof capabilities !== 'object') {
    throw new Error('telefunc/backend: invalid backend capabilities; expected an object')
  }
  if (
    capabilities.receivers !== 'global' &&
    capabilities.receivers !== 'node-local' &&
    capabilities.receivers !== 'none'
  ) {
    throw new Error('telefunc/backend: backend capabilities.receivers must be "global", "node-local", or "none"')
  }
  if (
    typeof capabilities.maxRetainedPayloadBytes !== 'number' ||
    Number.isNaN(capabilities.maxRetainedPayloadBytes) ||
    capabilities.maxRetainedPayloadBytes < 0
  ) {
    throw new Error('telefunc/backend: backend capabilities.maxRetainedPayloadBytes must be a non-negative number')
  }
}

function assertMethod(backend: object, method: string): void {
  if (typeof (backend as unknown as Record<string, unknown>)[method] !== 'function') {
    throw new Error(`telefunc/backend: invalid backend; missing required method "${method}"`)
  }
}
