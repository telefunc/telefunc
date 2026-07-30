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

type BackendStore = { current: BackendState; retiredDisposals?: Set<Promise<void>> }

const state = getGlobalObject<BackendStore>('wire-protocol/backend/install.ts', () => ({ current: { phase: 'empty' } }))
const retiredDisposals = (state.retiredDisposals ??= new Set())

const INSTALLING_ERROR = 'telefunc/backend: the backend is still installing; retry after installation settles'
const DISPOSING_ERROR = 'telefunc/backend: the backend is still disposing and cannot be acquired or installed yet'

/** Lazily installs one backend per isolate; repeated entry evaluation reuses the canonical instance. */
export function installBackend(factory: BackendFactory): BackendSpi {
  const current = state.current
  if (current.phase === 'ready' && current.selection === 'explicit') return current.backend
  return selectBackend(factory, 'explicit')
}

/** Installs an environment default above memory but below explicit selection. `identity` deduplicates
 * repeated wrapper evaluation without constructing a candidate. */
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
  selection: 'memory' | 'default' | 'explicit',
  defaultIdentity?: unknown,
): BackendSpi {
  const current = state.current
  if (current.phase === 'installing') throw new Error(INSTALLING_ERROR)
  if (current.phase === 'disposing') throw new Error(DISPOSING_ERROR)
  if (typeof factory !== 'function') throw new Error('telefunc/backend: installBackend() requires a backend factory')

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

  if (current.phase === 'ready') retireBackend(current.backend)
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

/** Returns the current generation. Callers must reacquire after the disposal barrier settles. */
export function getBackend(): BackendSpi {
  const current = state.current
  if (current.phase === 'ready') return current.backend
  return selectBackend(() => new MemoryBackend(), 'memory')
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

function retireBackend(backend: BackendSpi): void {
  // Replacement cleanup may overlap but remains observable through the next disposal barrier.
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
}

function assertMethod(backend: object, method: string): void {
  if (typeof (backend as unknown as Record<string, unknown>)[method] !== 'function') {
    throw new Error(`telefunc/backend: invalid backend; missing required method "${method}"`)
  }
}
