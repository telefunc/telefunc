import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { ROOM_SPI_VERSION, type RoomBackendSpi } from './spi.js'

export type RoomBackendFactory = () => RoomBackendSpi

type RoomBackendState = {
  backend: RoomBackendSpi | undefined
  disposing: Promise<void> | undefined
}

const state = getGlobalObject<RoomBackendState>('wire-protocol/backend/install.ts', () => ({
  backend: undefined,
  disposing: undefined,
}))

/**
 * Installs the per-isolate Room backend once. The factory is deliberately lazy: repeated entry-module
 * evaluation (such as HMR) returns the canonical backend without opening another backend connection.
 */
export function installRoomBackend(factory: RoomBackendFactory): RoomBackendSpi {
  if (state.backend !== undefined) return state.backend
  if (state.disposing !== undefined) {
    throw new Error('telefunc/backend: the Room backend is still disposing and cannot be installed yet')
  }
  if (typeof factory !== 'function') {
    throw new Error('telefunc/backend: installRoomBackend() requires a backend factory')
  }

  const backend = factory()
  assertRoomBackend(backend)
  state.backend = backend
  return backend
}

/** Returns the installed Room backend, or fails before Room policy can use an unconfigured backend. */
export function getRoomBackend(): RoomBackendSpi {
  if (state.backend === undefined) {
    throw new Error('telefunc/backend: no Room backend is installed; call installRoomBackend(...) first')
  }
  return state.backend
}

/**
 * Owns disposal of the canonical backend. The latch clears only after disposal settles, so a new
 * installation cannot overlap an old backend's resources.
 */
export async function disposeRoomBackend(): Promise<void> {
  if (state.disposing !== undefined) return state.disposing
  const backend = state.backend
  if (backend === undefined) return

  const disposing = backend.dispose().finally(() => {
    if (state.backend === backend) state.backend = undefined
    if (state.disposing === disposing) state.disposing = undefined
  })
  state.disposing = disposing
  return disposing
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
