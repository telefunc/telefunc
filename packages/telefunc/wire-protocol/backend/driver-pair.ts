export { BACKEND_SPI_VERSION }
export type { BackendDriverPair }

import type { BroadcastDriver } from './broadcast/contract.js'
import type { RoomDriver } from './room/contract.js'

const BACKEND_SPI_VERSION = 1 as const

type BackendPairLifecycle = {
  readonly spiVersion: typeof BACKEND_SPI_VERSION
  dispose(): Promise<void>
}

type BackendDriverPair = BackendPairLifecycle &
  (
    | { readonly driver: BroadcastDriver & RoomDriver; readonly broadcast?: never; readonly room?: never }
    | { readonly driver?: never; readonly broadcast: BroadcastDriver; readonly room: RoomDriver }
  )
