export { BACKEND_SPI_VERSION }
export type { BackendDriverPair }

import type { BroadcastDriver } from './broadcast/contract.js'
import type { RoomDriver } from './room/contract.js'

const BACKEND_SPI_VERSION = 1 as const

type BackendDriverPair = {
  readonly spiVersion: typeof BACKEND_SPI_VERSION
  readonly driver: BroadcastDriver & RoomDriver
  dispose(): Promise<void>
}
