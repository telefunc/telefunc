export type { BackendDriverPair }

import type { BroadcastDriver } from './broadcast/contract.js'
import type { RoomDriver } from './room/contract.js'

/** A backend: one driver for both planes, and its disposer. */
type BackendDriverPair = {
  readonly driver: BroadcastDriver & RoomDriver
  dispose(): Promise<void>
}
