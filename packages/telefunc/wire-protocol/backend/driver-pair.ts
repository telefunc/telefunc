export type { BackendDriverPair }

import type { BroadcastDriver } from './broadcast/contract.js'
import type { RoomDriver } from './room/contract.js'

type BackendPairLifecycle = {
  dispose(): Promise<void>
}

type BackendDriverPair = BackendPairLifecycle &
  (
    | { readonly driver: BroadcastDriver & RoomDriver; readonly broadcast?: never; readonly room?: never }
    | { readonly driver?: never; readonly broadcast: BroadcastDriver; readonly room: RoomDriver }
  )
