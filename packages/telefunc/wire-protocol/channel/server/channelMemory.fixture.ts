export { channelLedger }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import type { MemoryHolder, RoomWait } from './channelMemory.js'

type Ledger = { total: number; reserved: number; holders: Set<MemoryHolder>; waiting: Iterable<RoomWait> }

/** What this server's channels hold, on the ledger `channelMemory.ts` keeps for all of them. */
function channelLedger(): Ledger {
  return getGlobalObject<Ledger>('channelMemory.ts', () => {
    throw new Error('channelMemory.ts keeps no ledger yet')
  })
}
