export { admit, hold, letGo }
export type { MemoryHolder }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../../constants.js'
import type { PublishBody } from './PublishBody.js'

/** What a held publish takes beyond its share of the body: its head and its entries. */
const HELD_PUBLISH_OVERHEAD_BYTES = 200

/** Holds bytes for a page; one that can't let any go refuses instead. */
interface MemoryHolder {
  heldBytes: number
}

const memory = getGlobalObject('channelMemory.ts', { total: 0, holders: new Set<MemoryHolder>(), maxMemory: 0 })

/** Whether `bytes` for `holder` may be held; its caller tells whoever asked when not. Near the budget, a holder past an
 *  equal share is refused, so one still under it has room. */
function admit(holder: MemoryHolder, bytes: number): boolean {
  const maxMemory = (memory.maxMemory ||= getChannelConfig().maxMemory)
  if (holder.heldBytes + bytes > Math.min(CHANNEL_HOLD_MAX_BYTES, maxMemory)) return false
  if (memory.total + bytes <= shedTarget(maxMemory)) return true
  return holder.heldBytes + bytes <= maxMemory / Math.max(1, memory.holders.size) && memory.total + bytes <= maxMemory
}

/** Only counts: never reclaims, so whoever's mid-mutation finishes first. */
function hold(holder: MemoryHolder, held: number | PublishBody): void {
  if (holder.heldBytes === 0) memory.holders.add(holder)
  if (typeof held === 'number') {
    holder.heldBytes += held
    memory.total += held
  } else {
    holder.heldBytes += held.frame.byteLength + HELD_PUBLISH_OVERHEAD_BYTES
    memory.total += HELD_PUBLISH_OVERHEAD_BYTES + (held.refs++ === 0 ? held.frame.byteLength : 0)
  }
}

function letGo(holder: MemoryHolder, held: number | PublishBody): void {
  if (typeof held === 'number') {
    holder.heldBytes -= held
    memory.total -= held
  } else {
    holder.heldBytes -= held.frame.byteLength + HELD_PUBLISH_OVERHEAD_BYTES
    memory.total -= HELD_PUBLISH_OVERHEAD_BYTES + (--held.refs === 0 ? held.frame.byteLength : 0)
  }
  if (holder.heldBytes === 0) memory.holders.delete(holder)
}

/** The top eighth is for those under an equal share. */
function shedTarget(maxMemory: number): number {
  return (maxMemory * 7) / 8
}
