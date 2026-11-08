export {
  addWireCopies,
  admit,
  admitPublish,
  dropWireCopies,
  hasCopyRoom,
  hold,
  holdsEnough,
  letGo,
  queueMemory,
  releaseHolder,
}
export type { MemoryHolder }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../../constants.js'
import type { PublishBody } from './PublishBody.js'
import type { QueueMemory } from '../channelMessages.js'

/** What a held publish takes beyond its share of the body: its head and its entries. */
const HELD_PUBLISH_OVERHEAD_BYTES = 200

/** Holds bytes for a page. `shed` frees what no wire has, down to `toBytes`, and its own object ends it where its page
 *  would miss a message it may not, or at 0. A lossy page's holder only waits on its newest, so above 0 it frees
 *  nothing. One without `shed` refuses instead. `newestUnhanded` is its newest publish where no wire has it yet. */
interface MemoryHolder {
  heldBytes: number
  /** Its owner ended, so nothing is held for it any more (see `releaseHolder`). */
  released?: boolean
  shed?(toBytes: number): void
  readonly newestUnhanded?: PublishBody | undefined
}

const memory = getGlobalObject('channelMemory.ts', {
  total: 0,
  holders: new Set<MemoryHolder>(),
  maxMemory: 0,
  reclaiming: false,
  /** What wires hold as copies, on no holder: a replay holds the same bytes. */
  wireCopies: 0,
})

/** Whether `bytes` for `holder` may be held; its caller tells whoever asked when not. Near the budget, a holder past an
 *  equal share is refused, so one still under it has room. */
function admit(holder: MemoryHolder, bytes: number): boolean {
  const maxMemory = maxMemoryOf()
  const target = shedTarget(maxMemory)
  if (memory.total + bytes > target) reclaim(bytes, target)
  if (holder.heldBytes + bytes > Math.min(CHANNEL_HOLD_MAX_BYTES, maxMemory)) return false
  if (memory.total + bytes <= target) return true
  return holder.heldBytes + bytes <= maxMemory / memory.holders.size && memory.total + bytes <= maxMemory
}

/** Makes room for a delivery, which nobody can refuse. Near the budget, each page whose waiting publishes it replaces
 *  lets go of them first (`replace`), so what's measured is what stays; past what's older, it ends as few as must end,
 *  where that frees the bytes. */
function admitPublish(body: PublishBody, replace?: (holder: MemoryHolder) => void): void {
  const maxMemory = maxMemoryOf()
  const bytes = body.frame.byteLength
  const target = shedTarget(maxMemory)
  if (replace && memory.total + bytes > target) for (const holder of [...memory.holders]) replace(holder)
  reclaim(bytes, target)
  if (memory.total + bytes <= maxMemory || memory.reclaiming) return
  memory.reclaiming = true
  try {
    for (const [newest, holders] of newestsThatEndingFrees()) {
      if (memory.total + bytes <= maxMemory) return
      if (holders.length === newest.refs) for (const holder of holders) holder.shed!(0)
    }
  } finally {
    memory.reclaiming = false
  }
}

/** Fewest sharers first. A body another page has on the wire counts more references than holders here, so it's left. */
function newestsThatEndingFrees(): [PublishBody, MemoryHolder[]][] {
  const byBody = new Map<PublishBody, MemoryHolder[]>()
  for (const holder of memory.holders) {
    const newest = holder.newestUnhanded
    if (!newest) continue
    const holders = byBody.get(newest)
    if (holders) holders.push(holder)
    else byBody.set(newest, [holder])
  }
  return [...byBody].sort((a, b) => a[1].length - b[1].length)
}

/** Only counts: never reclaims, so whoever's mid-mutation finishes first. */
function hold(holder: MemoryHolder, held: number | PublishBody): void {
  if (holder.released) return
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
  if (holder.released) return
  if (typeof held === 'number') {
    holder.heldBytes -= held
    memory.total -= held
  } else {
    holder.heldBytes -= held.frame.byteLength + HELD_PUBLISH_OVERHEAD_BYTES
    memory.total -= HELD_PUBLISH_OVERHEAD_BYTES + (--held.refs === 0 ? held.frame.byteLength : 0)
  }
  if (holder.heldBytes === 0) memory.holders.delete(holder)
}

/** Every byte held has an owner, and when that owner ends all it holds goes, wherever it was charged: what still runs
 *  for it, like a listener, lets go of nothing after this. Publish bodies have to go first, as others share them. */
function releaseHolder(holder: MemoryHolder): void {
  memory.total -= holder.heldBytes
  holder.heldBytes = 0
  holder.released = true
  memory.holders.delete(holder)
}

/** Whether a wire may hand more copies: what a replay shares, a wire's socket can't. The top eighth is theirs, so the
 *  ledger's reclaiming never waits on a wire, nor a wire on it. */
function hasCopyRoom(): boolean {
  return memory.wireCopies < maxMemoryOf() / 8
}

function addWireCopies(bytes: number): void {
  memory.wireCopies += bytes
}

function dropWireCopies(bytes: number): void {
  memory.wireCopies -= bytes
}

/** Whether the server holds enough that it grants its pages no more credit. */
function holdsEnough(): boolean {
  return memory.total > shedTarget(maxMemoryOf())
}

/** What a `messages()` loop's queue holds, on the ledger as a holder of its own: nothing else can lower it. */
function queueMemory(): QueueMemory {
  const holder: MemoryHolder = { heldBytes: 0 }
  return {
    admit: (bytes) => admit(holder, bytes),
    hold: (bytes) => hold(holder, bytes),
    letGo: (bytes) => letGo(holder, bytes),
    release: () => releaseHolder(holder),
  }
}

/** Everyone above the level comes down to it. A body shared goes only with its last holder, so the level drops until the
 *  total does. A holder that can't reach it, its bytes on the wire, leaves this pass; 1, not 0, as its newest stays. */
function reclaim(need: number, target: number): void {
  if (memory.reclaiming) return
  memory.reclaiming = true
  try {
    const stuck = new Set<MemoryHolder>()
    while (memory.total + need > target) {
      let largest = 0
      for (const holder of memory.holders) {
        if (holder.shed && !stuck.has(holder) && holder.heldBytes > largest) largest = holder.heldBytes
      }
      const level = Math.max(1, largest - (memory.total + need - target))
      if (level >= largest) return
      for (const holder of [...memory.holders]) {
        if (!holder.shed || stuck.has(holder) || holder.heldBytes <= level) continue
        holder.shed(level)
        if (holder.heldBytes > level) stuck.add(holder)
      }
    }
  } finally {
    memory.reclaiming = false
  }
}

/** `config.channel` can't change once a channel exists. */
function maxMemoryOf(): number {
  return (memory.maxMemory ||= getChannelConfig().maxMemory)
}

/** The top eighth is for wires' copies and for those under an equal share. */
function shedTarget(maxMemory: number): number {
  return (maxMemory * 7) / 8
}
