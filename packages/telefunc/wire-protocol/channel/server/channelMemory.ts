export {
  HELD_SEND_OVERHEAD_BYTES,
  addWireCopies,
  admit,
  admitPublish,
  dropWireCopies,
  fits,
  fitsPublish,
  hasCopyRoom,
  heldAnswerBytes,
  heldPublishBytes,
  heldSendBytes,
  hold,
  holdsEnough,
  letGo,
  maxPendingBytesOf,
  queueMemory,
  releaseHolder,
  tryHold,
}
export type { MemoryHolder }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { HEADER } from '../../wire/header.js'
import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES } from '../../wire/frame.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import type { PublishBody } from './PublishBody.js'
import type { QueueMemory } from '../channelMessages.js'

/** What a held send takes beyond its payload, its callbacks and promise, rounded up: one buffered, waiting as a lossy
 *  channel's newest, awaiting its answer or credit, kept to replay, or waiting in the in-memory broadcast adapter. A
 *  lossy send kept to replay is a publish, and holds what one does. */
const HELD_SEND_OVERHEAD_BYTES = 512
/** What a held publish takes beyond its share of the body: its head and its entries. */
const HELD_PUBLISH_OVERHEAD_BYTES = 200

/** Holds bytes for a page. `shed` frees what no wire has, down to `toBytes`, and its own object ends it where its page
 *  would miss a message it may not, or at 0. A lossy subscriber's holder only waits on its newest, so above 0 it frees
 *  nothing. One without `shed` refuses instead. `newestUnhanded` is its newest publish where no wire has it yet. */
interface MemoryHolder {
  heldBytes: number
  /** Its owner ended, so nothing is held for it any more (see `releaseHolder`). */
  released?: boolean
  shed?(toBytes: number, owner?: string): void
  readonly newestUnhanded?: PublishBody | undefined
}

const memory = getGlobalObject('channelMemory.ts', {
  total: 0,
  holders: new Set<MemoryHolder>(),
  maxMemory: 0,
  maxPendingBytes: 0,
  /** What wires hold as copies, on no holder: a replay holds the same bytes. */
  wireCopies: 0,
})

/** Whether `bytes` for `holder` may be held; its caller tells whoever asked when not. Near the budget, a holder past an
 *  equal share is refused, so one still under it has room. A subscriber cut to make room ends after this returns. */
function admit(holder: MemoryHolder, bytes: number): boolean {
  const maxMemory = maxMemoryOf()
  const holderMax = Math.min(maxPendingBytesOf(), maxMemory)
  // Nothing let go makes room for more than one holder may ever hold.
  if (bytes > holderMax) return false
  const target = shedTarget(maxMemory)
  if (memory.total + bytes > target) reclaim(bytes, target)
  if (holder.heldBytes + bytes > holderMax) return false
  if (memory.total + bytes <= target) return true
  return holder.heldBytes + bytes <= maxMemory / memory.holders.size && memory.total + bytes <= maxMemory
}

/** Whether `bytes` for `holder` may be held with nothing let go to make room for them: `admit` then admits them as well. */
function fits(holder: MemoryHolder, bytes: number): boolean {
  const maxMemory = maxMemoryOf()
  return (
    holder.heldBytes + bytes <= Math.min(maxPendingBytesOf(), maxMemory) &&
    memory.total + bytes <= shedTarget(maxMemory)
  )
}

/** Makes room for a delivery to `owner`'s pages, which nobody can refuse. Near the budget, each page whose waiting
 *  publishes it replaces lets go of them first (`replace`), so what's measured is what stays. Past that, it ends the
 *  pages whose newest no wire has, fewest sharers first, until the body fits or none is left: where ending them can't
 *  make room, they end all the same, and so do its own pages (`fitsPublish`). */
function admitPublish(body: PublishBody, owner: string | undefined, replace: (holder: MemoryHolder) => void): void {
  const maxMemory = maxMemoryOf()
  const bytes = body.frame.byteLength
  // Nothing let go makes room for one that can't fit at all: its own pages end (`fitsPublish`), and no other page does.
  if (bytes + HELD_PUBLISH_OVERHEAD_BYTES > maxMemory) return
  const target = shedTarget(maxMemory)
  if (memory.total + bytes > target) for (const holder of [...memory.holders]) replace(holder)
  reclaim(bytes, target)
  if (memory.total + bytes <= maxMemory) return
  for (const [newest, holders] of newestsThatEndingFrees()) {
    if (memory.total + bytes <= maxMemory) return
    if (holders.length === newest.refs) for (const holder of holders) holder.shed!(0, owner)
  }
}

/** Whether one more page may hold `body` within `maxMemory`, once `admitPublish` made what room it could. */
function fitsPublish(body: PublishBody): boolean {
  const bytes = HELD_PUBLISH_OVERHEAD_BYTES + (body.refs === 0 ? body.frame.byteLength : 0)
  return memory.total + bytes <= maxMemoryOf()
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

/** What a send of `bytes` holds: its frame, and what it takes beyond that. */
function heldSendBytes(bytes: number): number {
  return bytes + HEADER + HELD_SEND_OVERHEAD_BYTES
}

/** What a publish holds for one page. */
function heldPublishBytes(body: PublishBody): number {
  return body.frame.byteLength + HELD_PUBLISH_OVERHEAD_BYTES
}

/** What an answer of `result` holds in a replay, or while its page is away. */
function heldAnswerBytes(result: string): number {
  return utf8ByteLength(result) + MESSAGE_FRAME_OVERHEAD_MAX_BYTES + HELD_SEND_OVERHEAD_BYTES
}

/** Only counts: never reclaims, so whoever's mid-mutation finishes first. */
function hold(holder: MemoryHolder, held: number | PublishBody): void {
  if (holder.released) return
  if (holder.heldBytes === 0) memory.holders.add(holder)
  if (typeof held === 'number') {
    holder.heldBytes += held
    memory.total += held
  } else {
    holder.heldBytes += heldPublishBytes(held)
    memory.total += HELD_PUBLISH_OVERHEAD_BYTES + (held.refs++ === 0 ? held.frame.byteLength : 0)
  }
}

/** Holds `bytes` for `holder` where `admit` admits them, and says whether it did. */
function tryHold(holder: MemoryHolder, bytes: number): boolean {
  if (!admit(holder, bytes)) return false
  hold(holder, bytes)
  return true
}

function letGo(holder: MemoryHolder, held: number | PublishBody): void {
  if (holder.released) return
  if (typeof held === 'number') {
    holder.heldBytes -= held
    memory.total -= held
  } else {
    holder.heldBytes -= heldPublishBytes(held)
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

/** What a `messages()` loop's queue or a stream's reader holds, on the ledger as a holder of its own: nothing else can
 *  lower it. */
function queueMemory(): QueueMemory {
  const holder: MemoryHolder = { heldBytes: 0 }
  return {
    tryHold: (bytes) => tryHold(holder, bytes),
    letGo: (bytes) => letGo(holder, bytes),
    release: () => releaseHolder(holder),
  }
}

/** Everyone above the level comes down to it. A body shared goes only with its last holder, so the level drops until the
 *  total does. A holder that can't reach it, its bytes on the wire, leaves this pass; 1, not 0, as its newest stays. */
function reclaim(need: number, target: number): void {
  const stuck = new Set<MemoryHolder>()
  while (memory.total + need > target) {
    const largest = largestSheddable(stuck)
    const level = Math.max(1, largest - (memory.total + need - target))
    if (level >= largest) return
    shedTo(level, stuck)
  }
}

/** The most a holder that sheds holds, of those not in `stuck`. */
function largestSheddable(stuck: ReadonlySet<MemoryHolder>): number {
  let largest = 0
  for (const holder of memory.holders) {
    if (holder.shed && !stuck.has(holder) && holder.heldBytes > largest) largest = holder.heldBytes
  }
  return largest
}

/** Every holder that sheds, of those not in `stuck`, comes down to `level`; one that can't joins `stuck`. */
function shedTo(level: number, stuck: Set<MemoryHolder>): void {
  for (const holder of [...memory.holders]) {
    if (!holder.shed || stuck.has(holder) || holder.heldBytes <= level) continue
    holder.shed(level)
    if (holder.heldBytes > level) stuck.add(holder)
  }
}

/** `config.channel` can't change once a channel exists. */
function maxMemoryOf(): number {
  return (memory.maxMemory ||= getChannelConfig().maxMemory)
}

/** The most one channel holds, as `maxMemoryOf`. */
function maxPendingBytesOf(): number {
  return (memory.maxPendingBytes ||= getChannelConfig().maxPendingBytes)
}

/** The top eighth is for wires' copies and for those under an equal share. */
function shedTarget(maxMemory: number): number {
  return (maxMemory * 7) / 8
}
