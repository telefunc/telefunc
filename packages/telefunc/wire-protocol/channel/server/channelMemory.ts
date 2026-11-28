export {
  HELD_PUBLISH_OVERHEAD_BYTES,
  HELD_SEND_OVERHEAD_BYTES,
  addWireCopies,
  admit,
  admitPublish,
  admitSend,
  dropWireCopies,
  fits,
  fitsPublish,
  hasCopyRoom,
  hasRoomFor,
  heldAnswerBytes,
  heldPublishBytes,
  heldSendBytes,
  hold,
  holdsAtMost,
  holdsEnough,
  letGo,
  maxPendingBytesOf,
  nearBudget,
  publishHoldCost,
  queueMemory,
  releaseHolder,
  RoomWait,
  tryHold,
}
export type { MemoryHolder }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { HEADER } from '../../wire/header.js'
import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES } from '../../wire/frame.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { Fifo } from '../../../utils/Fifo.js'
import type { PublishBody } from './PublishBody.js'
import type { QueueMemory } from '../channelMessages.js'
import { assert } from '../../../utils/assert.js'

/** What a held send takes beyond its payload, its callbacks and promise, rounded up: one buffered, waiting as a lossy
 *  channel's newest, awaiting its answer or credit, kept to replay, or waiting in a broadcast backend. A lossy send kept
 *  to replay is a publish, and holds what one does. */
const HELD_SEND_OVERHEAD_BYTES = 512
/** What a held publish takes beyond its share of the body: its head and its entries. */
const HELD_PUBLISH_OVERHEAD_BYTES = 200

/** Holds bytes for a page. `shed` frees what no wire has, down to `toBytes`, and its own object ends it where its page
 *  would miss a message it may not, or at 0. A lossy stream's holder only waits on its newest, so above 0 it frees
 *  nothing of it. One without `shed` refuses instead. `newestUnhanded` is its newest publish where no wire has it yet. */
interface MemoryHolder {
  heldBytes: number
  /** Its owner ended, so nothing is held for it any more (see `releaseHolder`). */
  released?: boolean
  shed?(toBytes: number): void
  /** A newer publish of lossy `stream` reaches its pages: what of that stream it replaces goes. */
  replaceSuperseded?(stream: string): void
  /** Lets go of all it holds where that is only the outcome of a close its page is in, having confirmed the server's
   *  CLOSE: reclaiming asks it before anything sheds. */
  letGoKept?(): void
  readonly newestUnhanded?: PublishBody | undefined
}

const memory = getGlobalObject('channelMemory.ts', {
  total: 0,
  holders: new Set<MemoryHolder>(),
  maxMemory: 0,
  maxPendingBytes: 0,
  /** What wires hold as copies, on no holder: a replay holds the same bytes. */
  wireCopies: 0,
  /** The sends waiting for room, in the order they were made: at most one per holder (`admitSend`). */
  waiting: new Fifo<RoomWait>(),
  /** What the sends waiting for room will hold. Held bytes and these together never pass `maxMemory` (`heldAtMost`). */
  reserved: 0,
  /** Whether a look at `waiting` is queued for the end of this task. */
  wakeQueued: false,
})

/** Whether a send of `bytes` for `holder` is admitted now, is refused, or is to wait for room (`RoomWait`), its bytes
 *  reserved. Sends wait in the order they were made, so one made while others wait waits too. One is refused where it is
 *  past what one holder may hold when it holds nothing, or where `maxMemory` has no room left for it with what is held
 *  and what waits. */
function admitSend(holder: MemoryHolder, bytes: number): 'now' | 'refused' | 'wait' {
  if (bytes > holdsAtMost()) return 'refused'
  if (memory.waiting.length === 0 && admit(holder, bytes)) return 'now'
  return hasRoomFor(bytes) ? 'wait' : 'refused'
}

/** A send of `bytes` for `holder` waiting for room, which `admitSend` found: `go` makes it, holding its bytes, once
 *  `admit` admits them and what the lossy send behind it reserves (`putBehind`). The line is looked at once this task
 *  ends, so a send that fits already goes then. One made out of line (`inLine: false`) reserves nothing, so a holder
 *  dropped before the ledger counts it leaves the ledger nothing, till `joinLine`. */
class RoomWait<Behind = unknown> {
  readonly holder: MemoryHolder
  readonly bytes: number
  private readonly go: () => void
  private parked = true
  /** It was made, or is never made: it is out of line. */
  private gone = false
  /** The lossy send to make right after it, and what that reserves. */
  private behind: { send: Behind; bytes: number } | null = null

  constructor({ holder, bytes, go, inLine }: { holder: MemoryHolder; bytes: number; go: () => void; inLine: boolean }) {
    this.holder = holder
    this.bytes = bytes
    this.go = go
    if (inLine) this.enterLine()
  }

  /** Whether it waits in line: it joined it, and is neither made nor cancelled. */
  get inLine(): boolean {
    return !this.parked && !this.gone
  }

  /** What it reserves, which it holds once made: its bytes, and what the lossy send behind it reserves. */
  get reserved(): number {
    return this.bytes + this.behindBytes
  }

  /** What the lossy send behind it reserves. */
  get behindBytes(): number {
    return this.behind?.bytes ?? 0
  }

  /** Puts a parked wait in line, with the lossy send behind it, where `maxMemory` has room for them. */
  joinLine(): boolean {
    if (!hasRoomFor(this.reserved)) return false
    this.enterLine()
    return true
  }

  /** The send `makeSend` makes waits behind it in place of the one that did, reserving `bytes`, where `maxMemory` has
   *  room for what that adds with what is held and what waits, or, while parked, as it joins the line; `makeSend` runs
   *  only then. It goes in this one's turn, never ahead of an older wait. */
  putBehind(bytes: number, makeSend: () => Behind): boolean {
    const added = bytes - this.behindBytes
    if (!this.parked && added > 0 && !hasRoomFor(added)) return false
    const send = makeSend()
    if (!this.parked && added > 0) memory.reserved += added
    if (!this.parked && added < 0) unreserve(-added)
    this.behind = { send, bytes }
    return true
  }

  /** The lossy send behind it, which no longer reserves anything: it is made now, or never. */
  takeBehind(): Behind | null {
    const behind = this.behind
    if (behind === null) return null
    this.behind = null
    if (!this.parked) unreserve(behind.bytes)
    return behind.send
  }

  /** It goes now, out of turn, holding what it reserved: its channel ends, and its page reads what was sent first. */
  goNow(): void {
    if (this.gone) return
    this.gone = true
    if (!this.parked) unreserve(this.bytes)
    this.go()
  }

  /** Its turn came (`wake`): it goes holding what it reserved, so the next one in line sees it held. */
  goInTurn(): void {
    this.gone = true
    memory.reserved -= this.bytes
    this.go()
  }

  /** It is never made: its reservation goes, and the line moves on without it. */
  cancel(): void {
    if (this.gone) return
    this.gone = true
    if (!this.parked) unreserve(this.reserved)
    this.behind = null
  }

  private enterLine(): void {
    this.parked = false
    memory.waiting.push(this)
    memory.reserved += this.reserved
    wakeSoon()
  }
}

/** Whether `maxMemory` has room for `bytes` more with what is held and what waits. */
function hasRoomFor(bytes: number): boolean {
  return memory.total + memory.reserved + bytes <= maxMemoryOf()
}

function unreserve(bytes: number): void {
  memory.reserved -= bytes
  wakeSoon()
}

/** Whether `bytes` for `holder` may be held; its caller tells whoever asked when not. Near the budget, a holder past an
 *  equal share is refused, so one still under it has room. A subscriber cut to make room ends after this returns. */
function admit(holder: MemoryHolder, bytes: number): boolean {
  const maxMemory = maxMemoryOf()
  // Nothing let go makes room for more than one holder may ever hold, so nothing goes for it.
  if (holder.heldBytes + bytes > holdsAtMost()) return false
  const target = shedTarget(maxMemory)
  if (memory.total + bytes > target) reclaim(bytes, target)
  return admitsAsIs(holder, bytes, 0, maxMemory, heldAtMost(maxMemory, memory.reserved))
}

/** Whether `bytes` for `holder` fit what is held now and `ahead`, which waits ahead of them: under the target, or, near
 *  the budget, within an equal share and `most`. */
function admitsAsIs(holder: MemoryHolder, bytes: number, ahead: number, maxMemory: number, most: number): boolean {
  const total = memory.total + ahead + bytes
  if (total > most) return false
  if (total <= shedTarget(maxMemory)) return true
  // One holding nothing is under any share.
  return holder.heldBytes === 0 || holder.heldBytes + bytes <= maxMemory / memory.holders.size
}

/** The room `maxMemory` keeps for sends waiting to be held: the most one channel may hold, within the top eighth. Held
 *  bytes stop short of it, so a send made while the ledger is full can still wait; and with what waits, they never pass
 *  `maxMemory`. */
function waitingRoom(maxMemory: number): number {
  return Math.min(maxPendingBytesOf(), maxMemory - shedTarget(maxMemory))
}

/** The most bytes held, where `reserved` waits: `maxMemory` less the room for waiting sends, or less what waits. */
function heldAtMost(maxMemory: number, reserved: number): number {
  return maxMemory - Math.max(waitingRoom(maxMemory), reserved)
}

/** Room freed: the sends in line that fit go, first in line first. One the cap holds back keeps its bytes in reserve, so
 *  the sends behind it take only what is left past it. Room is reclaimed once a look, for the first send that needs it. */
function wake(): void {
  memory.wakeQueued = false
  const maxMemory = maxMemoryOf()
  const target = shedTarget(maxMemory)
  // A send made from the line holds what it reserved, so it goes within the held bytes' room alone.
  const most = maxMemory - waitingRoom(maxMemory)
  // What waits ahead, held back by the cap.
  let ahead = memory.reserved
  for (const wait of memory.waiting) if (wait.inLine) ahead -= wait.reserved
  let reclaimed = false
  const perHolder = holdsAtMost()
  memory.waiting.remove((wait) => {
    if (!wait.inLine) return true
    // It goes with the lossy send behind it.
    const bytes = wait.reserved
    // One past its channel's limit waits for its own page, and holds back no other channel.
    if (wait.holder.heldBytes + bytes > perHolder) return false
    if (ahead + memory.total + bytes > most) {
      ahead += bytes
      return false
    }
    if (!reclaimed && memory.total + ahead + bytes > target) {
      reclaimed = true
      reclaim(ahead + bytes, target)
    }
    // One its share holds back leaves its room to the sends behind it, as one past its channel's limit does.
    if (!admitsAsIs(wait.holder, bytes, ahead, maxMemory, most)) return false
    wait.goInTurn()
    return true
  })
}

function wakeSoon(): void {
  if (memory.waiting.length === 0 || memory.wakeQueued) return
  memory.wakeQueued = true
  queueMicrotask(wake)
}

/** Whether `bytes` for `holder` may be held with nothing let go to make room for them: `admit` then admits them as well. */
function fits(holder: MemoryHolder, bytes: number): boolean {
  const maxMemory = maxMemoryOf()
  return (
    memory.waiting.length === 0 &&
    holder.heldBytes + bytes <= holdsAtMost() &&
    memory.total + bytes <= Math.min(shedTarget(maxMemory), heldAtMost(maxMemory, memory.reserved))
  )
}

/** Makes room for a delivery, which nobody can refuse. Near the budget, each page releases the bodies the new lossy
 *  publish replaces before admission measures what stays. Past that, it
 *  ends the pages whose newest no wire has, fewest sharers first, until the body fits or none is left: where ending them
 *  can't make room, they end all the same, and so do its own pages (`fitsPublish`). */
function admitPublish(body: PublishBody): void {
  const maxMemory = maxMemoryOf()
  const bytes = heldPublishBytes(body)
  // Nothing let go makes room for one that can't fit at all: its own pages end (`fitsPublish`), and no other page does.
  if (bytes > maxMemory) return
  const target = shedTarget(maxMemory)
  const most = heldAtMost(maxMemory, memory.reserved)
  if (nearBudget(bytes)) {
    const { stream } = body
    if (body.lossy && stream !== null) for (const holder of [...memory.holders]) holder.replaceSuperseded?.(stream)
    if (memory.total + bytes > target) reclaim(bytes, target)
  }
  if (memory.total + bytes <= most) return
  for (const [newest, holders] of newestsThatEndingFrees()) {
    if (memory.total + bytes <= most) return
    if (holders.length !== newest.refs) continue
    for (const holder of holders) {
      assert(holder.shed)
      holder.shed(0)
    }
  }
}

/** Whether one more page may hold `body` within `maxMemory`, once `admitPublish` made what room it could. */
function fitsPublish(body: PublishBody): boolean {
  return memory.total + publishHoldCost(body) <= heldAtMost(maxMemoryOf(), memory.reserved)
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

/** What one more holder adds to the ledger. */
function publishHoldCost(body: PublishBody): number {
  return HELD_PUBLISH_OVERHEAD_BYTES + (body.refs === 0 ? body.frame.byteLength : 0)
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
    memory.total += publishHoldCost(held)
    held.refs++
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
    held.refs--
    memory.total -= publishHoldCost(held)
  }
  if (holder.heldBytes === 0) memory.holders.delete(holder)
  wakeSoon()
}

/** Every byte held has an owner, and when that owner ends all it holds goes, wherever it was charged: what still runs
 *  for it, like a listener, lets go of nothing after this. Publish bodies have to go first, as others share them. */
function releaseHolder(holder: MemoryHolder): void {
  memory.total -= holder.heldBytes
  holder.heldBytes = 0
  holder.released = true
  memory.holders.delete(holder)
  wakeSoon()
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
  return memory.total + memory.reserved > shedTarget(maxMemoryOf())
}

/** What a `messages()` loop's queue, a stream's reader, or the upload a call keeps for its files once it has answered
 *  holds, on the ledger as a holder of its own: nothing else can lower it. */
function queueMemory(): QueueMemory {
  const holder: MemoryHolder = { heldBytes: 0 }
  return {
    tryHold: (bytes) => tryHold(holder, bytes),
    letGo: (bytes) => letGo(holder, bytes),
    release: () => releaseHolder(holder),
  }
}

/** What its page may already have goes first (`letGoKept`). Then everyone above the level comes down to it. A body shared
 *  goes only with its last holder, so the level drops until the total does. A holder that can't reach it, its bytes on the
 *  wire, leaves this pass; 1, not 0, as its newest stays. */
function reclaim(need: number, target: number): void {
  if (memory.total + need <= target) return
  // A holder let go leaves the set as the walk reaches it, which the walk over the rest allows.
  for (const holder of memory.holders) {
    holder.letGoKept?.()
    if (memory.total + need <= target) return
  }
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

/** `config.channel` can't change once the server has used it. */
function maxMemoryOf(): number {
  return (memory.maxMemory ||= getChannelConfig().maxMemory)
}

/** The most one channel holds, as `maxMemoryOf`. */
function maxPendingBytesOf(): number {
  return (memory.maxPendingBytes ||= getChannelConfig().maxPendingBytes)
}

/** The most one holder may ever hold: a channel's limit, within what `maxMemory` holds past the room for waiting sends. */
function holdsAtMost(): number {
  const maxMemory = maxMemoryOf()
  return Math.min(maxPendingBytesOf(), maxMemory - waitingRoom(maxMemory))
}

/** The top eighth is for wires' copies and for those under an equal share. */
/** Whether `bytes` more would reach past the shedding target or the space left by waiting sends. */
function nearBudget(bytes: number): boolean {
  const maxMemory = maxMemoryOf()
  return memory.total + bytes > Math.min(shedTarget(maxMemory), heldAtMost(maxMemory, memory.reserved))
}

function shedTarget(maxMemory: number): number {
  return (maxMemory * 7) / 8
}
