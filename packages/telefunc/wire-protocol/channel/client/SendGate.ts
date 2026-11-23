export { SendGate }
export type { SendGateOwner }

import type { ChannelRecord, ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport, OutboundFrame, OutboundFrameKind } from './ClientConnection.js'
import { Fifo } from '../../../utils/Fifo.js'
import { TAG } from '../../wire/tags.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
  waitingSince: number
}
type HeldFrame = BufferedFrame & { seq: number }

/** A WINDOW or MSG_WINDOW frame's key, made of its channel and its tag; null for any other frame. */
function limitKey({ frame, channelIx }: BufferedFrame): number | null {
  return frame[0] === TAG.WINDOW || frame[0] === TAG.MSG_WINDOW ? channelIx * 256 + frame[0] : null
}

/** The connection, whose transport takes what the gate lets go. */
interface SendGateOwner {
  /** False while a RECONCILE or an upgrade holds sends back. */
  readonly sendsOpen: boolean
  /** While a RECONCILE is out, the wire still takes new frames of the channels its session has (see `takesLive`). */
  readonly liveWire: boolean
  /** Whether, while a RECONCILE is out, the wire takes `record`'s new frames: the server has its channel. */
  takesLive(record: ChannelRecord): boolean
  /** The page lost its wire and has none yet. */
  readonly reconnecting: boolean
  readonly transport: ClientChannelTransport
}

type SendGateOptions = {
  owner: SendGateOwner
  channels: ChannelTable
}

/** What the page's channels send: at once while sends are open, or while a RECONCILE is out the wire takes the
 *  channel's frames and nothing ahead holds them back, and the transport has headroom; else queued, in order. */
class SendGate {
  private readonly owner: SendGateOwner
  private readonly channels: ChannelTable
  /** Frames held in order while sends are held back, or till the server attaches their channel (ATTACH_RESULT). */
  private sendBuffer: BufferedFrame[] = []
  /** How many frames at the front of `sendBuffer` are known to be of channels the server awaits, which hold nothing
   *  back; any change to the buffer but a push, and an attachment's drain, starts it over. */
  private awaitedPrefix = 0
  /** Sequenced frames waiting only for the transport's headroom, in order: a channel's next send waits behind them. */
  private waitingForHeadroom = new Fifo<HeldFrame>()
  private waitingForHeadroomBytes = 0
  /** Channels whose held and waiting frames are dropped, all in one pass at the next read (see `dropQueues`). */
  private droppedQueues = new Set<number>()

  constructor({ owner, channels }: SendGateOptions) {
    this.owner = owner
    this.channels = channels
  }

  /** Bytes of the frames waiting for the transport's headroom. */
  get waitingBytes(): number {
    return this.waitingForHeadroomBytes
  }

  /** Nothing waits to go. */
  get idle(): boolean {
    return this.settleQueue().length === 0 && this.waitingForHeadroom.length === 0
  }

  /** Sends after the frames waiting for headroom; holds it while sends are held back or its channel isn't attached. */
  send(record: ChannelRecord, seq: number, frame: Uint8Array<ArrayBuffer>, kind: OutboundFrameKind): void {
    if (!this.canSendImmediately(record)) {
      this.settleQueue().push({ frame, channelIx: record.ix, seq, waitingSince: performance.now() })
      return
    }
    // What waited goes first.
    this.sendWaiting()
    const { transport } = this.owner
    if (this.waitingForHeadroom.length === 0 && transport.headroom() > 0) {
      record.replay.push(seq, frame)
      transport.sendFrame({ kind, frame })
      return
    }
    this.waitForHeadroom({ frame, channelIx: record.ix, seq, waitingSince: performance.now() })
  }

  /** Puts `frame` in the place of channel `ix`'s frame `seq` while that waits, behind or not, and says whether it did. */
  replaceHeld(ix: number, seq: number, frame: Uint8Array<ArrayBuffer>): boolean {
    for (const entry of this.waitingForHeadroom) {
      if (entry.channelIx !== ix || entry.seq !== seq) continue
      this.waitingForHeadroomBytes += frame.byteLength - entry.frame.byteLength
      entry.frame = frame
      return true
    }
    const held = this.settleQueue().find((entry) => entry.channelIx === ix && entry.seq === seq)
    if (held === undefined) return false
    held.frame = frame
    return true
  }

  /** Puts `frame` in the place of channel `ix`'s frame `seq` while that waits and the page is behind, and returns the
   *  bytes it adds over that one: null once it went, as the wire then has it, or while the link keeps up. A page with no
   *  wire, and a channel no wire has opened yet, have no link to keep up with. */
  replaceWaiting(ix: number, seq: number, frame: Uint8Array<ArrayBuffer>): number | null {
    for (const entry of this.waitingForHeadroom) {
      if (entry.channelIx !== ix || entry.seq !== seq) continue
      if (!this.isBehind(this.waitingForHeadroom.peek()!)) return null
      const added = frame.byteLength - entry.frame.byteLength
      this.waitingForHeadroomBytes += added
      entry.frame = frame
      return added
    }
    const sendBuffer = this.settleQueue()
    const held = sendBuffer.find((entry) => entry.channelIx === ix && entry.seq === seq)
    if (held === undefined) return null
    const away = this.owner.reconnecting || this.channels.get(ix)?.state.tag === 'pending'
    if (!away && !this.isBehind(sendBuffer.find((entry) => entry.channelIx === ix)!)) return null
    const added = frame.byteLength - held.frame.byteLength
    held.frame = frame
    return added
  }

  /** Sends a frame without a seq, or holds it as `send` does: a late WINDOW is still right, its limit cumulative. */
  sendUnsequenced(record: ChannelRecord, frame: Uint8Array<ArrayBuffer>, kind: OutboundFrameKind): void {
    if (!this.canSendImmediately(record)) {
      this.settleQueue().push({ frame, channelIx: record.ix, seq: undefined, waitingSince: performance.now() })
      return
    }
    this.owner.transport.sendFrame({ kind, frame })
  }

  /** What waits for headroom goes, from the front, while the transport has headroom. */
  sendWaiting(): void {
    if (this.waitingForHeadroom.length === 0) return
    this.dropQueues()
    const { transport } = this.owner
    let headroom = transport.headroom()
    while (headroom > 0 && this.waitingForHeadroom.length > 0) {
      const { frame, channelIx, seq } = this.waitingForHeadroom.shift()!
      this.waitingForHeadroomBytes -= frame.byteLength
      headroom -= frame.byteLength
      this.channels.get(channelIx)!.replay.push(seq, frame)
      transport.sendFrame({ kind: 'released', frame })
    }
  }

  /** Takes out the held frames of every channel the server doesn't await. */
  drainSendable(): OutboundFrame[] {
    return this.drain(this.isSendable)
  }

  /** As `drainSendable`, recording in `carriedFrom` the first seq each channel's frames carry. */
  drainForReconcile(carriedFrom: Map<number, number>): OutboundFrame[] {
    for (const { channelIx, seq } of this.settleQueue()) {
      if (seq !== undefined && this.isSendable(channelIx) && !carriedFrom.has(channelIx)) {
        carriedFrom.set(channelIx, seq)
      }
    }
    return this.drain(this.isSendable)
  }

  /** Takes out what `releasable` channels held, in order: from a held frame of a channel no RECONCILE named yet, or one
   *  a live wire doesn't take yet (`holdsBack`), every frame stays. Past the headroom, the sequenced frames join those
   *  waiting for it. */
  drain(releasable: (ix: number) => boolean): OutboundFrame[] {
    const frames: OutboundFrame[] = []
    const sendBuffer = this.settleQueue()
    // A limit replaces the one before it, so of a channel's queued limits only its newest WINDOW and MSG_WINDOW go.
    const newestLimit = new Map<number, number>()
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const key = limitKey(sendBuffer[readIx]!)
      if (key !== null) newestLimit.set(key, readIx)
    }
    let headroom = this.owner.transport.headroom()
    let writeIx = 0
    let behind = false
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const { frame, channelIx, seq } = entry
      if (behind || !releasable(channelIx)) {
        behind ||= this.channels.isUndeclared(channelIx) || this.holdsBack(channelIx)
        sendBuffer[writeIx++] = entry
        continue
      }
      const key = limitKey(entry)
      if (key !== null && newestLimit.get(key) !== readIx) continue
      if (seq !== undefined) {
        if (headroom <= 0 || this.waitingForHeadroom.length > 0) {
          this.waitForHeadroom({ ...entry, seq })
          continue
        }
        headroom -= frame.byteLength
        this.channels.get(channelIx)?.replay.push(seq, frame)
      }
      frames.push({ kind: 'released', frame })
    }
    sendBuffer.length = writeIx
    this.awaitedPrefix = 0
    return frames
  }

  /** Drops the `wanting` channels' held WANT frames: the wants a RECONCILE declares supersede them. */
  dropWantFrames(wanting: Set<number>): void {
    this.awaitedPrefix = 0
    this.sendBuffer = this.settleQueue().filter(
      ({ channelIx, frame }) => !wanting.has(channelIx) || frame[0] !== TAG.WANT,
    )
  }

  /** The channels whose frames wait to go. */
  queuedIxes(): Set<number> {
    const ixes = new Set<number>()
    for (const { channelIx } of this.settleQueue()) ixes.add(channelIx)
    for (const { channelIx } of this.waitingForHeadroom) ixes.add(channelIx)
    return ixes
  }

  /** Drops all the channel holds, its ATTACH_RESULT wait included. On the server, the RECONCILE that releases it, or a
   *  new wire's RECONCILE that leaves it out, ends it too. */
  release(record: ChannelRecord): void {
    this.channels.release(record)
    this.dropQueue(record.ix)
  }

  /** Drops the frames channel `ix` holds or has waiting, at the next read. */
  dropQueue(ix: number): void {
    if (this.sendBuffer.length === 0 && this.waitingForHeadroom.length === 0) return
    this.droppedQueues.add(ix)
  }

  clear(): void {
    this.sendBuffer = []
    this.awaitedPrefix = 0
    this.waitingForHeadroom = new Fifo()
    this.waitingForHeadroomBytes = 0
    this.droppedQueues.clear()
  }

  private isBehind({ waitingSince }: BufferedFrame): boolean {
    return performance.now() - waitingSince > this.owner.transport.behindAfter
  }

  private canSendImmediately(record: ChannelRecord): boolean {
    if (this.owner.sendsOpen) return !record.awaited
    return this.owner.takesLive(record) && !this.holdsAhead()
  }

  /** Whether a frame waits that a later one must not pass: any but one of a channel the server awaits. */
  private holdsAhead(): boolean {
    const sendBuffer = this.settleQueue()
    for (; this.awaitedPrefix < sendBuffer.length; this.awaitedPrefix++) {
      const entry = sendBuffer[this.awaitedPrefix]
      if (entry === undefined || this.channels.get(entry.channelIx)?.awaited !== true) return true
    }
    return false
  }

  private readonly isSendable = (ix: number): boolean => this.channels.get(ix)?.awaited !== true

  /** Whether a held frame of channel `ix` may go now. */
  private readonly isRunnable = (ix: number): boolean => {
    const record = this.channels.get(ix)
    if (record === undefined || record.awaited) return false
    return this.owner.sendsOpen || this.owner.takesLive(record)
  }

  /** Sends what waits that may go now, in order: at an attachment, once a frame that held the rest back went, and once
   *  headroom returns. */
  sendRunnable(): void {
    this.awaitedPrefix = 0
    if (!this.owner.sendsOpen && !this.owner.liveWire) return
    this.sendWaiting()
    const { transport } = this.owner
    for (const frame of this.drain(this.isRunnable)) transport.sendFrame(frame)
  }

  private waitForHeadroom(entry: HeldFrame): void {
    this.waitingForHeadroom.push(entry)
    this.waitingForHeadroomBytes += entry.frame.byteLength
  }

  /** While a RECONCILE is out on a live wire, whether channel `ix`'s held frame holds back the frames behind it. */
  private holdsBack(ix: number): boolean {
    if (this.owner.sendsOpen || !this.owner.liveWire) return false
    const record = this.channels.get(ix)
    return record !== undefined && !record.awaited && !this.owner.takesLive(record)
  }

  /** `sendBuffer`; while the wire takes no sends, the frames waiting for headroom move to its front, in seq order. */
  private settleQueue(): BufferedFrame[] {
    this.dropQueues()
    if (!this.owner.sendsOpen && !this.owner.liveWire && this.waitingForHeadroom.length > 0) {
      this.sendBuffer = [...this.waitingForHeadroom, ...this.sendBuffer]
      this.awaitedPrefix = 0
      this.waitingForHeadroom = new Fifo()
      this.waitingForHeadroomBytes = 0
    }
    return this.sendBuffer
  }

  private dropQueues(): void {
    if (this.droppedQueues.size === 0) return
    const dropped = this.droppedQueues
    this.sendBuffer = this.sendBuffer.filter(({ channelIx }) => !dropped.has(channelIx))
    this.awaitedPrefix = 0
    if (this.waitingForHeadroom.length > 0) {
      const kept = new Fifo<HeldFrame>()
      for (const entry of this.waitingForHeadroom) {
        if (dropped.has(entry.channelIx)) this.waitingForHeadroomBytes -= entry.frame.byteLength
        else kept.push(entry)
      }
      this.waitingForHeadroom = kept
    }
    this.droppedQueues = new Set()
  }
}
