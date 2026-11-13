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
  /** The page lost its wire and has none yet. */
  readonly reconnecting: boolean
  readonly transport: ClientChannelTransport
}

type SendGateOptions = {
  owner: SendGateOwner
  channels: ChannelTable
}

/** What the page's channels send: at once while sends are open and the transport has headroom, else queued, in order. */
class SendGate {
  private readonly owner: SendGateOwner
  private readonly channels: ChannelTable
  /** Frames held in order while sends are held back, or till the server attaches their channel (ATTACH_RESULT). */
  private sendBuffer: BufferedFrame[] = []
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

  /** Puts `frame` in the place of channel `ix`'s frame `seq` while that waits and the page is behind, and returns the
   *  bytes it adds over that one: null once it went, as the wire then has it, or while the link keeps up. A page with no
   *  wire has no link to keep up with. */
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
    if (!this.owner.reconnecting && !this.isBehind(sendBuffer.find((entry) => entry.channelIx === ix)!)) return null
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

  /** Takes out what `releasable` channels held; past the headroom, their sequenced frames join those waiting for it. */
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
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const { frame, channelIx, seq } = entry
      if (!releasable(channelIx)) {
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
    return frames
  }

  /** Drops the `subscribing` channels' held SUB/UNSUB frames: the subscriptions a RECONCILE declares supersede them. */
  dropSubscriptionFrames(subscribing: Set<number>): void {
    this.sendBuffer = this.settleQueue().filter(
      ({ channelIx, frame }) =>
        !subscribing.has(channelIx) || (frame[0] !== TAG.BROADCAST_SUB && frame[0] !== TAG.BROADCAST_UNSUB),
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
    this.waitingForHeadroom = new Fifo()
    this.waitingForHeadroomBytes = 0
    this.droppedQueues.clear()
  }

  private isBehind({ waitingSince }: BufferedFrame): boolean {
    return performance.now() - waitingSince > this.owner.transport.behindAfter
  }

  private canSendImmediately(record: ChannelRecord): boolean {
    return this.owner.sendsOpen && !record.awaited
  }

  private readonly isSendable = (ix: number): boolean => this.channels.get(ix)?.awaited !== true

  private waitForHeadroom(entry: HeldFrame): void {
    this.waitingForHeadroom.push(entry)
    this.waitingForHeadroomBytes += entry.frame.byteLength
  }

  /** `sendBuffer`; while sends are held back, the frames waiting for headroom move to its front, keeping seq order. */
  private settleQueue(): BufferedFrame[] {
    this.dropQueues()
    if (!this.owner.sendsOpen && this.waitingForHeadroom.length > 0) {
      this.sendBuffer = [...this.waitingForHeadroom, ...this.sendBuffer]
      this.waitingForHeadroom = new Fifo()
      this.waitingForHeadroomBytes = 0
    }
    return this.sendBuffer
  }

  private dropQueues(): void {
    if (this.droppedQueues.size === 0) return
    const dropped = this.droppedQueues
    this.sendBuffer = this.sendBuffer.filter(({ channelIx }) => !dropped.has(channelIx))
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
