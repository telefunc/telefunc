export { SendGate }
export type { SendGateOwner }

import type { ChannelRecord, ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport, OutboundFrame, OutboundFrameKind } from './connection.js'
import { Fifo } from '../../../utils/Fifo.js'
import { TAG } from '../../wire/tags.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}
type HeldFrame = BufferedFrame & { seq: number }

/** Its channel and tag, for a WINDOW or MSG_WINDOW; null for any other frame. */
function limitKey({ frame, channelIx }: BufferedFrame): number | null {
  return frame[0] === TAG.WINDOW || frame[0] === TAG.MSG_WINDOW ? channelIx * 256 + frame[0] : null
}

/** The connection, whose transport takes what the gate lets go. */
interface SendGateOwner {
  /** No RECONCILE or upgrade holds sends back. */
  readonly sendsOpen: boolean
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
  /** Frames held while sends are, or for their channel's ATTACH_RESULT. */
  private sendBuffer: BufferedFrame[] = []
  /** Sequenced frames waiting only for the transport's headroom, in order: a channel's next send waits behind them. */
  private waitingForHeadroom = new Fifo<HeldFrame>()
  private waitingForHeadroomBytes = 0
  /** Channels whose queued frames go, all in one pass when they're next read (see `settleQueue`). */
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

  /** Sends, or queues while sends are held or the transport lacks headroom; one sent at once passes the settle queue. */
  send(record: ChannelRecord, seq: number, frame: Uint8Array<ArrayBuffer>, kind: OutboundFrameKind): void {
    if (!this.canSendImmediately(record)) {
      this.settleQueue().push({ frame, channelIx: record.ix, seq })
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
    this.waitForHeadroom({ frame, channelIx: record.ix, seq })
  }

  /** Sends a frame without a seq, or queues it while its channel waits: a limit is cumulative, so stays right. */
  sendUnsequenced(record: ChannelRecord, frame: Uint8Array<ArrayBuffer>, kind: OutboundFrameKind): void {
    if (!this.canSendImmediately(record)) {
      this.settleQueue().push({ frame, channelIx: record.ix, seq: undefined })
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

  /** Takes out what the channels not awaited queued. */
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

  /** Takes out what `releasable` channels queued; past the headroom, their sequenced frames go behind those waiting. */
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
          this.waitForHeadroom({ frame, channelIx, seq })
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

  /** The `subscribing` channels' queued SUB/UNSUB frames go: the subscriptions a RECONCILE declares supersede them. */
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

  /** What channel `ix` queued goes, at the next read. */
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

  private canSendImmediately(record: ChannelRecord): boolean {
    return this.owner.sendsOpen && !record.awaited
  }

  private readonly isSendable = (ix: number): boolean => this.channels.get(ix)?.awaited !== true

  private waitForHeadroom(entry: HeldFrame): void {
    this.waitingForHeadroom.push(entry)
    this.waitingForHeadroomBytes += entry.frame.byteLength
  }

  /** sendBuffer, with what waited for headroom in front of it once sends are held: a reconcile takes them all in order. */
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
