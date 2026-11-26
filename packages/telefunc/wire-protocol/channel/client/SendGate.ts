export { SendGate }
export type { FrameAt, SendGateOwner }

import { assertUsage } from '../../../utils/assert.js'
import type { ChannelRecord, ChannelTable } from './ChannelTable.js'
import type { ClientChannelTransport, MuxChannel, OutboundFrame, OutboundFrameKind } from './ClientConnection.js'
import { Fifo } from '../../../utils/Fifo.js'
import { TAG } from '../../wire/tags.js'
import { EVERY_STREAM, OWN_STREAM } from '../ownStream.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  /** The kind it was sent with, which it keeps while it waits. */
  kind: OutboundFrameKind
  channelIx: number
  seq?: number
  waitingSince: number
  receivedAt?: number
}
type HeldFrame = BufferedFrame & { seq: number }
/** The frame a channel sends at `seq`, made for its `record`. */
type FrameAt = (record: ChannelRecord, seq: number) => Uint8Array<ArrayBuffer>
/** The stream a sequenced frame is of, and whether a newer frame of that stream may take its place while it waits. */
type SendPlace = { stream?: string; replaceable?: boolean }

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
  /** The largest frame the server accepts from `channel`. */
  maxFrameBytesOf(channel: MuxChannel): number
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
      this.settleQueue().push({ frame, kind, channelIx: record.ix, seq, waitingSince: performance.now() })
      return
    }
    // What waited goes first.
    this.sendWaiting()
    const { transport } = this.owner
    if (this.waitingForHeadroom.length === 0 && transport.headroom() > 0) {
      record.replay.push(seq, frame, kind)
      transport.sendFrame({ kind, frame })
      return
    }
    this.waitForHeadroom({ frame, kind, channelIx: record.ix, seq, waitingSince: performance.now() })
  }

  /** Sends at `channel`'s next seq the frame `frameAt` makes, and takes the seq only then, so a frame refused as larger
   *  than the server accepts takes none. `onQueued` runs before the wire has it. Returns the frame, or null once the
   *  channel sends nothing more. */
  sendSequenced(
    channel: MuxChannel,
    kind: 'data' | 'ack' | 'control',
    frameAt: FrameAt,
    onQueued?: (seq: number) => void,
    { stream = OWN_STREAM, replaceable = false }: SendPlace = {},
  ): Uint8Array<ArrayBuffer> | null {
    const record = this.channels.of(channel)
    if (record === undefined) return null
    const frame = this.fitting(channel, frameAt(record, record.replay.seq + 1))
    const seq = record.replay.nextSeq()
    takePlace(record, stream, replaceable ? seq : null)
    onQueued?.(seq)
    // A data frame after which its channel is out of the server's credit goes at once: nothing the channel sends next
    // can join its batch POST before the server has it and grants more.
    const goesNow = kind === 'data' && !channel._attach.sendCredit.leavesCredit(frame.byteLength)
    this.send(record, seq, frame, goesNow ? 'control' : kind)
    return frame
  }

  /** Sends the frame `frameOf` makes for `channel` without a seq, or holds it as `send` does: a late WINDOW is still
   *  right, its limit cumulative. A WINDOW goes by the arrival of the newest seq it names. False, sending nothing, once
   *  the channel sends nothing more. */
  sendUnsequenced(
    channel: MuxChannel,
    kind: OutboundFrameKind,
    frameOf: (record: ChannelRecord) => Uint8Array<ArrayBuffer>,
  ): boolean {
    const record = this.channels.of(channel)
    if (record === undefined) return false
    const frame = frameOf(record)
    const receivedAt = frame[0] === TAG.WINDOW ? record.receivedAt : undefined
    if (!this.canSendImmediately(record)) {
      this.settleQueue().push({ frame, kind, channelIx: record.ix, waitingSince: performance.now(), receivedAt })
      return true
    }
    this.owner.transport.sendFrame({ kind, frame, receivedAt })
    return true
  }

  /** Throws where the frame `frameAt` makes at `channel`'s next seq is larger than the server accepts, as its send
   *  would: for a send held before it takes a seq, as a frame's header has the same size at any seq. */
  assertFits(channel: MuxChannel, frameAt: FrameAt): void {
    const record = this.channels.of(channel)
    if (record !== undefined) this.fitting(channel, frameAt(record, record.replay.seq + 1))
  }

  /** Puts the frame `frameAt` makes in the place of `channel`'s replaceable frame of `stream` while that waits, behind
   *  or not, and is the channel's last, and says whether it did. */
  replaceHeld(channel: MuxChannel, stream: string, frameAt: FrameAt): boolean {
    const record = this.channels.of(channel)
    const seq = record?.replaceable?.get(stream)
    if (record === undefined || seq !== record.replay.seq) return false
    record.replaceable?.delete(stream)
    const frame = frameAt(record, seq)
    const { ix } = record
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

  /** Puts the frame `frameAt` makes in the place of `channel`'s replaceable frame of `stream` while that waits and the
   *  page is behind, and returns that frame's seq and the bytes it adds over it: null once it went, as the wire then has
   *  it, once the channel sent a frame of the stream after it, or of every stream, which must not arrive ahead of what
   *  takes its place, or while the link keeps up. A page with no wire, and a channel no wire has opened yet, have no
   *  link to keep up with. */
  replaceWaiting(channel: MuxChannel, stream: string, frameAt: FrameAt): { seq: number; added: number } | null {
    const record = this.channels.of(channel)
    const seq = record?.replaceable?.get(stream)
    if (record === undefined || seq === undefined) return null
    const added = this.replaceAt(record, seq, this.fitting(channel, frameAt(record, seq)))
    return added === null ? null : { seq, added }
  }

  /** `replaceWaiting`'s swap of `record`'s frame `seq` for `frame`: the bytes it adds, or null. */
  private replaceAt(record: ChannelRecord, seq: number, frame: Uint8Array<ArrayBuffer>): number | null {
    const { ix } = record
    for (const entry of this.waitingForHeadroom) {
      if (entry.channelIx !== ix || entry.seq !== seq) continue
      if (!this.isBehind(this.waitingForHeadroom.peek() ?? entry)) return null
      const added = frame.byteLength - entry.frame.byteLength
      this.waitingForHeadroomBytes += added
      entry.frame = frame
      return added
    }
    const sendBuffer = this.settleQueue()
    const held = sendBuffer.find((entry) => entry.channelIx === ix && entry.seq === seq)
    if (held === undefined) return null
    const away = this.owner.reconnecting || record.state.tag === 'pending'
    if (!away && !this.isBehind(sendBuffer.find((entry) => entry.channelIx === ix) ?? held)) return null
    const added = frame.byteLength - held.frame.byteLength
    held.frame = frame
    return added
  }

  /** `frame`, where it is no larger than the server accepts from `channel`: the server ends any wire a larger frame
   *  arrives on, and the replay would send it again on the next one. */
  private fitting(channel: MuxChannel, frame: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
    const maxFrameBytes = this.owner.maxFrameBytesOf(channel)
    assertUsage(
      frame.byteLength <= maxFrameBytes,
      `Channel message too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`,
    )
    return frame
  }

  /** What waits for headroom goes, from the front, while the transport has headroom. */
  sendWaiting(): void {
    if (this.waitingForHeadroom.length === 0) return
    this.dropQueues()
    const { transport } = this.owner
    let headroom = transport.headroom()
    while (headroom > 0) {
      const held = this.waitingForHeadroom.shift()
      if (held === undefined) return
      const { frame, kind, channelIx, seq } = held
      this.waitingForHeadroomBytes -= frame.byteLength
      headroom -= frame.byteLength
      this.channels.get(channelIx)?.replay.push(seq, frame, kind)
      transport.sendFrame({ kind, frame, released: true })
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
      const { frame, kind, channelIx, seq, receivedAt } = entry
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
        this.channels.get(channelIx)?.replay.push(seq, frame, kind)
      }
      frames.push({ kind, frame, released: true, receivedAt })
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

  /** Drops the frames of channel `ix` not yet sent: those it holds or has waiting, at the next read, and those the
   *  transport holds. */
  dropQueue(ix: number): void {
    this.owner.transport.dropQueued(ix)
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

/** A frame of `stream` that must arrive ends the place of the stream's replaceable frame that still waits, or of every
 *  stream's: what was sent after a frame never arrives ahead of it in an older one's place. `seq`: the frame takes that
 *  place itself. */
function takePlace(record: ChannelRecord, stream: string, seq: number | null): void {
  if (stream === EVERY_STREAM) record.replaceable?.clear()
  else record.replaceable?.delete(stream)
  if (seq !== null) (record.replaceable ??= new Map()).set(stream, seq)
}
