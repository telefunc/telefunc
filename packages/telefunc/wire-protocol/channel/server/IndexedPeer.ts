export { IndexedPeer }
export type { PeerSender }

import { ACK_STATUS, encode } from '../../wire/frame.js'
import type { AckResultStatus, SequencedEncoder } from '../../wire/frame.js'
import { PublishBody } from './PublishBody.js'
import type { ServerReplay } from './ServerReplay.js'
import type { SendWait } from '../../flowControl/SendWaits.js'
import type { Urgency } from '../../flowControl/credit.js'
import type { WireQueue } from '../../flowControl/server/waitingPublishes.js'

interface PeerSender {
  /** What the wire holds back for its page. */
  readonly queue: WireQueue
  /** How long a lossy page on the wire may leave a publish unconfirmed before it counts as behind, where a confirmation
   *  takes at least `roundTrip` ms: `Infinity` before one was measured. */
  behindAfter(roundTrip: number): number
  send(frame: Uint8Array): void
  /** Sends `frame` in the next message the wire sends, where it can carry one (see `PieceSender.carry`). */
  carry(frame: Uint8Array): void
  /** Bytes the wire holds for the peer, not yet written out; `undefined` where the runtime can't tell. */
  bufferedAmount(): number | undefined
  /** The wait the wire reports for the frames that go at once (see `SocketWait`). */
  sendWait(): SendWait
  /** The bytes the wire delivered in the `ms` before the frame it's handling arrived. */
  deliveredBefore(ms: number): number
}

type IndexedPeerOptions = {
  sender: PeerSender
  index: number
  replay: ServerReplay
}

/** Encodes a channel's frames with its index; a sequenced one joins the replay buffer as it goes to the wire. */
class IndexedPeer {
  readonly sender: PeerSender
  readonly index: number
  private replay: ServerReplay

  constructor({ sender, index, replay }: IndexedPeerOptions) {
    this.sender = sender
    this.index = index
    this.replay = replay
  }

  /** Encodes `data` by `encodeFrame` at the next seq; `onQueued` gets its seq and bytes before the wire has it. Returns
   *  the bytes the frame counts against flow control. */
  sendSequenced<T>(encodeFrame: SequencedEncoder<T>, data: T, onQueued?: (seq: number, bytes: number) => void): number {
    const seq = this.replay.nextSeq()
    const frame = encodeFrame(this.index, data, seq)
    onQueued?.(seq, frame.byteLength)
    this.commit(seq, frame)
    return frame.byteLength
  }

  /** Sends `body` behind a head naming this channel and its next seq; both join the replay and the wire in one call. */
  sendPublish(body: PublishBody): void {
    this.sendBehindHead(this.replay.nextSeq(), body)
  }

  /** Sends `data` by `encodeFrame` at the next seq as a publish of this channel's own, which a newer one may replace while
   *  it waits on the wire. Returns the bytes the frame counts against flow control. */
  sendOwn<T>(encodeFrame: SequencedEncoder<T>, data: T): number {
    const seq = this.replay.nextSeq()
    const frame = encodeFrame(this.index, data, seq)
    this.sendBehindHead(seq, new PublishBody(frame))
    return frame.byteLength
  }

  /** Send an acknowledgement response for a message the client sent.
   *  ACK_RES frames use the normal sequenced send path and are replayable on reconnect. */
  sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const seq = this.replay.nextSeq()
    this.commit(seq, encode.ackRes(this.index, seq, ackedSeq, result, status))
  }

  /** Returns its seq. */
  sendCloseRequest(timeoutMs: number): number {
    const seq = this.replay.nextSeq()
    this.commit(seq, encode.close(this.index, timeoutMs, seq))
    return seq
  }

  sendCloseAck(): void {
    const seq = this.replay.nextSeq()
    this.commit(seq, encode.closeAck(this.index, seq))
  }

  /** `lastSeq`: the last seq the server has of what the page sent on the channel. */
  sendByteLimit(limit: number, lastSeq: number, urgency: Urgency): void {
    this.sendLimit(encode.window(this.index, limit, lastSeq), urgency)
  }

  sendMessageLimit(limit: number, urgency: Urgency): void {
    this.sendLimit(encode.msgWindow(this.index, limit), urgency)
  }

  sendBdpPing(probe: number): void {
    this.sender.send(encode.bdpPing(this.index, probe))
  }

  sendBdpPingAck(probe: number, backlog: number, pathRtt: number): void {
    this.sender.send(encode.bdpPingAck(this.index, probe, backlog, pathRtt))
  }

  private sendLimit(frame: Uint8Array, urgency: Urgency): void {
    if (urgency === 'acknowledgement') this.sender.carry(frame)
    else this.sender.send(frame)
  }

  private sendBehindHead(seq: number, body: PublishBody): void {
    const head = encode.publishTo(this.index, seq)
    this.replay.pushPublish(seq, head, body)
    this.sender.send(head)
    this.sender.send(body.frame)
  }

  /** Sends `frame` of `seq`, kept to replay, so a frame a dead wire lost goes again. */
  private commit(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    this.replay.push(seq, frame)
    this.sender.send(frame)
  }
}
