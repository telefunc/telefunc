export { IndexedPeer }
export type { PeerSender }

import { ACK_STATUS, encode, chargedBytes } from '../../wire/connectionFrame.js'
import type { AckResultStatus, ErrorReason } from '../../wire/connectionFrame.js'
import { ReplayBuffer } from '../ReplayBuffer.js'
import type { SendWait } from '../../flowControl/Link.js'
import type { ByteWindowKind } from '../../flowControl/FlowControl.js'

interface PeerSender {
  send(frame: Uint8Array, onCommit?: () => void): void
  /** Sends `frame` in the next message the wire sends, where it can carry one (see `PieceSender.carry`). */
  carry(frame: Uint8Array): void
  /** Bytes the wire holds for the peer, not yet written out; `undefined` where the runtime can't tell. */
  bufferedAmount(): number | undefined
  /** The wait the wire reports for the frames that go at once (see `SocketWait`). */
  sendWait(): SendWait
  /** The bytes the wire delivered in the `ms` before the frame it's handling arrived. */
  deliveredBeforeFrame(ms: number): number
}

type IndexedPeerOptions = {
  sender: PeerSender
  index: number
  replay: ReplayBuffer
}

/** Encodes a channel's frames with its index; a sequenced one joins the replay buffer once its wire commits it. */
class IndexedPeer {
  readonly sender: PeerSender
  private index: number
  private replay: ReplayBuffer

  constructor({ sender, index, replay }: IndexedPeerOptions) {
    this.sender = sender
    this.index = index
    this.replay = replay
  }

  /** Returns the bytes the frame counts against flow control. */
  sendText(data: string): number {
    const seq = this.replay.nextSeq()
    const frame = encode.text(this.index, data, seq)
    this.sendSequenced(seq, frame)
    return chargedBytes(frame)
  }

  /** Send a text frame that requests an ack response from the receiver. `onQueued` gets its seq and charged bytes. */
  sendTextAckReq(data: string, onQueued: (seq: number, bytes: number) => void): void {
    const seq = this.replay.nextSeq()
    const frame = encode.textAckReq(this.index, data, seq)
    onQueued(seq, chargedBytes(frame))
    this.sendSequenced(seq, frame)
  }

  /** Returns the bytes the frame counts against flow control. */
  sendBinary(data: Uint8Array): number {
    const seq = this.replay.nextSeq()
    const frame = encode.binary(this.index, data, seq)
    this.sendSequenced(seq, frame)
    return chargedBytes(frame)
  }

  /** Send a binary frame that requests an ack response from the receiver. `onQueued` gets its seq and charged bytes. */
  sendBinaryAckReq(data: Uint8Array, onQueued: (seq: number, bytes: number) => void): void {
    const seq = this.replay.nextSeq()
    const frame = encode.binaryAckReq(this.index, data, seq)
    onQueued(seq, chargedBytes(frame))
    this.sendSequenced(seq, frame)
  }

  /** Send an acknowledgement response for a message the client sent.
   *  ACK_RES frames use the normal sequenced send path and are replayable on reconnect. */
  sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const seq = this.replay.nextSeq()
    this.sendSequenced(seq, encode.ackRes(this.index, seq, ackedSeq, result, status))
  }

  /** Returns its seq. */
  sendCloseRequest(timeoutMs: number): number {
    const seq = this.replay.nextSeq()
    this.sendSequenced(seq, encode.close(this.index, timeoutMs, seq))
    return seq
  }

  sendCloseAck(): void {
    const seq = this.replay.nextSeq()
    this.sendSequenced(seq, encode.closeAck(this.index, seq))
  }

  sendAbort(abortValue: string): void {
    const seq = this.replay.nextSeq()
    this.sendSequenced(seq, encode.abort(this.index, abortValue, seq))
  }

  sendError(reason: ErrorReason): void {
    const seq = this.replay.nextSeq()
    this.sendSequenced(seq, encode.error(this.index, reason, seq))
  }

  /** `lastSeq`: the last seq the server has of what the page sent on the channel. */
  sendByteWindowUpdate(limit: number, lastSeq: number, kind: ByteWindowKind): void {
    const frame = encode.window(this.index, limit, lastSeq)
    if (kind === 'acknowledgement') this.sender.carry(frame)
    else this.sender.send(frame)
  }

  sendMsgWindowUpdate(limit: number): void {
    this.sender.send(encode.msgWindow(this.index, limit))
  }

  sendBdpPing(probe: number): void {
    this.sender.send(encode.bdpPing(this.index, probe))
  }

  sendBdpPingAck(probe: number, backlog: number, pathRtt: number): void {
    this.sender.send(encode.bdpPingAck(this.index, probe, backlog, pathRtt))
  }

  /** Returns the bytes the frame counts against flow control. */
  sendPublish(data: string): number {
    const seq = this.replay.nextSeq()
    const frame = encode.publish(this.index, data, seq)
    this.sendSequenced(seq, frame)
    return chargedBytes(frame)
  }

  /** Returns the bytes the frame counts against flow control. */
  sendPublishBinary(data: Uint8Array): number {
    const seq = this.replay.nextSeq()
    const frame = encode.publishBinary(this.index, data, seq)
    this.sendSequenced(seq, frame)
    return chargedBytes(frame)
  }

  /** Replayed once committed, so a frame a dead wire lost goes again. */
  private sendSequenced(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    this.sender.send(frame, () => this.replay.push(seq, frame))
  }
}
