export { IndexedPeer }
export type { PeerSender }

import { ACK_STATUS, encode, payloadBytes } from '../shared-ws.js'
import type { AckResultStatus, ErrorReason } from '../shared-ws.js'
import { ReplayBuffer } from '../replay-buffer.js'

interface PeerSender {
  send(frame: Uint8Array, onCommit?: () => void): void
  /** Bytes the wire holds for the peer, not yet written out; `undefined` where the runtime can't tell. */
  bufferedAmount(): number | undefined
}

/** Wraps a `PeerSender`, encodes frames with a fixed channel index.
 *  Assigns sequence numbers. Frames are added to the replay buffer only once
 *  they are committed to a transport send path. */
class IndexedPeer {
  constructor(
    readonly sender: PeerSender,
    private index: number,
    private replay: ReplayBuffer,
  ) {}

  /** Returns the frame's payload byte count (the caller's flow-control unit). */
  sendText(data: string): number {
    const seq = this.replay.nextSeq()
    const frame = encode.text(this.index, data, seq)
    this.sender.send(frame, () => this.replay.push(seq, frame))
    return payloadBytes(frame)
  }

  /** Send a text frame that requests an ack response from the receiver. `onQueued` gets its seq and payload bytes. */
  sendTextAckReq(data: string, onQueued: (seq: number, bytes: number) => void): void {
    const seq = this.replay.nextSeq()
    const frame = encode.textAckReq(this.index, data, seq)
    onQueued(seq, payloadBytes(frame))
    this.sender.send(frame, () => this.replay.push(seq, frame))
  }

  sendBinary(data: Uint8Array): void {
    const seq = this.replay.nextSeq()
    const frame = encode.binary(this.index, data, seq)
    this.sender.send(frame, () => this.replay.push(seq, frame))
  }

  /** Send a binary frame that requests an ack response from the receiver. `onQueued` gets its seq and payload bytes. */
  sendBinaryAckReq(data: Uint8Array, onQueued: (seq: number, bytes: number) => void): void {
    const seq = this.replay.nextSeq()
    const frame = encode.binaryAckReq(this.index, data, seq)
    onQueued(seq, data.byteLength)
    this.sender.send(frame, () => this.replay.push(seq, frame))
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
  sendByteWindowUpdate(limit: number, lastSeq: number): void {
    this.sendCtrl(encode.window(this.index, limit, lastSeq))
  }

  sendMsgWindowUpdate(limit: number): void {
    this.sendCtrl(encode.msgWindow(this.index, limit))
  }

  sendBdpPing(probe: number): void {
    this.sendCtrl(encode.bdpPing(this.index, probe))
  }

  sendBdpPingAck(probe: number, starved: boolean, pathRtt: number): void {
    this.sendCtrl(encode.bdpPingAck(this.index, probe, starved, pathRtt))
  }

  /** Returns the frame's payload byte count. */
  sendPublish(data: string): number {
    const seq = this.replay.nextSeq()
    const frame = encode.publish(this.index, data, seq)
    this.sendSequenced(seq, frame)
    return payloadBytes(frame)
  }

  /** Returns the frame's payload byte count. */
  sendPublishBinary(data: Uint8Array): number {
    const seq = this.replay.nextSeq()
    const frame = encode.publishBinary(this.index, data, seq)
    this.sendSequenced(seq, frame)
    return payloadBytes(frame)
  }

  /** Replayed once committed, so a frame a dead wire lost goes again. */
  private sendSequenced(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    try {
      this.sender.send(frame, () => this.replay.push(seq, frame))
    } catch {
      /* transport may already be closed */
    }
  }

  private sendCtrl(frame: Uint8Array<ArrayBuffer>): void {
    try {
      this.sender.send(frame)
    } catch {
      /* transport may already be closed */
    }
  }
}
