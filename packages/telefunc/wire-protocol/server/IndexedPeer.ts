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

/** Wraps a crossws peer, encodes frames with a fixed channel index.
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
    this.sender.send(frame, () => this.replay.push(seq, frame, true))
  }

  /** Send a binary frame that requests an ack response from the receiver. `onQueued` gets its seq and payload bytes. */
  sendBinaryAckReq(data: Uint8Array, onQueued: (seq: number, bytes: number) => void): void {
    const seq = this.replay.nextSeq()
    const frame = encode.binaryAckReq(this.index, data, seq)
    onQueued(seq, data.byteLength)
    this.sender.send(frame, () => this.replay.push(seq, frame, true))
  }

  /** Send an acknowledgement response for a message the client sent.
   *  ACK_RES frames use the normal sequenced send path and are replayable on reconnect. */
  sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const seq = this.replay.nextSeq()
    const frame = encode.ackRes(this.index, seq, ackedSeq, result, status)
    try {
      this.sender.send(frame, () => this.replay.push(seq, frame))
    } catch {
      /* transport may already be closed */
    }
  }

  sendCloseRequest(timeoutMs: number): void {
    try {
      this.sender.send(encode.close(this.index, timeoutMs))
    } catch {
      /* transport may already be closed */
    }
  }

  sendCloseAck(): void {
    try {
      this.sender.send(encode.closeAck(this.index))
    } catch {
      /* transport may already be closed */
    }
  }

  sendAbort(abortValue: string): void {
    try {
      this.sender.send(encode.abort(this.index, abortValue))
    } catch {
      /* transport may already be closed */
    }
  }

  sendError(reason: ErrorReason): void {
    try {
      this.sender.send(encode.error(this.index, reason))
    } catch {
      /* transport may already be closed */
    }
  }

  sendByteWindowUpdate(limit: number): void {
    try {
      this.sender.send(encode.window(this.index, limit))
    } catch {
      /* transport may already be closed */
    }
  }

  sendMsgWindowUpdate(limit: number): void {
    try {
      this.sender.send(encode.msgWindow(this.index, limit))
    } catch {
      /* transport may already be closed */
    }
  }

  /** The totals cover every frame through the latest seq. */
  sendSent(bytes: number, messages: number): void {
    try {
      this.sender.send(encode.sent(this.index, this.replay.seq, bytes, messages))
    } catch {
      /* transport may already be closed */
    }
  }

  sendBdpPing(): void {
    try {
      this.sender.send(encode.bdpPing(this.index))
    } catch {
      /* transport may already be closed */
    }
  }

  sendBdpPingAck(): void {
    try {
      this.sender.send(encode.bdpPingAck(this.index))
    } catch {
      /* transport may already be closed */
    }
  }

  /** Returns the frame's payload byte count. */
  sendPublish(data: string): number {
    const seq = this.replay.nextSeq()
    const frame = encode.publish(this.index, data, seq)
    try {
      this.sender.send(frame, () => this.replay.push(seq, frame))
    } catch {
      /* transport may already be closed */
    }
    return payloadBytes(frame)
  }

  /** Returns the frame's payload byte count. */
  sendPublishBinary(data: Uint8Array): number {
    const seq = this.replay.nextSeq()
    const frame = encode.publishBinary(this.index, data, seq)
    try {
      this.sender.send(frame, () => this.replay.push(seq, frame, true))
    } catch {
      /* transport may already be closed */
    }
    return payloadBytes(frame)
  }
}
