export { IndexedPeer }
export type { PeerSender }

import { ACK_STATUS, encode } from '../shared-ws.js'
import type { AckResultStatus } from '../shared-ws.js'
import { ReplayBuffer } from '../replay-buffer.js'

interface PeerSender {
  send(frame: Uint8Array, onCommit?: () => void): void | Promise<void>
}

/** Wraps a crossws peer, encodes frames with a fixed channel index.
 *  Assigns sequence numbers. Frames are added to the replay buffer only once
 *  they are committed to a transport send path. */
class IndexedPeer {
  constructor(
    private sender: PeerSender,
    private index: number,
    private replay: ReplayBuffer,
  ) {}

  sendText(data: string): void | Promise<void> {
    const seq = this.replay.nextSeq()
    const frame = encode.text(this.index, data, seq)
    return this.sender.send(frame, () => this.replay.push(seq, frame))
  }

  /** Send a text frame that requests an ack response from the receiver. Returns seq. */
  sendTextAckReq(data: string, onQueued?: (seq: number) => void): number {
    const seq = this.replay.nextSeq()
    const frame = encode.textAckReq(this.index, data, seq)
    onQueued?.(seq)
    this.sender.send(frame, () => this.replay.push(seq, frame))
    return seq
  }

  sendBinary(data: Uint8Array): void | Promise<void> {
    const seq = this.replay.nextSeq()
    const frame = encode.binary(this.index, data, seq)
    return this.sender.send(frame, () => this.replay.push(seq, frame, true))
  }

  /** Send a binary frame that requests an ack response from the receiver. Returns seq. */
  sendBinaryAckReq(data: Uint8Array, onQueued?: (seq: number) => void): number {
    const seq = this.replay.nextSeq()
    const frame = encode.binaryAckReq(this.index, data, seq)
    onQueued?.(seq)
    this.sender.send(frame, () => this.replay.push(seq, frame, true))
    return seq
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

  sendError(): void {
    try {
      this.sender.send(encode.error(this.index))
    } catch {
      /* transport may already be closed */
    }
  }

  sendWindowUpdate(bytes: number): void {
    try {
      this.sender.send(encode.window(this.index, bytes))
    } catch {
      /* transport may already be closed */
    }
  }

  sendPublish(data: string): void {
    const seq = this.replay.nextSeq()
    const frame = encode.publish(this.index, data, seq)
    try {
      this.sender.send(frame, () => this.replay.push(seq, frame))
    } catch {
      /* transport may already be closed */
    }
  }

  sendPublishBinary(data: Uint8Array): void {
    const seq = this.replay.nextSeq()
    const frame = encode.publishBinary(this.index, data, seq)
    try {
      this.sender.send(frame, () => this.replay.push(seq, frame, true))
    } catch {
      /* transport may already be closed */
    }
  }
}
