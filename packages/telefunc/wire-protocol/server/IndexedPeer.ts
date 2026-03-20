export { IndexedPeer }
export type { PeerSender }

import { encode, encodeCtrl } from '../shared-ws.js'
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

  sendText(data: string): void {
    const seq = this.replay.nextSeq()
    const frame = encode.text(this.index, data, seq)
    this.sender.send(frame, () => this.replay.push(seq, frame))
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
    return this.sender.send(frame, () => this.replay.push(seq, frame))
  }

  /** Send an acknowledgement response for a message the client sent.
   *  ACK_RES frames use the normal sequenced send path and are replayable on reconnect. */
  sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = 'ok'): void {
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
      this.sender.send(encodeCtrl({ t: 'close', ix: this.index, timeoutMs }))
    } catch {
      /* transport may already be closed */
    }
  }

  sendCloseAck(): void {
    try {
      this.sender.send(encodeCtrl({ t: 'close-ack', ix: this.index }))
    } catch {
      /* transport may already be closed */
    }
  }

  sendAbort(abortValue: string): void {
    try {
      this.sender.send(encodeCtrl({ t: 'abort', ix: this.index, abortValue }))
    } catch {
      /* transport may already be closed */
    }
  }

  sendError(): void {
    try {
      this.sender.send(encodeCtrl({ t: 'error', ix: this.index }))
    } catch {
      /* transport may already be closed */
    }
  }
}
