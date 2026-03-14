export { IndexedPeer }

import type { Peer } from 'crossws'
import { encode, encodeCtrl } from '../shared-ws.js'
import { ReplayBuffer } from '../replay-buffer.js'

/** Wraps a crossws peer, encodes frames with a fixed channel index.
 *  Assigns sequence numbers and stores frames in the replay buffer. */
class IndexedPeer {
  constructor(
    private ws: Peer,
    private index: number,
    private replay: ReplayBuffer,
  ) {}

  sendText(data: string): void {
    const seq = this.replay.nextSeq()
    const frame = encode.text(this.index, data, seq)
    this.replay.push(seq, frame)
    this.ws.send(frame)
  }

  /** Send a text frame that requests an ack response from the receiver. Returns seq. */
  sendTextAckReq(data: string): number {
    const seq = this.replay.nextSeq()
    const frame = encode.textAckReq(this.index, data, seq)
    this.replay.push(seq, frame)
    this.ws.send(frame)
    return seq
  }

  sendBinary(data: Uint8Array): void {
    const seq = this.replay.nextSeq()
    const frame = encode.binary(this.index, data, seq)
    this.replay.push(seq, frame)
    this.ws.send(frame)
  }

  /** Send an acknowledgement response for a message the client sent.
   *  ACK_RES frames use the normal sequenced send path and are replayable on reconnect. */
  sendAckRes(ackedSeq: number, result: string): void {
    const seq = this.replay.nextSeq()
    const frame = encode.ackRes(this.index, seq, ackedSeq, result)
    this.replay.push(seq, frame)
    try {
      this.ws.send(frame)
    } catch {
      /* WS may already be closed */
    }
  }

  close(): void {
    try {
      this.ws.send(encodeCtrl({ t: 'close', ix: this.index }))
    } catch {
      /* WS may already be closed */
    }
  }

  abort(abortValue: string): void {
    try {
      this.ws.send(encodeCtrl({ t: 'abort', ix: this.index, abortValue }))
    } catch {
      /* WS may already be closed */
    }
  }

  error(): void {
    try {
      this.ws.send(encodeCtrl({ t: 'error', ix: this.index }))
    } catch {
      /* WS may already be closed */
    }
  }
}
