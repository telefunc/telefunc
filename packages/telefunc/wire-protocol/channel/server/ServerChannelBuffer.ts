export { ServerChannelBuffer, HELD_SEND_OVERHEAD_BYTES }
export type { BufferedSender, EntryCallback, EntryData }

import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { TAG } from '../../wire/tags.js'
import { HEADER } from '../../wire/header.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { PublishBody } from './PublishBody.js'

/** What a send held in memory (buffered, or awaiting its answer or credit) takes beyond its payload: its callbacks and
 *  promise, about 480 B. */
const HELD_SEND_OVERHEAD_BYTES = 512

/** Callback pair for buffered entries: resolve on flush, reject where the channel ends first. */
type EntryCallback = {
  resolve: (...args: any[]) => void
  reject: (err: Error) => void
}

/** The channel `flush` hands each held entry to, with the entry's tag, data and callbacks (null once rejected). */
interface BufferedSender {
  _sendBuffered(peer: IndexedPeer, tag: number, data: EntryData, callback: EntryCallback | null): void
}

type EntryData = string | Uint8Array | PublishBody
type Entry = { tag: number; data: EntryData; callback: EntryCallback | null }

/**
 * Holds what a channel sends while no peer is attached (text, binary, publishes, ack requests), for `flush` to send in
 * order. It takes all it is given, and counts it: the channel refuses a send past `maxPendingBytes`, and a broadcast ends
 * past `PUBLISH_HOLD_BYTES`.
 */
class ServerChannelBuffer<TAck = never> {
  private entries: Entry[] = []
  /** What the held sends count as against `maxPendingBytes`. */
  sendBytes = 0
  /** What the held publishes count as against `PUBLISH_HOLD_BYTES`. */
  publishBytes = 0

  get size(): number {
    return this.entries.length
  }

  pushText(data: string, resolve: () => void, reject: (err: Error) => void): void {
    this.pushSend(TAG.TEXT, data, utf8ByteLength(data), { resolve, reject })
  }

  pushTextAck(data: string, resolve: (value: TAck) => void, reject: (err: Error) => void): void {
    this.pushSend(TAG.TEXT_ACK_REQ, data, utf8ByteLength(data), { resolve, reject })
  }

  /** Holds a copy, as an attached send encodes the caller's bytes at once and the caller may then reuse them. */
  pushBinary(data: Uint8Array, resolve: () => void, reject: (err: Error) => void): void {
    this.pushSend(TAG.BINARY, new Uint8Array(data), data.byteLength, { resolve, reject })
  }

  pushBinaryAck(data: Uint8Array, resolve: (value: unknown) => void, reject: (err: Error) => void): void {
    this.pushSend(TAG.BINARY_ACK_REQ, new Uint8Array(data), data.byteLength, { resolve, reject })
  }

  pushPublish(body: PublishBody): void {
    this.entries.push({ tag: TAG.PUBLISH, data: body, callback: null })
    this.publishBytes += body.frame.byteLength
  }

  /** Sends every entry to `sender` in the order it came, then empties the buffer. */
  flush(sender: BufferedSender, peer: IndexedPeer): void {
    for (const { tag, data, callback } of this.entries) sender._sendBuffered(peer, tag, data, callback)
    this.clear()
  }

  clear(err?: Error): void {
    if (err) for (const { callback } of this.entries) callback?.reject(err)
    this.entries = []
    this.sendBytes = 0
    this.publishBytes = 0
  }

  /** Rejects the ack requests, whose answers can't be taken, and keeps them to flush with the rest. */
  rejectAcks(err: Error): void {
    for (const entry of this.entries) {
      if (entry.tag !== TAG.TEXT_ACK_REQ && entry.tag !== TAG.BINARY_ACK_REQ) continue
      entry.callback?.reject(err)
      entry.callback = null
    }
  }

  private pushSend(tag: number, data: EntryData, bytes: number, callback: EntryCallback): void {
    this.entries.push({ tag, data, callback })
    this.sendBytes += bytes + HEADER + HELD_SEND_OVERHEAD_BYTES
  }
}
