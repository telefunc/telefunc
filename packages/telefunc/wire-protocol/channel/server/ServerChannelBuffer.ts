export { ServerChannelBuffer, HELD_SEND_OVERHEAD_BYTES }
export type { BufferedSender, EntryCallback, EntryData }

import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { TAG } from '../../wire/tags.js'
import { HEADER } from '../../wire/header.js'
import type { IndexedPeer } from './IndexedPeer.js'

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
  /** The buffer holds `bytes` more for its page. */
  _holdOffline(bytes: number): void
  /** The buffer let `bytes` go. */
  _letGoOffline(bytes: number): void
  _sendBuffered(peer: IndexedPeer, tag: number, data: EntryData, callback: EntryCallback | null): void
}

type EntryData = string | Uint8Array
type Entry = { tag: number; data: EntryData; callback: EntryCallback | null }

/**
 * Holds what a channel sends while no peer is attached (text, binary, ack requests), for `flush` to send in order. It
 * takes all it is given, and counts it: the channel admits each send first.
 */
class ServerChannelBuffer<TAck = never> {
  private entries: Entry[] = []
  private readonly owner: BufferedSender
  /** What the held sends count as on the channel's ledger. */
  sendBytes = 0

  constructor(owner: BufferedSender) {
    this.owner = owner
  }

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

  /** Sends every entry to its channel in the order it came, then empties the buffer. */
  flush(peer: IndexedPeer): void {
    this.letGoSends()
    for (const { tag, data, callback } of this.entries) this.owner._sendBuffered(peer, tag, data, callback)
    this.clear()
  }

  clear(err?: Error): void {
    if (err) for (const { callback } of this.entries) callback?.reject(err)
    this.entries = []
    this.letGoSends()
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
    const held = bytes + HEADER + HELD_SEND_OVERHEAD_BYTES
    this.sendBytes += held
    this.owner._holdOffline(held)
  }

  private letGoSends(): void {
    if (this.sendBytes > 0) this.owner._letGoOffline(this.sendBytes)
    this.sendBytes = 0
  }
}
