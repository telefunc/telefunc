export { ServerChannelBuffer }
export type { BufferedSender, Entry, EntryCallback, EntryData }

import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { TAG } from '../../wire/tags.js'
import { HEADER } from '../../wire/header.js'
import { HELD_SEND_OVERHEAD_BYTES } from './channelMemory.js'
import type { IndexedPeer } from './IndexedPeer.js'

/** Callback pair for buffered entries: resolve on flush, reject where the channel ends first. */
type EntryCallback = {
  resolve: (...args: any[]) => void
  reject: (err: Error) => void
}

/** The channel `flush` hands each held entry to. */
interface BufferedSender {
  /** The buffer holds `bytes` more for its page. */
  _holdOffline(bytes: number): void
  /** The buffer let `bytes` go. */
  _letGoOffline(bytes: number): void
  _sendBuffered(peer: IndexedPeer, entry: Entry): void
}

type EntryData = string | Uint8Array
/** `callback`: null once rejected, and for a lossy send, which nothing waits on. */
type Entry = { tag: number; data: EntryData; callback: EntryCallback | null; held: number; lossy: boolean }

/**
 * Holds what a channel sends while no peer is attached (text, binary, ack requests), for `flush` to send in order. It
 * takes all it is given, and counts it: the channel admits each send first.
 */
class ServerChannelBuffer<TAck = never> {
  private entries: Entry[] = []
  private readonly owner: BufferedSender
  /** What the held sends count as on the channel's ledger. */
  private sendBytes = 0

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

  /** What the next lossy send takes the place of: the last entry's, where that is a lossy send. */
  get replaceableHeld(): number {
    const last = this.entries.at(-1)
    return last?.lossy ? last.held : 0
  }

  /** A lossy send of `bytes`, which takes the place of the last entry where that is a lossy send too: between two sends
   *  that must arrive, a page that was away gets only the newest. Holds a copy of binary data, as `pushBinary` does. */
  pushLossy(tag: typeof TAG.TEXT | typeof TAG.BINARY, data: EntryData, bytes: number): void {
    const last = this.entries.at(-1)
    if (last?.lossy) {
      this.entries.pop()
      this.sendBytes -= last.held
      this.owner._letGoOffline(last.held)
    }
    const copy = typeof data === 'string' ? data : new Uint8Array(data)
    this.keep({ tag, data: copy, callback: null, held: bytes + HEADER + HELD_SEND_OVERHEAD_BYTES, lossy: true })
  }

  /** Sends every entry to its channel in the order it came, then empties the buffer. */
  flush(peer: IndexedPeer): void {
    this.letGoSends()
    for (const entry of this.entries) this.owner._sendBuffered(peer, entry)
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

  /** Decides anew, oldest first, as for a send made now: those `admit` takes stay, and the rest go and reject. */
  admitAgain(admit: (held: number) => boolean, refusal: (held: number) => Error): void {
    const entries = this.entries
    this.entries = []
    this.sendBytes = 0
    for (const entry of entries) {
      if (admit(entry.held)) this.keep(entry)
      else entry.callback?.reject(refusal(entry.held))
    }
  }

  private pushSend(tag: number, data: EntryData, bytes: number, callback: EntryCallback): void {
    this.keep({ tag, data, callback, held: bytes + HEADER + HELD_SEND_OVERHEAD_BYTES, lossy: false })
  }

  private keep(entry: Entry): void {
    this.entries.push(entry)
    this.sendBytes += entry.held
    this.owner._holdOffline(entry.held)
  }

  private letGoSends(): void {
    if (this.sendBytes > 0) this.owner._letGoOffline(this.sendBytes)
    this.sendBytes = 0
  }
}
