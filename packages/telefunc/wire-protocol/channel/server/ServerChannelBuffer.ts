export { ServerChannelBuffer }
export type { BufferedSender, Entry, EntryCallback, EntryData }

import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { TAG } from '../../wire/tags.js'
import { heldSendBytes } from './channelMemory.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { OWN_STREAM } from '../ownStream.js'

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
/** `callback`: null once rejected, and for a lossy send or a publish, which nothing waits on. `lossyOf`: the stream of a
 *  lossy entry, whose next lossy entry takes its place where it is the last; null for one that must arrive. */
type Entry = {
  tag: number
  data: EntryData
  callback: EntryCallback | null
  held: number
  lossyOf: string | null
}

/**
 * Holds what a channel sends while no peer is attached (text, binary, ack requests), for `flush` to send in order. It takes all it is given, and counts it: the channel admits each send first.
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
    return last?.lossyOf === OWN_STREAM ? last.held : 0
  }

  /** A lossy send of `bytes`, which takes the place of the last entry where that is a lossy send too: between two sends
   *  that must arrive, a page that was away gets only the newest. Holds a copy of binary data, as `pushBinary` does. */
  pushLossy(tag: typeof TAG.TEXT | typeof TAG.BINARY, data: EntryData, bytes: number): void {
    // Copied first: detached bytes throw before they take the place of the last entry.
    const copy = typeof data === 'string' ? data : new Uint8Array(data)
    this.dropReplaced(OWN_STREAM)
    this.keep({ tag, data: copy, callback: null, held: heldSendBytes(bytes), lossyOf: OWN_STREAM })
  }

  /** Lets go of its lossy sends, unsent, and of what they held. */
  dropLossy(): void {
    let held = 0
    for (const entry of this.entries) if (entry.lossyOf !== null) held += entry.held
    if (held === 0) return
    this.entries = this.entries.filter((entry) => entry.lossyOf === null)
    this.sendBytes -= held
    this.owner._letGoOffline(held)
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
    this.keep({ tag, data, callback, held: heldSendBytes(bytes), lossyOf: null })
  }

  /** The last entry goes, unsent, where it is a lossy one of `stream`. */
  private dropReplaced(stream: string): void {
    const last = this.entries.at(-1)
    if (last?.lossyOf !== stream) return
    this.entries.pop()
    this.sendBytes -= last.held
    this.owner._letGoOffline(last.held)
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
