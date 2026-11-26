export { EventStream }

import { Fifo } from '../../../utils/Fifo.js'
import { assert } from '../../../utils/assert.js'
import { createPushReadable, type PushReadable } from '../../../utils/pushReadable.js'
import { createPushReadableStream, type PushReadableStream } from '../../../utils/pushReadableStream.js'
import { SSE_EVENT_SLICE_BYTES, WIRE_GATHER_BYTES } from '../../constants.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { textEncoder } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { TAG, passesData } from '../../wire/tags.js'
import {
  countWaitingPublishes,
  droppingFrom,
  droppingLossy,
  droppingPublishes,
  overtakes,
  type WireQueue,
} from '../../flowControl/server/waitingPublishes.js'

type EventStreamOptions = {
  useNodeStream: boolean
  onCancel: () => void
}

/** The event stream to the page: data waits for it to ask for more, and the frames that go at once pass that data. An
 *  event goes in chunks, as the stream asks, and nothing passes it once it started. */
class EventStream implements WireQueue {
  readonly stream: PushReadable | PushReadableStream<Uint8Array<ArrayBuffer>>
  private waiting = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The frames that go at once, waiting for the event going to the stream to end. */
  private passing = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The encoded bytes of `waiting` and `passing`. */
  private waitingBytes = 0
  /** The frame whose event is going to the stream, and how far it went. */
  private current: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** The newest event started in order where it is a publish's head, whose body comes next in order: a body at the
   *  front of `waiting` names no channel, only its head does. */
  private headStarted: Uint8Array<ArrayBuffer> | null = null
  /** Whether the stream asks for more. */
  private takes: boolean

  constructor({ useNodeStream, onCancel }: EventStreamOptions) {
    // A Web stream asks from inside `enqueue`: an ask waits, so that no push runs inside another.
    const onTake = () => queueMicrotask(() => this.handWaiting())
    this.stream = useNodeStream
      ? createPushReadable(onCancel, onTake)
      : createPushReadableStream<Uint8Array<ArrayBuffer>>(onCancel, onTake)
    this.takes = this.stream.push(textEncoder.encode(': open\n\n'))
  }

  /** The encoded bytes the stream holds, and those waiting for it to ask. */
  get heldBytes(): number {
    return this.streamBytes + this.waitingBytes
  }

  /** The encoded bytes a frame that goes at once is behind. */
  get streamBytes(): number {
    const { current } = this
    return this.stream.bufferedAmount + (current === null ? 0 : encodedBytes(current.frame.byteLength - current.offset))
  }

  /** A pair fits as one event when nothing on the stream or in either queue is ahead of it. */
  canGatherPublish(bytes: number): boolean {
    return (
      bytes <= WIRE_GATHER_BYTES &&
      this.takes &&
      this.current === null &&
      this.headStarted === null &&
      this.waiting.length === 0 &&
      this.passing.length === 0 &&
      this.stream.bufferedAmount === 0
    )
  }

  /** Gives an idle stream both frames of a publish in one event. */
  gatherPublish(head: Uint8Array<ArrayBuffer>, body: Uint8Array<ArrayBuffer>): void {
    const message = encode.message([head, body], head.byteLength + body.byteLength)
    this.takes = this.stream.push(encodeChunk(message, 0, message.byteLength))
  }

  /** Data waits for the stream to ask for it, as the frame the replay also holds: it's encoded a slice at a time. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    const tag = frame[0]
    const atOnce =
      tag === undefined ||
      passesData(tag) ||
      (frame.byteLength <= WIRE_GATHER_BYTES && overtakes(frame, this.framesAhead()))
    if (atOnce && this.current !== null) {
      this.passing.push(frame)
      this.waitingBytes += eventBytes(frame)
      return
    }
    if (atOnce) {
      this.startEvent(frame)
      return
    }
    if (this.takes && this.current === null && this.waiting.length === 0) {
      this.startInOrder(frame)
      return
    }
    this.waiting.push(frame)
    this.waitingBytes += eventBytes(frame)
  }

  private *framesAhead(): Generator<Uint8Array> {
    if (this.headStarted !== null) yield this.headStarted
    yield* this.waiting
  }

  waitingPublishes(ix: number): number {
    return countWaitingPublishes(this.waiting, ix)
  }

  dropWaitingPublishes(ix: number, keep: number, upTo?: number): void {
    this.dropWaiting(droppingPublishes(ix, keep, upTo))
  }

  dropWaitingFrom(ix: number, from: 'publish' | 'start'): void {
    this.dropWaiting(droppingFrom(ix, from))
  }

  /** The stream is given an event as it starts: what passes and what waits, it has no byte of. */
  dropUngiven(ix: number): number {
    const cut = droppingLossy(ix)
    this.dropWaiting(cut.drops)
    return cut.count()
  }

  /** `drops` is asked in the order the stream takes the frames: what passes goes first. */
  private dropWaiting(drops: (frame: Uint8Array) => boolean): void {
    const goes = (frame: Uint8Array<ArrayBuffer>) => {
      if (!drops(frame)) return false
      this.waitingBytes -= eventBytes(frame)
      return true
    }
    this.passing.remove(goes)
    this.waiting.remove(goes)
  }

  /** What waits goes no more: past a wire only lost, the page's reconnect gets it from its channels' replay, and it could
   *  wait here for as long as the socket stays open taking nothing. An end the server decided goes as the `last` event,
   *  after the event under way, which nothing passes once it started: what waits ended with the wire, but for the
   *  channels' ends decided before it (`ERROR`), which go ahead of it in the order they were decided, so the page reads
   *  each channel's end as its server channel settled with it. */
  close(last: Uint8Array<ArrayBuffer> | null): void {
    const ends = last === null ? [] : [...this.passing, ...this.waiting].filter((frame) => frame[0] === TAG.ERROR)
    this.passing = new Fifo()
    this.waiting = new Fifo()
    this.waitingBytes = 0
    if (last !== null) {
      while (this.current !== null) this.pushChunks()
      for (const end of ends) this.stream.push(encodeChunk(end, 0, end.byteLength))
      this.stream.push(encodeChunk(last, 0, last.byteLength))
    }
    this.current = null
    this.stream.close()
  }

  /** Hands the stream the data that waits, as far as it asks. */
  private handWaiting(): void {
    this.takes = true
    if (this.current !== null) this.pushChunks()
    while (this.takes) {
      const frame = this.waiting.shift()
      if (frame === undefined) return
      this.waitingBytes -= eventBytes(frame)
      this.startInOrder(frame)
    }
  }

  /** Starts the event of `frame`, next in the order data keeps. A head names its channel only until its body's event
   *  starts: nothing passes an event once it started. */
  private startInOrder(frame: Uint8Array<ArrayBuffer>): void {
    this.headStarted = frame[0] === TAG.PUBLISH_TO ? frame : null
    this.startEvent(frame)
  }

  private startEvent(frame: Uint8Array<ArrayBuffer>): void {
    this.current = { frame, offset: 0 }
    this.pushChunks()
  }

  /** Pushes the current event's next chunks, at least one, as far as the stream asks. What passes goes as the event
   *  ends. */
  private pushChunks(): void {
    const current = this.current
    assert(current !== null)
    const { frame } = current
    do {
      const start = current.offset
      const end = Math.min(frame.byteLength, start + SSE_EVENT_SLICE_BYTES)
      this.takes = this.stream.push(encodeChunk(frame, start, end))
      current.offset = end
    } while (this.takes && current.offset < frame.byteLength)
    if (current.offset < frame.byteLength) return
    this.current = null
    for (let passing = this.passing.shift(); passing !== undefined; passing = this.passing.shift()) {
      this.waitingBytes -= eventBytes(passing)
      this.takes = this.stream.push(encodeChunk(passing, 0, passing.byteLength))
    }
  }
}

/** The part of `frame`'s event that holds its bytes from `start` to `end`, the first and last parts with the event's
 *  ends. */
function encodeChunk(frame: Uint8Array<ArrayBuffer>, start: number, end: number): Uint8Array<ArrayBuffer> {
  const base64 = uint8ArrayToBase64url(frame.subarray(start, end))
  return textEncoder.encode(`${start === 0 ? 'data: ' : ''}${base64}${end === frame.byteLength ? '\n\n' : ''}`)
}

/** What `bytes` of a frame take as base64url, unpadded. */
function encodedBytes(bytes: number): number {
  return Math.ceil((bytes * 4) / 3)
}

/** What `frame`'s event takes: its base64url and the 8 bytes around it. */
function eventBytes(frame: Uint8Array<ArrayBuffer>): number {
  return encodedBytes(frame.byteLength) + 8
}
