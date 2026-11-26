export { ClientBroadcast }

import type {
  BroadcastBinaryListener,
  BroadcastListener,
  BroadcastListenerByKind,
  BroadcastListeners,
  BroadcastPublishOptions,
  ChannelData,
  ChannelPublishAck,
  ChannelPublishInfo,
} from '../channel.js'
import { Outcome, neverOpened, stringifyValue, unsubscribeOf, type Unsubscribe } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assert } from '../../../utils/assert.js'
import { encode, type ChannelCtrlFrame, type ChannelDataFrame, type SequencedEncoder } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import { PUBLISH_BINARY_HEADER, type WirePublishInfo } from '../../wire/publishInfo.js'
import { HEADER } from '../../wire/header.js'
import { Listeners, type Registration } from '../../../utils/Listeners.js'
import { broadcastMessages } from '../channelMessages.js'
import { binaryData, binaryPayload, isLossy, textPayload } from '../broadcastPayload.js'
import { ChannelClosedError, isExpectedChannelFailure } from '../channelErrors.js'
import { classifyTelefuncError } from '../errorClassification.js'
import { isPromise } from '../../../utils/isPromise.js'
import { forListener } from '../listenerAnswer.js'
import { ClientChannel, reportChannelError, type ClientChannelOptions } from './ClientChannel.js'
import { EVERY_STREAM } from '../ownStream.js'

/** A page's end of a Broadcast: it receives `T`, and publishes `Publish`, which is `T` unless the server takes another
 *  message than it sends. */
class ClientBroadcast<T = unknown, Publish = T> extends ClientChannel {
  readonly key: string
  private readonly subscribers: BroadcastListeners<T> = { text: new Listeners(), binary: new Listeners() }
  /** Reads the parts that changed in this task, until it declares them. */
  private dueParts: (() => unknown) | null = null

  constructor({ key, ...options }: ClientChannelOptions & { key: string }) {
    super(options)
    this.key = key
    // Nothing waits on a publish's credit, so the page grants the largest window from the start.
    this.receiveCredit.widenByteWindow()
  }

  /** @internal Register a local listener without changing what it asks the server for; it gets each payload as it
   *  arrived. */
  _subscribeLocal<K extends BroadcastKind>(kind: K, callback: BroadcastListenerByKind<T>[K]): () => void {
    return this.subscribers[kind].add(callback)
  }

  /** @internal Sends what `reply` resolves to, also while closing: a graceful close waits for it, as for an ack's
   *  answer. A reply it can't send is reported, and `failure` goes instead. */
  _sendReply(reply: Promise<unknown>, failure: unknown): void {
    void this.lifecycle.trackAck(this.sendSettledReply(reply, failure))
  }

  private async sendSettledReply(reply: Promise<unknown>, failure: unknown): Promise<void> {
    const data = await reply
    try {
      this.sendReplyData(data)
    } catch (err) {
      this.lifecycle.handleCallbackError(err)
      this.sendReplyData(failure)
    }
  }

  /** @internal A publish of a lossy stream arrived, which the page confirms soon: the server tells by that whether it
   *  keeps up. */
  _confirmSoon(): void {
    this.receiveCredit.confirmSoon()
  }

  /** @internal Changes what it wants the server to send it, as one versioned want: the wire, a reconnect's RECONCILE
   *  entry and each call made while the server hasn't confirmed it carry it. */
  _setSubscribed(kind: BroadcastKind, on: boolean): void {
    if (on === (this._attach.wants?.[kind] ?? false) || this.stopped) return
    this.connection.sendWant(this, this._attach.changeWant({ [kind]: on }))
  }

  /** @internal Changes what an add-on built on it wants beyond its two kinds (`Want`'s `parts`), as `_setSubscribed`
   *  does, once for all the changes of a task: `parts` reads them when it declares them. */
  _setWantParts(parts: () => unknown): void {
    if (this.stopped) return
    if (this.dueParts === null) this.connection.wantDue(this)
    this.dueParts = parts
  }

  /** @internal Declares the parts that changed in this task, if any. */
  _declareWant(): void {
    const parts = this.dueParts
    this.dueParts = null
    if (parts === null || this.stopped) return
    this.connection.sendWant(this, this._attach.changeWant({ parts: parts() }))
  }

  publish(data: ChannelData<Publish>, options?: BroadcastPublishOptions): Promise<ChannelPublishAck> {
    const { lossy = false } = options ?? {}
    const payload = textPayload(stringifyValue(data, 'publish'), lossy)
    return reportingUnexpected(this.sendPublish(encode.publishAckReq, payload, lossy ? 'text' : EVERY_STREAM, lossy))
  }

  /** @internal A publish that leaves reporting its rejection to the caller. Its answer is what the add-on's own end
   *  of the channel answers it with, which only the add-on reads. */
  _publishUnreported(data: ChannelData<Publish>): Promise<unknown> {
    return this.sendPublish(encode.publishAckReq, stringifyValue(data, 'publish'), null, false)
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options)
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.subscribeWired('text', callback, options)
  }

  publishBinary(data: Uint8Array, options?: BroadcastPublishOptions): Promise<ChannelPublishAck> {
    const { lossy = false } = options ?? {}
    return reportingUnexpected(
      this.sendPublish(encode.publishBinaryAckReq, binaryPayload(data, lossy), lossy ? 'binary' : EVERY_STREAM, lossy),
    )
  }

  /** @internal A publish of `data` as it is, which leaves reporting its rejection to the caller. Its answer is what the
   *  add-on's own end of the channel answers it with, which only the add-on reads. */
  _publishBinaryUnreported(data: Uint8Array, stream: string, lossy: boolean): Promise<unknown> {
    return this.sendPublish(encode.publishBinaryAckReq, data, stream, lossy)
  }

  /** Of a lossy `stream`, once its uplink is behind, a publish that still waits to go takes the newer one's data in its
   *  place, and both get its receipt. A plain publish of the stream, or of `EVERY_STREAM`, ends that place, so no later
   *  one goes ahead of it. */
  private sendPublish<D>(
    encodeFrame: SequencedEncoder<D>,
    data: D,
    stream: string | null,
    lossy: boolean,
  ): Promise<ChannelPublishAck> {
    if (this.stopped) throw this.lifecycle.sendRefusal()
    if (stream === null) return this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data)
    if (!lossy) return this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data, { stream })
    const replaced = this.connection.replaceWaiting(this, stream, encodeFrame, data)
    if (replaced === null) return this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data, { stream, lossy })
    // Both get the receipt of the publish whose place it took, which still waits for its answer: the close waits for
    // that answer through the first one.
    const waiting = this.pendingAcks.get(replaced.seq)
    assert(waiting)
    return waiting.promise
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): Unsubscribe {
    const listener: BroadcastBinaryListener = (payload, info) => {
      if (isLossy(payload)) this._confirmSoon()
      return callback(binaryData(payload), info)
    }
    return this.subscribeWired('binary', listener, options)
  }

  protected override dispatchCtrl(frame: ChannelCtrlFrame): void {
    if (frame.tag === TAG.WANTED) this.onWanted(frame.version)
    else super.dispatchCtrl(frame)
  }

  private onWanted(version: number): void {
    this._attach.confirmWant(version)
    if (this._attach.unconfirmedWant === null) this.connection.wantSettled(this)
  }

  override _finalize(err: Error | undefined): void {
    super._finalize(err)
    // What waits gets the end: one its close callbacks removed too.
    this._attach.failWants(err ?? new ChannelClosedError())
    this.subscribers.text.clear()
    this.subscribers.binary.clear()
  }

  /** The first listener of a kind subscribes the wire, and the last one's removal unsubscribes it. Its `opened` settles
   *  once the server confirmed the want this subscriber needs, or the subscriber is removed: with the end as known then
   *  where the close callbacks were called. */
  private subscribeWired<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListenerByKind<T>[K],
    options: { signal?: AbortSignal },
  ): Unsubscribe {
    if (options.signal?.aborted) return unsubscribeOf(() => {})
    // A closing one sends no want any more.
    if (this.stopped) return neverOpened(this.endError())
    const opened = new Outcome()
    if (this.subscribers[kind].size === 0) this._setSubscribed(kind, true)
    const remove = this.subscribers[kind].add(callback, {
      ...options,
      onRemove: () => {
        // Removed, it can no longer take effect: once its close callbacks were called, that is the end as known now.
        this._attach.stopWaiting(opened, this.lifecycle.closeFired ? this.endError() : true)
        if (this.subscribers[kind].size === 0) this._setSubscribed(kind, false)
      },
    })
    this._attach.waitFor(opened, this._attach.wants?.version ?? 0)
    return unsubscribeOf(remove, opened)
  }

  private sendReplyData(data: unknown): void {
    this.sendCredit.count(this.connection.sendSequenced(this, 'data', encode.text, stringify(data)))
  }

  protected override dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH) {
      this.onPeerPublish(frame.text, frame.info, frame.bytes)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY) {
      this.onPeerPublishBinary(frame.data as Uint8Array<ArrayBuffer>, frame.info, frame.bytes)
      return
    }
    super.dispatchDataFrame(frame)
  }

  /** A publish counts in bytes, consumed once the listeners ran, so the server sees how far behind the page is. */
  private onPeerPublish(payload: string, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const parsed = parse(payload) as ChannelData<T>
    this.deliverPublish(this.subscribers.text.list(), () => parsed, wireInfo, bytes, isLossy(payload))
  }

  /** A `subscribeBinary()` listener reads the payload's mark, as Room's local ones read their own frames. */
  private onPeerPublishBinary(payload: Uint8Array, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const list = this.subscribers.binary.list()
    const valueFor = (i: number) => forListener(payload, list, i, HEADER + PUBLISH_BINARY_HEADER)
    this.deliverPublish(list, valueFor, wireInfo, bytes, false)
  }

  /** Calls each listener with its value, `valueFor` its index; an `Abort` stops the rest and leaves the publish
   *  unconsumed. A listener's promise isn't awaited. A `lossy` one is confirmed soon, so the server tells whether the
   *  page keeps up. */
  private deliverPublish<V>(
    listeners: readonly Registration<(value: V, info: ChannelPublishInfo) => unknown>[],
    valueFor: (i: number) => V,
    wireInfo: WirePublishInfo,
    bytes: number,
    lossy: boolean,
  ): void {
    const info = { key: this.key, seq: wireInfo.seq, timestamp: wireInfo.timestamp }
    for (const [i, { listener }] of listeners.entries()) {
      if (!listener) continue
      try {
        const result = listener(valueFor(i), info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
    if (lossy) this._confirmSoon()
  }
}

/** Reports a publish rejection that is a bug; the caller still sees every rejection. */
function reportingUnexpected(publish: Promise<ChannelPublishAck>): Promise<ChannelPublishAck> {
  void reportIfBug(publish)
  return publish
}

async function reportIfBug(publish: Promise<ChannelPublishAck>): Promise<void> {
  try {
    await publish
  } catch (err) {
    if (classifyTelefuncError(err, isExpectedChannelFailure).kind === 'bug') reportChannelError(err)
  }
}
