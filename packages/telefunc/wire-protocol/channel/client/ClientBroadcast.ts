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
import { stringifyMessage } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assert } from '../../../utils/assert.js'
import { encode, type ChannelDataFrame, type SequencedEncoder } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { BroadcastKind, BroadcastSubscriptions } from '../../wire/payloads.js'
import { PUBLISH_BINARY_HEADER, type WirePublishInfo } from '../../wire/publishInfo.js'
import { HEADER } from '../../wire/header.js'
import { Listeners } from '../../../utils/Listeners.js'
import { broadcastMessages } from '../channelMessages.js'
import { binaryData, binaryPayload, isLossy, textPayload } from '../broadcastPayload.js'
import { ChannelClosedError, isExpectedChannelFailure } from '../channelErrors.js'
import { classifyTelefuncError } from '../errorClassification.js'
import { isPromise } from '../../../utils/isPromise.js'
import { forListener } from '../listenerAnswer.js'
import { ClientChannel, reportChannelError, type ClientChannelOptions } from './ClientChannel.js'

class ClientBroadcast<T = unknown> extends ClientChannel {
  private readonly subscribers: BroadcastListeners<T> = { text: new Listeners(), binary: new Listeners() }
  /** The subscriptions this page asks the server for. */
  private readonly subscribed: BroadcastSubscriptions = { text: false, binary: false }

  constructor(options: ClientChannelOptions) {
    super(options)
    // Nothing waits on a publish's credit, so the page grants the largest window from the start.
    this.receiveCredit.widenByteWindow()
    this._attach.declareSubscriptions(this.subscribed)
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

  /** @internal Declare what it asks the server for; a reconnect carries it in the RECONCILE entry. */
  _setSubscribed(kind: BroadcastKind, on: boolean): void {
    if (on === this.subscribed[kind] || this.stopped) return
    this.subscribed[kind] = on
    if (on) this.connection.sendBroadcastSubscribe(this, kind === 'binary')
    else this.connection.sendBroadcastUnsubscribe(this, kind === 'binary')
  }

  publish(data: ChannelData<T>, options?: BroadcastPublishOptions): Promise<ChannelPublishAck> {
    const lossy = options?.lossy ?? false
    const payload = textPayload(stringifyMessage(data, 'publish'), lossy)
    return reportingUnexpected(this.sendPublish(encode.publishAckReq, payload, 'text', lossy))
  }

  /** @internal A publish that leaves reporting its rejection to the caller. */
  _publishUnreported(data: ChannelData<T>): Promise<ChannelPublishAck> {
    return this.sendPublish(encode.publishAckReq, stringifyMessage(data, 'publish'), null, false)
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options)
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): () => void {
    return this.subscribeWired('text', callback, options)
  }

  publishBinary(data: Uint8Array, options?: BroadcastPublishOptions): Promise<ChannelPublishAck> {
    const lossy = options?.lossy ?? false
    return reportingUnexpected(
      this.sendPublish(encode.publishBinaryAckReq, binaryPayload(data, lossy), 'binary', lossy),
    )
  }

  /** @internal A publish of `data` as it is, which leaves reporting its rejection to the caller. */
  _publishBinaryUnreported(data: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    return this.sendPublish(encode.publishBinaryAckReq, data, stream, lossy)
  }

  /** Of a lossy `stream`, once its uplink is behind, a publish that still waits to go takes the newer one's data in its
   *  place, and both get its receipt. A plain publish of the stream ends that place, so no later one goes ahead of it. */
  private sendPublish<D>(
    encodeFrame: SequencedEncoder<D>,
    data: D,
    stream: string | null,
    lossy: boolean,
  ): Promise<ChannelPublishAck> {
    if (this.stopped) throw new ChannelClosedError()
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

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): () => void {
    const listener: BroadcastBinaryListener = (payload, info) => {
      if (isLossy(payload)) this._confirmSoon()
      return callback(binaryData(payload), info)
    }
    return this.subscribeWired('binary', listener, options)
  }

  override _end(err?: Error): void {
    super._end(err)
    this.subscribers.text.clear()
    this.subscribers.binary.clear()
  }

  /** The first listener of a kind subscribes the wire, and the last one's removal unsubscribes it. */
  private subscribeWired<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListenerByKind<T>[K],
    options: { signal?: AbortSignal },
  ): () => void {
    if (options.signal?.aborted) return () => {}
    if (this.subscribers[kind].size === 0) this._setSubscribed(kind, true)
    return this.subscribers[kind].add(callback, {
      ...options,
      onRemove: () => {
        if (this.subscribers[kind].size === 0) this._setSubscribed(kind, false)
      },
    })
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
    listeners: readonly ((value: V, info: ChannelPublishInfo) => unknown)[],
    valueFor: (i: number) => V,
    wireInfo: WirePublishInfo,
    bytes: number,
    lossy: boolean,
  ): void {
    const info = { key: this.key!, seq: wireInfo.seq, timestamp: wireInfo.timestamp }
    for (const [i, cb] of listeners.entries()) {
      try {
        const result = cb(valueFor(i), info)
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
