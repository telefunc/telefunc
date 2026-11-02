export { ClientBroadcast }

import type {
  BroadcastBinaryListener,
  BroadcastListener,
  BroadcastListenerByKind,
  BroadcastListeners,
  ChannelData,
  ChannelPublishAck,
} from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { encode, type ChannelDataFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { BroadcastKind, BroadcastSubscriptions, AttachState } from '../../wire/payloads.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ChannelClosedError, isExpectedChannelFailure } from '../channelErrors.js'
import { classifyTelefuncError } from '../errorClassification.js'
import { isPromise } from '../../../utils/isPromise.js'
import { ClientChannel, reportChannelError, type ClientChannelOptions } from './channel.js'

class ClientBroadcast<T = unknown> extends ClientChannel {
  private readonly subscribers: BroadcastListeners<T> = { text: new Listeners(), binary: new Listeners() }
  /** The subscriptions this page asks the server for. */
  private readonly subscribed: BroadcastSubscriptions = { text: false, binary: false }

  constructor(options: ClientChannelOptions) {
    super(options)
    // Nothing waits on a publish's credit: the window only bounds how far behind the server lets the page fall.
    this.receiveCredit.widenByteWindow()
  }

  /** @internal Register a local listener without changing what it asks the server for. */
  _subscribeLocal<K extends BroadcastKind>(kind: K, callback: BroadcastListenerByKind<T>[K]): () => void {
    return this.subscribers[kind].add(callback)
  }

  /** @internal Sends what `reply` resolves to, also while closing: a graceful close waits for it, as for an ack's answer.
   *  A reply it can't send is reported, and `failure` goes instead. */
  _sendReply(reply: Promise<unknown>, failure: unknown): void {
    void this.lifecycle.trackAck(
      reply.then((data) => {
        try {
          this.sendReplyData(data)
        } catch (err) {
          this.lifecycle.handleCallbackError(err)
          this.sendReplyData(failure)
        }
      }),
    )
  }

  /** @internal Declare what it asks the server for; a reconnect carries it in the RECONCILE entry. */
  _setSubscribed(kind: BroadcastKind, on: boolean): void {
    if (on === this.subscribed[kind] || this.closed) return
    this.subscribed[kind] = on
    if (on) this.connection.sendBroadcastSubscribe(this, kind === 'binary')
    else this.connection.sendBroadcastUnsubscribe(this, kind === 'binary')
  }

  /** @internal Declares the subscriptions on every (re)attach: a SUB or UNSUB written to a dead wire isn't replayed. */
  override _attachState(): AttachState {
    return { broadcast: this.subscribed }
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    return reportingUnexpected(this._publishUnreported(data))
  }

  /** @internal A publish that leaves reporting its rejection to the caller. */
  _publishUnreported(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this.closed) throw new ChannelClosedError()
    return this.sendAckReq<ChannelPublishAck, string>(encode.publishAckReq, stringify(data))
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    return this.subscribeWired('text', callback)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    return reportingUnexpected(this._publishBinaryUnreported(data))
  }

  /** @internal A publish that leaves reporting its rejection to the caller. */
  _publishBinaryUnreported(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this.closed) throw new ChannelClosedError()
    return this.sendAckReq<ChannelPublishAck, Uint8Array>(encode.publishBinaryAckReq, data)
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    return this.subscribeWired('binary', callback)
  }

  /** The first listener of a kind subscribes the wire, and the last one unsubscribes it. */
  private subscribeWired<K extends BroadcastKind>(kind: K, callback: BroadcastListenerByKind<T>[K]): () => void {
    if (this.subscribers[kind].size === 0) this._setSubscribed(kind, true)
    const unsubscribe = this._subscribeLocal(kind, callback)
    return () => {
      unsubscribe()
      if (this.subscribers[kind].size === 0) this._setSubscribed(kind, false)
    }
  }

  private sendReplyData(data: unknown): void {
    this.sendCredit.countSent(this.connection.sendSequenced(this, 'data', encode.text, stringify(data)))
  }

  protected override dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH) {
      this._onPeerPublish(frame.text, frame.info, frame.bytes)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY) {
      this._onPeerPublishBinary(frame.data as Uint8Array<ArrayBuffer>, frame.info, frame.bytes)
      return
    }
    super.dispatchDataFrame(frame)
  }

  /** A publish counts in bytes, consumed once the listeners ran, so the server sees how far behind the page is. */
  _onPeerPublish(data: string, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const parsed = parse(data) as ChannelData<T>
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const cb of this.subscribers.text.list()) {
      try {
        const result = cb(parsed, info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
  }

  _onPeerPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const cb of this.subscribers.binary.list()) {
      try {
        // Bytes of its own: a listener may write into them or transfer their buffer.
        const result = cb(new Uint8Array(data), info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
  }
}

/** Reports a publish rejection that is a bug; the caller still sees every rejection. */
function reportingUnexpected(publish: Promise<ChannelPublishAck>): Promise<ChannelPublishAck> {
  publish.catch((err) => {
    if (classifyTelefuncError(err, isExpectedChannelFailure).kind === 'bug') reportChannelError(err)
  })
  return publish
}
