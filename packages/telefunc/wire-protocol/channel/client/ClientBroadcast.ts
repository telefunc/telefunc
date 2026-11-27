export { ClientBroadcast }

import type {
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelData,
  ChannelPublishAck,
  ChannelPublishInfo,
} from '../channel.js'
import { Outcome, makePublishInfo, neverOpened, stringifyValue, unsubscribeOf, type Unsubscribe } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { encode, type ChannelCtrlFrame, type ChannelDataFrame, type SequencedEncoder } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { PUBLISH_BINARY_HEADER, type WirePublishInfo } from '../../wire/publishInfo.js'
import { HEADER } from '../../wire/header.js'
import { Listeners, type Registration } from '../../../utils/Listeners.js'
import { broadcastMessages } from '../channelMessages.js'
import { ChannelClosedError } from '../channelErrors.js'
import { isPromise } from '../../../utils/isPromise.js'
import { forListener } from '../listenerAnswer.js'
import { ClientChannel, type ClientChannelOptions } from './ClientChannel.js'

type ClientBroadcastOptions = ClientChannelOptions & { lossy?: boolean }

class ClientBroadcast<T = unknown> extends ClientChannel {
  private readonly broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  /** A newer publish replaces this page's own that no wire has taken yet: see `replaceNewestLossy`. */
  private readonly lossy: boolean
  /** The receipt of this page's newest lossy publish, which one that takes its place gets too. */
  private newestReceipt: Promise<ChannelPublishAck> | null = null

  constructor(options: ClientBroadcastOptions) {
    super(options)
    this.lossy = options.lossy === true
    // Nothing waits on a publish's credit, so the page grants the largest window from the start.
    this.receiveCredit.widenByteWindow()
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this.stopped) throw this.lifecycle.sendRefusal()
    const ret = this.sendPublish(encode.publishAckReq, stringifyValue(data, 'publish'))
    ret.catch(() => {})
    return ret
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options)
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.addSubscriber(this.broadcastListeners, 'text', callback, options)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this.stopped) throw this.lifecycle.sendRefusal()
    const ret = this.sendPublish(encode.publishBinaryAckReq, data)
    ret.catch(() => {})
    return ret
  }

  /** On a lossy broadcast, a publish that takes the place of one still waiting gets that one's receipt. */
  private sendPublish<D>(encodeFrame: SequencedEncoder<D>, data: D): Promise<ChannelPublishAck> {
    const start = (onQueued?: (seq: number) => void) =>
      this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data, onQueued)
    if (!this.lossy) return start()
    const newest = this.newestReceipt
    if (newest !== null && this.replaceNewestLossy(encodeFrame, data) !== null) return newest
    this.newestReceipt = this.startLossy(start)
    return this.newestReceipt
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.addSubscriber(this.broadcastBinaryListeners, 'binary', callback, options)
  }

  /** Its `opened` settles once the server confirmed the want this subscriber needs, or the subscriber is removed: with
   *  the end as known then where the close callbacks were called. */
  private addSubscriber<L>(
    listeners: Listeners<L>,
    kind: 'text' | 'binary',
    callback: L,
    options: { signal?: AbortSignal },
  ): Unsubscribe {
    if (options.signal?.aborted) return unsubscribeOf(() => {})
    // A closing one sends no want any more.
    if (this.stopped) return neverOpened(this.endError())
    const opened = new Outcome()
    const remove = listeners.add(callback, {
      ...options,
      onRemove: () => {
        // Removed, it can no longer take effect: once its close callbacks were called, that is the end as known now.
        this._attach.stopWaiting(opened, this.lifecycle.closeFired ? this.endError() : true)
        if (listeners.size === 0) this.setSubscribed(kind, false)
      },
    })
    if (listeners.size === 1) this.setSubscribed(kind, true)
    this._attach.waitFor(opened, this._attach.wants?.version ?? 0)
    return unsubscribeOf(remove, opened)
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
    this.broadcastListeners.clear()
    this.broadcastBinaryListeners.clear()
  }

  private setSubscribed(kind: 'text' | 'binary', on: boolean): void {
    if (on === (this._attach.wants?.[kind] ?? false) || this.stopped) return
    this.connection.sendWant(this, this._attach.changeWant(kind, on))
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
  private onPeerPublish(data: string, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const parsed = parse(data) as ChannelData<T>
    this.deliverPublish(this.broadcastListeners.list(), () => parsed, wireInfo, bytes)
  }

  private onPeerPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const list = this.broadcastBinaryListeners.list()
    this.deliverPublish(list, (i) => forListener(data, list, i, HEADER + PUBLISH_BINARY_HEADER), wireInfo, bytes)
  }

  /** Calls each listener with its value, `valueFor` its index; an `Abort` stops the rest and leaves the publish
   *  unconsumed. A listener's promise isn't awaited. */
  private deliverPublish<V>(
    listeners: readonly Registration<(value: V, info: ChannelPublishInfo) => unknown>[],
    valueFor: (i: number) => V,
    wireInfo: WirePublishInfo,
    bytes: number,
  ): void {
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    if (this.lossy) this.receiveCredit.confirmSoon()
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
  }
}
