export { ClientBroadcast }

import type { BroadcastBinaryListener, BroadcastListener, ChannelData, ChannelPublishAck } from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { encode, type ChannelDataFrame, type SequencedEncoder } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { PUBLISH_BINARY_HEADER, type WirePublishInfo } from '../../wire/publishInfo.js'
import { HEADER } from '../../wire/header.js'
import { Listeners } from '../../../utils/Listeners.js'
import { broadcastMessages } from '../channelMessages.js'
import { ChannelClosedError } from '../channelErrors.js'
import { isPromise } from '../../../utils/isPromise.js'
import { forListener } from '../listenerAnswer.js'
import { ClientChannel, type ClientChannelOptions } from './ClientChannel.js'

type ClientBroadcastOptions = ClientChannelOptions & { lossy?: boolean }

class ClientBroadcast<T = unknown> extends ClientChannel {
  private readonly broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  /** The subscriptions this page asks the server for. */
  private readonly subscribed = { text: false, binary: false }
  /** A newer publish replaces this page's own that no wire has taken yet: see `sendPublish`. */
  private readonly lossy: boolean
  /** This page's newest publish on a lossy broadcast, which may still wait to go. */
  private newestPublish: { seq: number; receipt: Promise<ChannelPublishAck> } | null = null

  constructor(options: ClientBroadcastOptions) {
    super(options)
    this.lossy = options.lossy === true
    // Nothing waits on a publish's credit, so the page grants the largest window from the start.
    this.receiveCredit.widenByteWindow()
    this._attach.declareSubscriptions(this.subscribed)
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this.stopped) throw new ChannelClosedError()
    const ret = this.sendPublish(encode.publishAckReq, stringify(data))
    ret.catch(() => {})
    return ret
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options)
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): () => void {
    if (options.signal?.aborted) return () => {}
    const remove = this.broadcastListeners.add(callback, {
      ...options,
      onRemove: () => {
        if (this.broadcastListeners.size === 0) this.setSubscribed('text', false)
      },
    })
    if (this.broadcastListeners.size === 1) this.setSubscribed('text', true)
    return remove
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this.stopped) throw new ChannelClosedError()
    const ret = this.sendPublish(encode.publishBinaryAckReq, data)
    ret.catch(() => {})
    return ret
  }

  /** On a lossy broadcast, a publish that still waits to go takes the newer one's data in its place, and both get its
   *  receipt. */
  private sendPublish<D>(encodeFrame: SequencedEncoder<D>, data: D): Promise<ChannelPublishAck> {
    const newest = this.newestPublish
    if (newest !== null && this.connection.replaceWaiting(this, newest.seq, encodeFrame, data)) return newest.receipt
    let seq = 0
    const receipt = this.sendAckReq<ChannelPublishAck, D>(encodeFrame, data, (queued) => (seq = queued))
    if (this.lossy) this.newestPublish = { seq, receipt }
    return receipt
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): () => void {
    if (options.signal?.aborted) return () => {}
    const remove = this.broadcastBinaryListeners.add(callback, {
      ...options,
      onRemove: () => {
        if (this.broadcastBinaryListeners.size === 0) this.setSubscribed('binary', false)
      },
    })
    if (this.broadcastBinaryListeners.size === 1) this.setSubscribed('binary', true)
    return remove
  }

  override _end(err?: Error): void {
    super._end(err)
    this.broadcastListeners.clear()
    this.broadcastBinaryListeners.clear()
  }

  private setSubscribed(kind: 'text' | 'binary', on: boolean): void {
    if (on === this.subscribed[kind] || this.stopped) return
    this.subscribed[kind] = on
    if (on) this.connection.sendBroadcastSubscribe(this, kind === 'binary')
    else this.connection.sendBroadcastUnsubscribe(this, kind === 'binary')
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
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const cb of this.broadcastListeners.list()) {
      try {
        const result = cb(parsed, info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
  }

  private onPeerPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo, bytes: number): void {
    this.receiveCredit.onPublishArrived()
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    const list = this.broadcastBinaryListeners.list()
    for (const [i, cb] of list.entries()) {
      try {
        const result = cb(forListener(data, list, i, HEADER + PUBLISH_BINARY_HEADER), info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    this.receiveCredit.onPublishConsumed(bytes)
  }
}
