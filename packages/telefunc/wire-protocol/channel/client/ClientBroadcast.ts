export { ClientBroadcast }

import type { BroadcastBinaryListener, BroadcastListener, ChannelData, ChannelPublishAck } from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { encode, type ChannelDataFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { Listeners } from '../../../utils/Listeners.js'
import { broadcastMessages } from '../channelMessages.js'
import { CHANNEL_BUFFER_LIMIT_BINARY_BYTES, CHANNEL_BUFFER_LIMIT_BYTES } from '../../constants.js'
import { ChannelClosedError } from '../channelErrors.js'
import { isPromise } from '../../../utils/isPromise.js'
import { ClientChannel, type ClientChannelOptions } from './ClientChannel.js'

class ClientBroadcast<T = unknown> extends ClientChannel {
  private readonly broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  /** The subscriptions this page asks the server for. */
  private readonly subscribed = { text: false, binary: false }

  constructor(options: ClientChannelOptions) {
    super(options)
    // Nothing waits on a publish's credit, so the page grants the largest window from the start.
    this.receiveCredit.widenByteWindow()
    this._attach.declareSubscriptions(this.subscribed)
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this.stopped) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this.sendAckReq<ChannelPublishAck, string>(encode.publishAckReq, serialized)
    ret.catch(() => {})
    return ret
  }

  /** The page holds what its loop falls behind by within the server's default buffer limits. */
  override messages(options?: { signal?: AbortSignal }) {
    const budget = { text: CHANNEL_BUFFER_LIMIT_BYTES, binary: CHANNEL_BUFFER_LIMIT_BINARY_BYTES }
    return broadcastMessages<ChannelData<T>>(this, budget, options)
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
    const ret = this.sendAckReq<ChannelPublishAck, Uint8Array>(encode.publishBinaryAckReq, data)
    ret.catch(() => {})
    return ret
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
    for (const cb of this.broadcastBinaryListeners.list()) {
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
