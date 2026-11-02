export { ClientBroadcast }

import type { BroadcastBinaryListener, BroadcastListener, ChannelData, ChannelPublishAck } from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { encode, type ChannelDataFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ChannelClosedError } from '../channelErrors.js'
import { isPromise } from '../../../utils/isPromise.js'
import { ClientChannel, reportChannelError, type ClientChannelOptions } from './ClientChannel.js'

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
    if (this.closed) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this.sendAckReq<ChannelPublishAck, string>(encode.publishAckReq, serialized)
    ret.catch(reportChannelError)
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    if (this.broadcastListeners.size === 0) this.setSubscribed('text', true)
    const remove = this.broadcastListeners.add(callback)
    return () => {
      remove()
      if (this.broadcastListeners.size === 0) this.setSubscribed('text', false)
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this.closed) throw new ChannelClosedError()
    const ret = this.sendAckReq<ChannelPublishAck, Uint8Array>(encode.publishBinaryAckReq, data)
    ret.catch(reportChannelError)
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    if (this.broadcastBinaryListeners.size === 0) this.setSubscribed('binary', true)
    const remove = this.broadcastBinaryListeners.add(callback)
    return () => {
      remove()
      if (this.broadcastBinaryListeners.size === 0) this.setSubscribed('binary', false)
    }
  }

  private setSubscribed(kind: 'text' | 'binary', on: boolean): void {
    if (on === this.subscribed[kind] || this.closed) return
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
