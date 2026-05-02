export { pubsub, ServerPubSub }

import type { ChannelData, ChannelPublishAck, PubSub, PubSubBinaryListener, PubSubListener } from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { ServerChannel } from './channel.js'
import { getPubSubAdapter } from './pubsub.js'
import type { PubSubPublishResult, PubSubAdapter, PubSubUnsubscribe } from './pubsub.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import { ChannelClosedError } from '../channel-errors.js'
import { ACK_STATUS, encodePublishText, encodePublishBinary, TAG } from '../shared-ws.js'
import type { ChannelCtrlFrame, ChannelDataFrame, WirePublishInfo } from '../shared-ws.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const SERVER_PUBSUB_BRAND: unique symbol = Symbol.for('ServerPubSub')

class ServerPubSub<T = unknown> extends ServerChannel {
  readonly [SERVER_PUBSUB_BRAND] = true
  readonly key: string

  private _pubSubListeners: Array<PubSubListener<T>> = []
  private _pubSubBinaryListeners: Array<PubSubBinaryListener> = []
  private _adapter: PubSubAdapter | null = null
  private _unsubPubSub: PubSubUnsubscribe | null = null
  private _unsubBinaryPubSub: PubSubUnsubscribe | null = null
  private _peerSubscribedText = false
  private _peerSubscribedBinary = false

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
  }

  static isServerPubSub(value: unknown): value is ServerPubSub {
    return value !== null && typeof value === 'object' && SERVER_PUBSUB_BRAND in value
  }

  // Channel methods that don't apply to pubsub: throw at runtime so the contract
  // matches the public `PubSub<T>` interface (which hides them at the type level).
  override send(): never {
    assertUsage(false, '`send()` is not available on a `pubsub()` channel — use `publish()`.')
  }
  override sendBinary(): never {
    assertUsage(false, '`sendBinary()` is not available on a `pubsub()` channel — use `publishBinary()`.')
  }
  override listen(): never {
    assertUsage(false, '`listen()` is not available on a `pubsub()` channel — use `subscribe()`.')
  }
  override listenBinary(): never {
    assertUsage(false, '`listenBinary()` is not available on a `pubsub()` channel — use `subscribeBinary()`.')
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    this._ensurePubSub()
    if (!this._adapter) throw new ChannelClosedError()
    const serialized = stringify(data, { forbidReactElements: false })
    const ret = this._trackAck(Promise.resolve(this._publishPubSub(serialized)))
    ret.catch(() => {})
    return ret
  }

  subscribe(callback: PubSubListener<T>): () => void {
    this._ensurePubSub()
    this._subscribePubSub()
    this._pubSubListeners.push(callback)
    return () => {
      const index = this._pubSubListeners.indexOf(callback)
      if (index >= 0) this._pubSubListeners.splice(index, 1)
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._ensurePubSub()
    if (!this._adapter) throw new ChannelClosedError()
    const ret = this._trackAck(Promise.resolve(this._publishBinaryPubSub(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: PubSubBinaryListener): () => void {
    this._ensurePubSub()
    this._subscribeBinaryPubSub()
    this._pubSubBinaryListeners.push(callback)
    return () => {
      const index = this._pubSubBinaryListeners.indexOf(callback)
      if (index >= 0) this._pubSubBinaryListeners.splice(index, 1)
    }
  }

  // --- Transport callbacks ---

  protected override _dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH_ACK_REQ) {
      void this._onPeerPublishAckReqMessage(frame.text, frame.seq)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY_ACK_REQ) {
      void this._onPeerPublishBinaryAckReqMessage(frame.data, frame.seq)
      return
    }
    super._dispatchDataFrame(frame)
  }

  override _dispatchCtrl(frame: ChannelCtrlFrame): void {
    if (frame.tag === TAG.PUBSUB_SUB) {
      this._onPeerPubSubSubscribe(frame.binary)
      return
    }
    if (frame.tag === TAG.PUBSUB_UNSUB) {
      this._onPeerPubSubUnsubscribe(frame.binary)
      return
    }
    super._dispatchCtrl(frame)
  }

  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishAckReq(text, seq))
  }

  _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishBinaryAckReq(data, seq))
  }

  _deliverPubSubMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.ts)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this._pubSubListeners) {
      try {
        cb(data, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (!this._peerSubscribedText) return
    const wireText = encodePublishText(serialized, rawInfo)
    if (this._peer) {
      this._peer.sendPublish(wireText)
      return
    }
    this._prePeerBuffer.pushPublish(wireText)
  }

  _deliverPubSubBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.ts)
    for (const cb of this._pubSubBinaryListeners) {
      try {
        cb(data, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (!this._peerSubscribedBinary) return
    const wireData = encodePublishBinary(data, rawInfo)
    if (this._peer) {
      this._peer.sendPublishBinary(wireData)
      return
    }
    this._prePeerBuffer.pushPublishBinary(wireData)
  }

  _onPeerPubSubSubscribe(binary: boolean): void {
    this._ensurePubSub()
    if (binary) {
      this._peerSubscribedBinary = true
      this._subscribeBinaryPubSub()
    } else {
      this._peerSubscribedText = true
      this._subscribePubSub()
    }
  }

  _onPeerPubSubUnsubscribe(binary: boolean): void {
    if (binary) {
      this._peerSubscribedBinary = false
      this._unsubBinaryPubSub?.()
      this._unsubBinaryPubSub = null
    } else {
      this._peerSubscribedText = false
      this._unsubPubSub?.()
      this._unsubPubSub = null
    }
  }

  protected override _shutdown(err?: Error): void {
    this._unsubPubSub?.()
    this._unsubPubSub = null
    this._unsubBinaryPubSub?.()
    this._unsubBinaryPubSub = null
    super._shutdown(err)
  }

  // --- Internal pubsub helpers ---

  private _ensurePubSub(): void {
    if (this._adapter) return
    this._adapter = getPubSubAdapter()
  }

  private _subscribePubSub(): void {
    if (this._unsubPubSub) return
    assert(this._adapter)
    this._unsubPubSub = this._adapter.subscribe(this.key, (serialized, rawInfo) =>
      this._deliverPubSubMessage(serialized, rawInfo),
    )
  }

  private _subscribeBinaryPubSub(): void {
    if (this._unsubBinaryPubSub) return
    assert(this._adapter)
    this._unsubBinaryPubSub = this._adapter.subscribeBinary(this.key, (data, rawInfo) =>
      this._deliverPubSubBinaryMessage(data, rawInfo),
    )
  }

  private _publishPubSub(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._adapter)
    const toAck = (r: PubSubPublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.ts), { meta: r.meta })
    const result = this._adapter.publish(this.key, serialized)
    if (isPromise(result)) return result.then(toAck)
    return toAck(result)
  }

  private _publishBinaryPubSub(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._adapter)
    const toAck = (r: PubSubPublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.ts), { meta: r.meta })
    const result = this._adapter.publishBinary(this.key, data)
    if (isPromise(result)) return result.then(toAck)
    return toAck(result)
  }

  private async _dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
      this._ensurePubSub()
      const result = await this._publishPubSub(serialized)
      this._sendAckRes(seq, stringify(result, { forbidReactElements: false }))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async _dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this._ensurePubSub()
      const result = await this._publishBinaryPubSub(data)
      this._sendAckRes(seq, stringify(result, { forbidReactElements: false }))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }
}

// --- pubsub() function with static methods ---

interface PubSubFunction {
  <T = unknown>(key: string): PubSub<T>
  publish<T = unknown>(key: string, data: ChannelData<T>): PubSubPublishResult | Promise<PubSubPublishResult>
  subscribe<T = unknown>(key: string, callback: PubSubListener<T>): PubSubUnsubscribe
  publishBinary(key: string, data: Uint8Array): PubSubPublishResult | Promise<PubSubPublishResult>
  subscribeBinary(key: string, callback: PubSubBinaryListener): PubSubUnsubscribe
}

const pubsub: PubSubFunction = Object.assign(
  function pubsub<T = unknown>(key: string): PubSub<T> {
    return new ServerPubSub<T>({ key })
  },
  {
    publish<T = unknown>(key: string, data: ChannelData<T>): PubSubPublishResult | Promise<PubSubPublishResult> {
      const adapter = getPubSubAdapter()
      const serialized = stringify(data, { forbidReactElements: false })
      return adapter.publish(key, serialized)
    },
    subscribe<T = unknown>(key: string, callback: PubSubListener<T>): PubSubUnsubscribe {
      const adapter = getPubSubAdapter()
      return adapter.subscribe(key, (serialized, info) => {
        const data = parse(serialized) as ChannelData<T>
        callback(data, { key, seq: info.seq, ts: info.ts })
      })
    },
    publishBinary(key: string, data: Uint8Array): PubSubPublishResult | Promise<PubSubPublishResult> {
      const adapter = getPubSubAdapter()
      return adapter.publishBinary(key, data)
    },
    subscribeBinary(key: string, callback: PubSubBinaryListener): PubSubUnsubscribe {
      const adapter = getPubSubAdapter()
      return adapter.subscribeBinary(key, (data, info) => {
        callback(data, { key, seq: info.seq, ts: info.ts })
      })
    },
  },
)
