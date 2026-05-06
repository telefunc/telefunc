export { Broadcast, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
} from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { ServerChannel } from './channel.js'
import { getBroadcastAdapter } from './broadcast.js'
import type { BroadcastPublishResult, BroadcastAdapter, BroadcastUnsubscribe } from './broadcast.js'
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

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')

class ServerBroadcast<T = unknown> extends ServerChannel {
  readonly [SERVER_BROADCAST_BRAND] = true
  /** @see __DEFINE_TELEFUNC_SHIELDS on ChannelBase — broadcast only validates incoming
   *  publishes from clients (the `data` direction). The `ack` slot is unused: publish
   *  receipts are server-generated, not client-supplied. */
  declare readonly __DEFINE_TELEFUNC_SHIELDS: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly key: string

  private _broadcastListeners: Array<BroadcastListener<T>> = []
  private _broadcastBinaryListeners: Array<BroadcastBinaryListener> = []
  private _adapter: BroadcastAdapter | null = null
  private _unsubBroadcast: BroadcastUnsubscribe | null = null
  private _unsubBinaryBroadcast: BroadcastUnsubscribe | null = null
  private _peerSubscribedText = false
  private _peerSubscribedBinary = false

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
  }

  static isServerBroadcast(value: unknown): value is ServerBroadcast {
    return value !== null && typeof value === 'object' && SERVER_BROADCAST_BRAND in value
  }

  static publish<U = unknown>(
    key: string,
    data: ChannelData<U>,
  ): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    const adapter = getBroadcastAdapter()
    const serialized = stringify(data, { forbidReactElements: false })
    return adapter.publish(key, serialized)
  }

  static subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe {
    const adapter = getBroadcastAdapter()
    return adapter.subscribe(key, (serialized, info) => {
      const data = parse(serialized) as ChannelData<U>
      callback(data, { key, seq: info.seq, ts: info.ts })
    })
  }

  static publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    const adapter = getBroadcastAdapter()
    return adapter.publishBinary(key, data)
  }

  static subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe {
    const adapter = getBroadcastAdapter()
    return adapter.subscribeBinary(key, (data, info) => {
      callback(data, { key, seq: info.seq, ts: info.ts })
    })
  }

  // Channel methods that don't apply to broadcast: throw at runtime.
  override send(): never {
    assertUsage(false, '`send()` is not available on a `Broadcast` — use `publish()`.')
  }
  override sendBinary(): never {
    assertUsage(false, '`sendBinary()` is not available on a `Broadcast` — use `publishBinary()`.')
  }
  override listen(): never {
    assertUsage(false, '`listen()` is not available on a `Broadcast` — use `subscribe()`.')
  }
  override listenBinary(): never {
    assertUsage(false, '`listenBinary()` is not available on a `Broadcast` — use `subscribeBinary()`.')
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    this._ensureBroadcast()
    if (!this._adapter) throw new ChannelClosedError()
    const serialized = stringify(data, { forbidReactElements: false })
    const ret = this._trackAck(Promise.resolve(this._publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    this._ensureBroadcast()
    this._subscribeBroadcast()
    this._broadcastListeners.push(callback)
    return () => {
      const index = this._broadcastListeners.indexOf(callback)
      if (index >= 0) this._broadcastListeners.splice(index, 1)
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._ensureBroadcast()
    if (!this._adapter) throw new ChannelClosedError()
    const ret = this._trackAck(Promise.resolve(this._publishBinaryBroadcast(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    this._ensureBroadcast()
    this._subscribeBinaryBroadcast()
    this._broadcastBinaryListeners.push(callback)
    return () => {
      const index = this._broadcastBinaryListeners.indexOf(callback)
      if (index >= 0) this._broadcastBinaryListeners.splice(index, 1)
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
    if (frame.tag === TAG.BROADCAST_SUB) {
      this._onPeerBroadcastSubscribe(frame.binary)
      return
    }
    if (frame.tag === TAG.BROADCAST_UNSUB) {
      this._onPeerBroadcastUnsubscribe(frame.binary)
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

  _deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.ts)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this._broadcastListeners) {
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

  _deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.ts)
    for (const cb of this._broadcastBinaryListeners) {
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

  _onPeerBroadcastSubscribe(binary: boolean): void {
    this._ensureBroadcast()
    if (binary) {
      this._peerSubscribedBinary = true
      this._subscribeBinaryBroadcast()
    } else {
      this._peerSubscribedText = true
      this._subscribeBroadcast()
    }
  }

  _onPeerBroadcastUnsubscribe(binary: boolean): void {
    if (binary) {
      this._peerSubscribedBinary = false
      this._unsubBinaryBroadcast?.()
      this._unsubBinaryBroadcast = null
    } else {
      this._peerSubscribedText = false
      this._unsubBroadcast?.()
      this._unsubBroadcast = null
    }
  }

  protected override _shutdown(err?: Error): void {
    this._unsubBroadcast?.()
    this._unsubBroadcast = null
    this._unsubBinaryBroadcast?.()
    this._unsubBinaryBroadcast = null
    super._shutdown(err)
  }

  // --- Internal broadcast helpers ---

  private _ensureBroadcast(): void {
    if (this._adapter) return
    this._adapter = getBroadcastAdapter()
  }

  private _subscribeBroadcast(): void {
    if (this._unsubBroadcast) return
    assert(this._adapter)
    this._unsubBroadcast = this._adapter.subscribe(this.key, (serialized, rawInfo) =>
      this._deliverBroadcastMessage(serialized, rawInfo),
    )
  }

  private _subscribeBinaryBroadcast(): void {
    if (this._unsubBinaryBroadcast) return
    assert(this._adapter)
    this._unsubBinaryBroadcast = this._adapter.subscribeBinary(this.key, (data, rawInfo) =>
      this._deliverBroadcastBinaryMessage(data, rawInfo),
    )
  }

  private _publishBroadcast(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._adapter)
    const toAck = (r: BroadcastPublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.ts), { meta: r.meta })
    const result = this._adapter.publish(this.key, serialized)
    if (isPromise(result)) return result.then(toAck)
    return toAck(result)
  }

  private _publishBinaryBroadcast(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._adapter)
    const toAck = (r: BroadcastPublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.ts), { meta: r.meta })
    const result = this._adapter.publishBinary(this.key, data)
    if (isPromise(result)) return result.then(toAck)
    return toAck(result)
  }

  private async _dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
      this._ensureBroadcast()
      const validateData = this._validators.get('data')
      if (validateData) {
        const data = parse(serialized) as ChannelData<T>
        const result = validateData(data)
        // `shield-error` status lets the client reject its `publish()` promise with a branded
        // ShieldValidationError — same identity every other shield-fail surface produces.
        if (result !== true) {
          this._sendAckRes(seq, result, ACK_STATUS.SHIELD_ERROR)
          return
        }
      }
      const result = await this._publishBroadcast(serialized)
      this._sendAckRes(seq, stringify(result, { forbidReactElements: false }))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async _dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this._ensureBroadcast()
      const result = await this._publishBinaryBroadcast(data)
      this._sendAckRes(seq, stringify(result, { forbidReactElements: false }))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }
}

/** Public surface of `Broadcast` — same shape `Channel` uses to hide internal `_methods`
 *  from autocomplete on user-facing `chat.` etc. The underlying class is `ServerBroadcast`. */
type Broadcast<T = unknown> = {
  readonly key: string
  readonly id: string
  readonly isClosed: boolean
  readonly __DEFINE_TELEFUNC_SHIELDS: {
    data: ChannelData<T>
    ack: unknown
  }
  publish(data: ChannelData<T>): Promise<ChannelPublishAck>
  subscribe(callback: BroadcastListener<T>): () => void
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  subscribeBinary(callback: BroadcastBinaryListener): () => void
  onClose(callback: ChannelCloseCallback): void
  onOpen(callback: () => void): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

const Broadcast = ServerBroadcast as {
  new <T = unknown>(opts: { key: string }): Broadcast<T>
  publish<U = unknown>(key: string, data: ChannelData<U>): BroadcastPublishResult | Promise<BroadcastPublishResult>
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe
  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult>
  subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe
}
