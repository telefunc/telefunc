export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
} from '../channel/channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shieldKey.js'
import { makePublishInfo } from '../channel/channel.js'
import { ServerChannel, reportServerChannelError } from '../channel/server/channel.js'
import { getBroadcastAdapter } from './broadcastAdapter.js'
import type { BroadcastPublishResult, BroadcastAdapter, BroadcastUnsubscribe } from './broadcastAdapter.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import { Listeners } from '../../utils/Listeners.js'
import { ChannelClosedError, ChannelOverflowError } from '../channel/channelErrors.js'
import { ACK_STATUS, ERROR_REASON, encodePublishText, encodePublishBinary, TAG } from '../wire/connectionFrame.js'
import type { ChannelDataFrame, WirePublishInfo } from '../wire/connectionFrame.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../node/server/Abort.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')

class ServerBroadcast<T = unknown> extends ServerChannel {
  readonly [SERVER_BROADCAST_BRAND] = true
  /** @see ChannelShield in ../channel/channel.ts — broadcast only validates incoming
   *  publishes from clients (the `data` direction). The `ack` slot is unused: publish
   *  receipts are server-generated, not client-supplied. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly key: string

  private readonly _broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly _broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  private _adapter: BroadcastAdapter | null = null
  private _unsubBroadcast: BroadcastUnsubscribe | null = null
  private _unsubBinaryBroadcast: BroadcastUnsubscribe | null = null
  private _peerSubscribedText = false
  private _peerSubscribedBinary = false

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
    // Its page grants it the largest window from the start (see `ClientBroadcast`).
    this._flow.onPeerByteWindow(this._flow.peerByteWindowMax)
  }

  /** Its page grants the largest window at once: a quarter of it past credit is more than the page reads unreported. */
  protected override _pastCreditAllowance(): number {
    return this._flow.peerByteWindowMax / 4
  }

  static isServerBroadcast(value: unknown): value is ServerBroadcast {
    return value !== null && typeof value === 'object' && SERVER_BROADCAST_BRAND in value
  }

  // Channel methods that don't apply to broadcast: throw at runtime.
  override send(): never {
    assertUsage(false, '`send()` is not available on a `BroadcastChannel` — use `publish()`.')
  }
  override sendBinary(): never {
    assertUsage(false, '`sendBinary()` is not available on a `BroadcastChannel` — use `publishBinary()`.')
  }
  override listen(): never {
    assertUsage(false, '`listen()` is not available on a `BroadcastChannel` — use `subscribe()`.')
  }
  override listenBinary(): never {
    assertUsage(false, '`listenBinary()` is not available on a `BroadcastChannel` — use `subscribeBinary()`.')
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    this._ensureBroadcast()
    if (!this._adapter) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this._trackAck(Promise.resolve(this._publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    this._ensureBroadcast()
    this._subscribeBroadcast()
    return this._broadcastListeners.add(callback)
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
    return this._broadcastBinaryListeners.add(callback)
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

  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishAckReq(text, seq))
  }

  _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishBinaryAckReq(data, seq))
  }

  _deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this._broadcastListeners.list()) {
      try {
        const result = cb(data, info)
        if (isPromise(result)) result.catch((err: unknown) => this._handleCallbackError(err))
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (!this._peerSubscribedText) return
    this._forwardPublish(encodePublishText(serialized, rawInfo))
  }

  _deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of this._broadcastBinaryListeners.list()) {
      try {
        // A copy of its own, which a Node.js Buffer's slice() isn't: a listener may write into it or transfer its buffer.
        const result = cb(new Uint8Array(data), info)
        if (isPromise(result)) result.catch((err: unknown) => this._handleCallbackError(err))
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (!this._peerSubscribedBinary) return
    this._forwardPublish(encodePublishBinary(data, rawInfo))
  }

  /** A text (`string`) or binary publish to the page, buffered while it is away; a page behind is let go. */
  private _forwardPublish(wire: string | Uint8Array): void {
    const binary = typeof wire !== 'string'
    const peer = this._peer
    if (!peer) {
      if (binary) this._prePeerBuffer.pushPublishBinary(wire)
      else this._prePeerBuffer.pushPublish(wire)
      this._closeIfDroppedOffline(binary)
      return
    }
    if (this._flow.isPastByteCredit && this._isPeerBehind()) {
      this._closeBehind()
      return
    }
    this._flow.countPublishSent(binary ? peer.sendPublishBinary(wire) : peer.sendPublish(wire))
  }

  /** A page behind the broadcast has no send to reject: it leaves the group on both ends rather than get a gap. */
  private _closeBehind(): void {
    this._endWithError(
      ERROR_REASON.OVERFLOW,
      new ChannelOverflowError('Broadcast closed: its client fell further behind than the server holds for a client'),
    )
  }

  /** A publish the buffer for an offline page dropped would leave it a gap: it gets the end at its next attach instead. */
  private _closeIfDroppedOffline(binary: boolean): void {
    if (!this._prePeerBuffer.dropped) return
    const limit = binary ? 'bufferLimitBinary' : 'bufferLimit'
    this._endWithError(
      ERROR_REASON.OVERFLOW,
      new ChannelOverflowError(
        `Broadcast closed: more was published to its client while it was offline than config.channel.${limit} lets the server hold`,
      ),
    )
  }

  override _onPeerSubscription({ binary, subscribed }: { binary: boolean; subscribed: boolean }): void {
    if (binary) this._peerSubscribedBinary = subscribed
    else this._peerSubscribedText = subscribed
    if (subscribed) {
      this._ensureBroadcast()
      if (binary) this._subscribeBinaryBroadcast()
      else this._subscribeBroadcast()
      return
    }
    if (binary && this._broadcastBinaryListeners.size === 0) {
      this._unsubBinaryBroadcast?.()
      this._unsubBinaryBroadcast = null
    }
    if (!binary && this._broadcastListeners.size === 0) {
      this._unsubBroadcast?.()
      this._unsubBroadcast = null
    }
  }

  protected override _shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    this._unsubBroadcast?.()
    this._unsubBroadcast = null
    this._unsubBinaryBroadcast?.()
    this._unsubBinaryBroadcast = null
    super._shutdown(err, options)
  }

  // --- Internal broadcast helpers ---

  private _ensureBroadcast(): void {
    if (this._adapter) return
    this._adapter = getBroadcastAdapter()
  }

  private _subscribeBroadcast(): void {
    if (this._unsubBroadcast) return
    assert(this._adapter)
    this._unsubBroadcast = this._adapter.subscribe(
      this.key,
      (serialized, rawInfo) => this._deliverBroadcastMessage(serialized, rawInfo),
      this._wireRequest,
    )
  }

  private _subscribeBinaryBroadcast(): void {
    if (this._unsubBinaryBroadcast) return
    assert(this._adapter)
    this._unsubBinaryBroadcast = this._adapter.subscribeBinary(
      this.key,
      (data, rawInfo) => this._deliverBroadcastBinaryMessage(data, rawInfo),
      this._wireRequest,
    )
  }

  private _publishBroadcast(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._adapter)
    return receiptOf(this.key, this._adapter.publish(this.key, serialized))
  }

  private _publishBinaryBroadcast(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._adapter)
    return receiptOf(this.key, this._adapter.publishBinary(this.key, data))
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
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async _dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this._ensureBroadcast()
      const result = await this._publishBinaryBroadcast(data)
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }
}

/** Public surface of a `BroadcastChannel` instance — same shape `Channel` uses to hide internal
 *  `_methods` from autocomplete on user-facing `chat.` etc. The underlying class is `ServerBroadcast`. */
type BroadcastChannel<T = unknown> = {
  readonly key: string
  readonly id: string
  readonly isClosed: boolean
  readonly [TELEFUNC_SHIELDS]: {
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

const BroadcastChannel = ServerBroadcast as {
  new <T = unknown>(opts: { key: string }): BroadcastChannel<T>
}

/** The adapter's receipt as the public one, carrying its key like a subscriber's `info`. */
function receiptOf(
  key: string,
  result: BroadcastPublishResult | Promise<BroadcastPublishResult>,
): ChannelPublishAck | Promise<ChannelPublishAck> {
  const toAck = (r: BroadcastPublishResult): ChannelPublishAck => ({
    key,
    seq: r.seq,
    timestamp: r.timestamp,
    meta: r.meta,
  })
  return isPromise(result) ? result.then(toAck) : toAck(result)
}

const Broadcast = {
  publish<U = unknown>(key: string, data: ChannelData<U>): ChannelPublishAck | Promise<ChannelPublishAck> {
    const adapter = getBroadcastAdapter()
    const serialized = stringify(data)
    return receiptOf(key, adapter.publish(key, serialized))
  },
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe {
    const adapter = getBroadcastAdapter()
    return adapter.subscribe(key, (serialized, info) => {
      const data = parse(serialized) as ChannelData<U>
      callStaticListener(() => callback(data, { key, seq: info.seq, timestamp: info.timestamp }))
    })
  },
  publishBinary(key: string, data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    const adapter = getBroadcastAdapter()
    return receiptOf(key, adapter.publishBinary(key, data))
  },
  subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe {
    const adapter = getBroadcastAdapter()
    return adapter.subscribeBinary(key, (data, info) => {
      // A copy of its own, as a `BroadcastChannel` listener gets.
      callStaticListener(() => callback(new Uint8Array(data), { key, seq: info.seq, timestamp: info.timestamp }))
    })
  },
}

/** Reports a listener's error as a channel listener's is, but an Abort: there is no channel to close. */
function callStaticListener(call: () => unknown): void {
  const report = (err: unknown) => {
    if (!isAbort(err)) reportServerChannelError(err)
  }
  try {
    const pending = call()
    if (isPromise(pending)) pending.catch(report)
  } catch (err) {
    report(err)
  }
}
