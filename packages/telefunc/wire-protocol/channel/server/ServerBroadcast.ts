export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  ChannelPublishInfo,
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
import { makePublishInfo } from '../channel.js'
import { ServerChannel, reportServerChannelError } from './ServerChannel.js'
import { getBroadcastAdapter } from '../../broadcast/broadcastAdapter.js'
import type {
  BroadcastPublishResult,
  BroadcastAdapter,
  BroadcastUnsubscribe,
} from '../../broadcast/broadcastAdapter.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
import { ACK_STATUS, ERROR_REASON, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { encodePublishText, encodePublishBinary } from '../../wire/publishInfo.js'
import type { ChannelDataFrame } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../../shared/constants.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../../node/server/Abort.js'
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

  private readonly broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  private adapter: BroadcastAdapter | null = null
  private unsubBroadcast: BroadcastUnsubscribe | null = null
  private unsubBinaryBroadcast: BroadcastUnsubscribe | null = null
  private peerSubscribedText = false
  private peerSubscribedBinary = false

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
  }

  /** A publish can't be awaited per page, so a broadcast's page gets a quarter of the largest window past credit, not
   *  what was published to it: a page that stops reading has its wire cut, and reconnects. */
  protected override pastCreditAllowance(): number {
    return this.sendCredit.peerByteWindowMax / 4
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
    this.ensureBroadcast()
    if (!this.adapter) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    this.ensureBroadcast()
    this.subscribeBroadcast()
    return this.broadcastListeners.add(callback)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this.ensureBroadcast()
    if (!this.adapter) throw new ChannelClosedError()
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBinaryBroadcast(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    this.ensureBroadcast()
    this.subscribeBinaryBroadcast()
    return this.broadcastBinaryListeners.add(callback)
  }

  // --- Transport callbacks ---

  protected override dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH_ACK_REQ) {
      void this._onPeerPublishAckReqMessage(frame.text, frame.seq)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY_ACK_REQ) {
      void this._onPeerPublishBinaryAckReqMessage(frame.data, frame.seq)
      return
    }
    super.dispatchDataFrame(frame)
  }

  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.dispatchPublishAckReq(text, seq))
  }

  _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.dispatchPublishBinaryAckReq(data, seq))
  }

  private deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this.broadcastListeners.list()) {
      try {
        const result = cb(data, info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    if (!this.peerSubscribedText) return
    this.forwardPublish(encodePublishText(serialized, rawInfo))
  }

  private deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of this.broadcastBinaryListeners.list()) {
      try {
        // A copy of its own, which a Node.js Buffer's slice() isn't: a listener may write into it or transfer its buffer.
        const result = cb(new Uint8Array(data), info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    if (!this.peerSubscribedBinary) return
    this.forwardPublish(encodePublishBinary(data, rawInfo))
  }

  /** A text (`string`) or binary publish to the page, buffered while it is away. */
  private forwardPublish(wire: string | Uint8Array): void {
    const binary = typeof wire !== 'string'
    const peer = this.peer
    if (!peer) {
      if (binary) this.offlineBuffer.pushPublishBinary(wire)
      else this.offlineBuffer.pushPublish(wire)
      this.closeIfDroppedOffline(binary)
      return
    }
    if (binary) peer.sendSequenced(encode.publishBinary, wire)
    else peer.sendSequenced(encode.publish, wire)
  }

  /** A publish the buffer for an offline page dropped would leave it a gap: it gets the end at its next attach instead. */
  private closeIfDroppedOffline(binary: boolean): void {
    if (!this.offlineBuffer.dropped) return
    const limit = binary ? 'bufferLimitBinary' : 'bufferLimit'
    this.endWithError(
      ERROR_REASON.OVERFLOW,
      new ChannelOverflowError(
        `Broadcast closed: more was published to its client while it was offline than config.channel.${limit} lets the server hold`,
      ),
    )
  }

  protected override onPeerSubscription({ binary, subscribed }: { binary: boolean; subscribed: boolean }): void {
    if (binary) this.peerSubscribedBinary = subscribed
    else this.peerSubscribedText = subscribed
    if (subscribed) {
      this.ensureBroadcast()
      if (binary) this.subscribeBinaryBroadcast()
      else this.subscribeBroadcast()
      return
    }
    if (binary && this.broadcastBinaryListeners.size === 0) {
      this.unsubBinaryBroadcast?.()
      this.unsubBinaryBroadcast = null
    }
    if (!binary && this.broadcastListeners.size === 0) {
      this.unsubBroadcast?.()
      this.unsubBroadcast = null
    }
  }

  protected override shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    this.unsubBroadcast?.()
    this.unsubBroadcast = null
    this.unsubBinaryBroadcast?.()
    this.unsubBinaryBroadcast = null
    super.shutdown(err, options)
  }

  // --- Internal broadcast helpers ---

  private ensureBroadcast(): void {
    if (this.adapter) return
    this.adapter = getBroadcastAdapter()
  }

  private subscribeBroadcast(): void {
    if (this.unsubBroadcast) return
    assert(this.adapter)
    this.unsubBroadcast = this.adapter.subscribe(
      this.key,
      (serialized, rawInfo) => this.deliverBroadcastMessage(serialized, rawInfo),
      this._wireRequest,
    )
  }

  private subscribeBinaryBroadcast(): void {
    if (this.unsubBinaryBroadcast) return
    assert(this.adapter)
    this.unsubBinaryBroadcast = this.adapter.subscribeBinary(
      this.key,
      (data, rawInfo) => this.deliverBroadcastBinaryMessage(data, rawInfo),
      this._wireRequest,
    )
  }

  private publishBroadcast(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this.adapter)
    return receiptOf(this.key, this.adapter.publish(this.key, serialized))
  }

  private publishBinaryBroadcast(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this.adapter)
    return receiptOf(this.key, this.adapter.publishBinary(this.key, data))
  }

  private async dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
      this.ensureBroadcast()
      const validateData = this._validators.get('data')
      if (validateData) {
        const data = parse(serialized) as ChannelData<T>
        const result = validateData(data)
        // `shield-error` status lets the client reject its `publish()` promise with a branded
        // ShieldValidationError — same identity every other shield-fail surface produces.
        if (result !== true) {
          this.sendAckRes(seq, result, ACK_STATUS.SHIELD_ERROR)
          return
        }
      }
      const result = await this.publishBroadcast(serialized)
      this.sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this.ensureBroadcast()
      const result = await this.publishBinaryBroadcast(data)
      this.sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
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
      callStaticListener(callback, data, makePublishInfo(key, info.seq, info.timestamp))
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
      callStaticListener(callback, new Uint8Array(data), makePublishInfo(key, info.seq, info.timestamp))
    })
  },
}

/** Reports a listener's error as a channel listener's is, but an Abort: there is no channel to close. */
function callStaticListener<D>(
  callback: (data: D, info: ChannelPublishInfo) => unknown,
  data: D,
  info: ChannelPublishInfo,
): void {
  try {
    const pending = callback(data, info)
    if (isPromise(pending)) pending.catch(reportStaticListenerError)
  } catch (err) {
    reportStaticListenerError(err)
  }
}

function reportStaticListenerError(err: unknown): void {
  if (!isAbort(err)) reportServerChannelError(err)
}
