export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelPublishInfo,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { invokeChannelListener, makePublishInfo } from '../channel.js'
import { ServerChannel, reportServerChannelError } from './channel.js'
import type { BroadcastBackend, PublishResult } from '../backend/broadcast/contract.js'
import { getBroadcastBackend } from '../backend/install.js'
import type { BackendSubscription } from '../backend/subscription.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import { ChannelClosedError, isExpectedChannelFailure } from '../channel-errors.js'
import { ACK_STATUS, encodePublishText, encodePublishBinary } from '../shared-ws.js'
import type { BroadcastKind, WirePublishInfo } from '../shared-ws.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { classifyTelefuncError } from '../error-classification.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
type BroadcastUnsubscribe = () => void
type BroadcastListeners<T> = { text: Array<BroadcastListener<T>>; binary: Array<BroadcastBinaryListener> }
const BROADCAST_KINDS = ['text', 'binary'] as const

class ServerBroadcast<T = unknown> extends ServerChannel {
  readonly [SERVER_BROADCAST_BRAND] = true
  /** @see ChannelShield in ../channel.ts — broadcast only validates incoming
   *  publishes from clients (the `data` direction). The `ack` slot is unused: publish
   *  receipts are server-generated, not client-supplied. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly key: string

  private readonly _subscribers: BroadcastListeners<T> = { text: [], binary: [] }
  private _backend: BroadcastBackend | null = null
  private readonly _subscriptions: Record<BroadcastKind, BackendSubscription | null> = { text: null, binary: null }
  private readonly _peerSubscriptions: Record<BroadcastKind, boolean> = { text: false, binary: false }

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
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
    return this._publishTracked('text', textEncoder.encode(stringify(data)))
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    return this._subscribe('text', callback)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    return this._publishTracked('binary', data)
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    return this._subscribe('binary', callback)
  }

  // --- Transport callbacks ---

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishAckReq(text, seq))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishBinaryAckReq(data, seq))
  }

  _deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this._subscribers.text) {
      if (invokeChannelListener(cb, [data, info], (error) => this._handleCallbackError(error))) return
    }
    if (!this._peerSubscriptions.text) return
    this._sendPublish(encodePublishText(serialized, rawInfo))
  }

  _deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of this._subscribers.binary) {
      if (invokeChannelListener(cb, [data, info], (error) => this._handleCallbackError(error))) return
    }
    if (!this._peerSubscriptions.binary) return
    this._sendPublishBinary(encodePublishBinary(data, rawInfo))
  }

  override _onPeerBroadcastSubscribe(binary: boolean): void {
    this._setPeerSubscription(binary ? 'binary' : 'text', true)
  }

  override _onPeerBroadcastUnsubscribe(binary: boolean): void {
    this._setPeerSubscription(binary ? 'binary' : 'text', false)
  }

  protected override _shutdown(err?: Error): void {
    for (const kind of BROADCAST_KINDS) {
      this._peerSubscriptions[kind] = false
      this._clearSubscription(kind)
    }
    super._shutdown(err)
  }

  // --- Internal broadcast helpers ---

  private _setPeerSubscription(kind: BroadcastKind, on: boolean): void {
    if (on) this._ensureBroadcast()
    this._peerSubscriptions[kind] = on
    this._reconcileSubscription(kind)
  }

  private _ensureBroadcast(): void {
    if (this._backend) return
    this._backend = getBroadcastBackend()
  }

  private _subscribe<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListeners<T>[K][number],
  ): BroadcastUnsubscribe {
    const listeners = this._subscribers[kind] as Array<typeof callback>
    if (this._isClosed) throw new ChannelClosedError()
    this._ensureBroadcast()
    listeners.push(callback)
    try {
      this._reconcileSubscription(kind)
    } catch (error) {
      listeners.pop()
      throw error
    }
    return () => {
      const index = listeners.indexOf(callback)
      if (index < 0) return
      listeners.splice(index, 1)
      this._reconcileSubscription(kind)
    }
  }

  private _reconcileSubscription(kind: BroadcastKind): void {
    if (this._isClosed || (!this._peerSubscriptions[kind] && this._subscribers[kind].length === 0)) {
      this._clearSubscription(kind)
      return
    }
    if (this._subscriptions[kind] !== null) return
    assert(this._backend)
    const subscription = this._backend.subscribe({ key: this.key, kind }, (payload, rawInfo) => {
      if (kind === 'text') this._deliverBroadcastMessage(textDecoder.decode(payload), rawInfo)
      else this._deliverBroadcastBinaryMessage(payload, rawInfo)
    })
    this._subscriptions[kind] = subscription
    // A dead handle would keep the next reconcile from subscribing again.
    onSubscriptionEnd(subscription, () => {
      if (this._subscriptions[kind] === subscription) this._subscriptions[kind] = null
    })
  }

  private _clearSubscription(kind: BroadcastKind): void {
    const subscription = this._subscriptions[kind]
    if (subscription) void subscription.unsubscribe()
    this._subscriptions[kind] = null
  }

  private _publishTracked(kind: BroadcastKind, payload: Uint8Array): Promise<ChannelPublishAck> {
    this._ensureBroadcast()
    const ret = this._trackAck(Promise.resolve(this._publish(kind, payload)))
    ret.catch(() => {})
    return ret
  }

  private _publish(kind: BroadcastKind, payload: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._backend)
    const toAck = (r: PublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.timestamp), {
        meta: r.meta,
        ...(r.receivers === undefined ? {} : { receivers: r.receivers }),
      })
    const result = this._backend.publish({ key: this.key, kind }, payload)
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
      const result = await this._publish('text', textEncoder.encode(serialized))
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async _dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this._ensureBroadcast()
      const result = await this._publish('binary', data)
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

const Broadcast = {
  publish<U = unknown>(key: string, data: ChannelData<U>): PublishResult | Promise<PublishResult> {
    const backend = getBroadcastBackend()
    const serialized = stringify(data)
    const lane = { key, kind: 'text' } as const
    return backend.publish(lane, textEncoder.encode(serialized))
  },
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe {
    return subscribeLane(
      { key, kind: 'text' },
      (payload) => parse(textDecoder.decode(payload)) as ChannelData<U>,
      callback,
    )
  },
  publishBinary(key: string, data: Uint8Array): PublishResult | Promise<PublishResult> {
    const backend = getBroadcastBackend()
    const lane = { key, kind: 'binary' } as const
    return backend.publish(lane, data)
  },
  subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe {
    return subscribeLane({ key, kind: 'binary' }, (payload) => payload, callback)
  },
}

function subscribeLane<Data>(
  lane: { key: string; kind: BroadcastKind },
  decode: (payload: Uint8Array) => Data,
  callback: (data: Data, info: ChannelPublishInfo) => unknown,
): BroadcastUnsubscribe {
  const subscription = getBroadcastBackend().subscribe(lane, (payload, info) => {
    invokeChannelListener(
      callback,
      [decode(payload), makePublishInfo(lane.key, info.seq, info.timestamp)],
      reportStaticListenerError,
    )
  })
  onSubscriptionEnd(subscription, () => {})
  return () => {
    void subscription.unsubscribe()
  }
}

/** Runs `onEnd` once the subscription is closed, reporting a terminal failure: only that rejects `ready`, while an
 *  unsubscribe or a stop resolves it. */
function onSubscriptionEnd(subscription: BackendSubscription, onEnd: () => void): void {
  const ended = () => {
    subscription.ready.catch(reportServerChannelError)
    onEnd()
  }
  if (subscription.state() === 'closed') return ended()
  subscription.onStateChange((state) => {
    if (state === 'closed') ended()
  })
}

function reportStaticListenerError(error: unknown): void {
  if (classifyTelefuncError(error, isExpectedChannelFailure).kind !== 'bug') return
  reportServerChannelError(error)
}
