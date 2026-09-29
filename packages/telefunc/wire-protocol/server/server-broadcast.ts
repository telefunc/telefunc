export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  BroadcastListeners,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelPublishInfo,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { invokeChannelListener, makePublishInfo } from '../channel.js'
import { ServerChannel, reportServerChannelError } from './channel.js'
import type { BroadcastPayload, BroadcastRoute, PublishResult } from '../backend/broadcast/contract.js'
import { followBroadcastPlane, getBroadcastBackend, unfollowBroadcastPlane } from '../backend/install.js'
import type { BackendReceiver, BackendSubscription } from '../backend/subscription.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import { markHandled } from '../../utils/markHandled.js'
import { ChannelOverflowError } from '../channel-errors.js'
import { ACK_STATUS, encodePublishText, encodePublishBinary } from '../shared-ws.js'
import type { BroadcastKind, WirePublishInfo } from '../shared-ws.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
type BroadcastUnsubscribe = () => void

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

  /** Each kind's array is replaced, never mutated, so a delivery iterates the listeners it started with. */
  private readonly _subscribers: BroadcastListeners<T> = { text: [], binary: [] }
  private readonly _routes: { [Kind in BroadcastKind]: RouteSubscription<Kind> }
  private readonly _peerSubscriptions: Record<BroadcastKind, boolean> = { text: false, binary: false }

  constructor(opts: { key: string }) {
    super({ publishes: true })
    assertBroadcastKey(opts.key)
    this.key = opts.key
    this._routes = {
      text: new RouteSubscription({ key: this.key, kind: 'text' }, (payload, rawInfo) =>
        this._deliverBroadcastMessage(payload, rawInfo),
      ),
      binary: new RouteSubscription({ key: this.key, kind: 'binary' }, (payload, rawInfo) =>
        this._deliverBroadcastBinaryMessage(payload, rawInfo),
      ),
    }
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
    return this._publishTracked('text', stringify(data))
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
    const data = parse(serialized) as ChannelData<T>
    if (!this._callListeners(this._subscribers.text, data, rawInfo)) return
    if (!this._peerSubscriptions.text) return
    this._sendPublish(encodePublishText(serialized, rawInfo))
  }

  _deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    if (!this._callListeners(this._subscribers.binary, data, rawInfo)) return
    if (!this._peerSubscriptions.binary) return
    this._sendPublishBinary(encodePublishBinary(data, rawInfo))
  }

  /** Calls each listener directly, as a channel's receive does, since this runs per subscriber per message; false once a
   *  listener's error ended the channel. */
  private _callListeners<Data>(
    listeners: Array<(data: Data, info: ChannelPublishInfo) => unknown>,
    data: Data,
    rawInfo: WirePublishInfo,
  ): boolean {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of listeners) {
      try {
        const result = cb(data, info)
        if (isPromise(result)) void result.catch((error: unknown) => this._handleCallbackError(error))
      } catch (error) {
        if (this._handleCallbackError(error)) return false
      }
    }
    return true
  }

  override _onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    this._peerSubscriptions[kind] = on
    this._syncSubscription(kind)
  }

  protected override _shutdown(err?: Error, pageGone?: boolean): void {
    for (const route of Object.values(this._routes)) route.close()
    super._shutdown(err, pageGone)
  }

  // --- Internal broadcast helpers ---

  private _subscribe<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListeners<T>[K][number],
  ): BroadcastUnsubscribe {
    if (!this._isClosed) this._routes[kind].open()
    this._subscribers[kind] = [...this._subscribers[kind], callback] as BroadcastListeners<T>[K]
    return () => {
      const index = (this._subscribers[kind] as Array<typeof callback>).indexOf(callback)
      if (index < 0) return
      this._subscribers[kind] = this._subscribers[kind].filter((_, j) => j !== index) as BroadcastListeners<T>[K]
      this._syncSubscription(kind)
    }
  }

  private _syncSubscription(kind: BroadcastKind): void {
    if (this._isClosed || (!this._peerSubscriptions[kind] && this._subscribers[kind].length === 0)) {
      this._routes[kind].close()
      return
    }
    this._routes[kind].open()
  }

  private _publishTracked<Kind extends BroadcastKind>(
    kind: Kind,
    payload: BroadcastPayload<Kind>,
  ): Promise<ChannelPublishAck> {
    return markHandled(this._trackAck(Promise.resolve(this._publish(kind, payload))))
  }

  private _publish<Kind extends BroadcastKind>(
    kind: Kind,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    return publishRoute({ key: this.key, kind }, payload)
  }

  private async _dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
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
      const result = await this._publish('text', serialized)
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      this._sendPublishFailure(seq, err)
    }
  }

  private async _dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      const result = await this._publish('binary', data)
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      this._sendPublishFailure(seq, err)
    }
  }

  /** A full buffer refusing the publish is no bug: the client's publish() rejects with a ChannelOverflowError. */
  private _sendPublishFailure(seq: number, err: unknown): void {
    if (err instanceof ChannelOverflowError) return this._sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (this._handleCallbackError(err)) return
    this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
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
  publish<U = unknown>(key: string, data: ChannelData<U>): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return markHandled(publishRoute({ key, kind: 'text' }, stringify(data)))
  },
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe {
    return subscribeRoute({ key, kind: 'text' }, (payload) => parse(payload) as ChannelData<U>, callback)
  },
  publishBinary(key: string, data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return markHandled(publishRoute({ key, kind: 'binary' }, data))
  },
  subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe {
    return subscribeRoute({ key, kind: 'binary' }, (payload) => payload, callback)
  },
}

function subscribeRoute<Kind extends BroadcastKind, Data>(
  route: BroadcastRoute<Kind>,
  decode: (payload: BroadcastPayload<Kind>) => Data,
  callback: (data: Data, info: ChannelPublishInfo) => unknown,
): BroadcastUnsubscribe {
  assertBroadcastKey(route.key)
  const subscription = new RouteSubscription(route, (payload, info) => {
    invokeChannelListener(
      callback,
      [decode(payload), makePublishInfo(route.key, info.seq, info.timestamp)],
      reportServerChannelError,
    )
  })
  subscription.open()
  return () => subscription.close()
}

// Every consumer of a shared subscription gets its end as one failure object, reported once.
const reportedEnds = new WeakSet<object>()
function reportSubscriptionEnd(error: unknown): void {
  if (typeof error === 'object' && error !== null) {
    if (reportedEnds.has(error)) return
    reportedEnds.add(error)
  }
  reportServerChannelError(error)
}

/** A route's subscription while wanted; one that ends on its own is reported and replaced once, as a Room lane's is, and
 *  a transport that replaces the plane gets it. */
class RouteSubscription<Kind extends BroadcastKind> {
  private _current: BackendSubscription | null = null

  constructor(
    private readonly _route: BroadcastRoute<Kind>,
    private readonly _receiver: BackendReceiver<BroadcastPayload<Kind>>,
  ) {}

  /** Subscribes unless a subscription is live; throws where the backend can't bind the route. */
  open(): void {
    if (this._current === null) this._subscribe(false)
    followBroadcastPlane(this)
  }

  close(): void {
    unfollowBroadcastPlane(this)
    const current = this._current
    this._current = null
    void current?.unsubscribe()
  }

  /** Subscribes on the plane that replaced this subscription's. */
  planeReplaced(): void {
    this.close()
    this.open()
  }

  private _subscribe(replacing: boolean): void {
    // The plane in effect now; the subscription keeps its own handle.
    const subscription = getBroadcastBackend().subscribe(this._route, this._receiver)
    this._current = subscription
    let wasReady = subscription.state() === 'ready'
    // Only a terminal end rejects `ready`, after the manager retired the subscription; an unsubscribe resolves it.
    const ended = () =>
      void subscription.ready.catch((error: unknown) => {
        reportSubscriptionEnd(error)
        if (this._current !== subscription) return
        this._current = null
        if (!replacing || wasReady) this._subscribe(true)
      })
    if (subscription.state() === 'closed') return ended()
    subscription.onStateChange((state) => {
      if (state === 'ready') wasReady = true
      else if (state === 'closed') ended()
    })
  }
}

function publishRoute<Kind extends BroadcastKind>(
  route: BroadcastRoute<Kind>,
  payload: BroadcastPayload<Kind>,
): ChannelPublishAck | Promise<ChannelPublishAck> {
  const result = getBroadcastBackend().publish(route, payload)
  return isPromise(result) ? result.then((receipt) => toAck(route.key, receipt)) : toAck(route.key, result)
}

/** The backend's receipt as the public ack, carrying its key like a subscriber's `info`. */
function toAck(key: string, { seq, timestamp, meta, receivers }: PublishResult): ChannelPublishAck {
  return receivers === undefined ? { key, seq, timestamp, meta } : { key, seq, timestamp, meta, receivers }
}

function assertBroadcastKey(key: unknown): void {
  assertUsage(typeof key === 'string' && key.isWellFormed(), 'The broadcast key should be a well-formed string')
}
