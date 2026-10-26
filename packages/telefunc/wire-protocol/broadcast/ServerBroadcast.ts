export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  BroadcastListenerByKind,
  BroadcastListeners,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelPublishInfo,
} from '../channel/channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shieldKey.js'
import { invokeChannelListener, makePublishInfo } from '../channel/channel.js'
import { reportServerChannelError } from '../channel/server/channel.js'
import { ServerBroadcastEnd } from '../channel/server/ServerBroadcastEnd.js'
import type { ShieldValidator } from '../../node/server/shield.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'
import type { BroadcastPayload, BroadcastRoute } from '../backend/broadcast/contract.js'
import { followBroadcastPlane, getBroadcastBackend, unfollowBroadcastPlane } from '../backend/install.js'
import type { BackendReceiver, BackendSubscription } from '../backend/subscription.js'
import { reportSubscriptionEnd } from '../backend/SubscriptionManager.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import { markHandled } from '../../utils/markHandled.js'
import { Listeners } from '../../utils/Listeners.js'
import { encodePublishText, encodePublishBinary } from '../wire/connectionFrame.js'
import type { BroadcastKind, WirePublishInfo } from '../wire/connectionFrame.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../node/server/Abort.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
type BroadcastUnsubscribe = () => void

class ServerBroadcast<T = unknown> {
  readonly [SERVER_BROADCAST_BRAND] = true
  /** @see ChannelShield in ../channel/channel.ts — broadcast only validates incoming
   *  publishes from clients (the `data` direction). The `ack` slot is unused: publish
   *  receipts are server-generated, not client-supplied. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly key: string
  /** @internal Its channel to the page it is returned to. */
  readonly _end: ServerBroadcastEnd
  /** @internal The `data` shield its response found for it, which each publish from its page passes. */
  _publishShield: ShieldValidator | undefined

  private readonly _subscribers: BroadcastListeners<T> = { text: new Listeners(), binary: new Listeners() }
  private readonly _routes: { [Kind in BroadcastKind]: RouteSubscription<Kind> }
  private readonly _peerSubscriptions: Record<BroadcastKind, boolean> = { text: false, binary: false }

  constructor(opts: { key: string }) {
    assertBroadcastKey(opts.key)
    this.key = opts.key
    this._end = new ServerBroadcastEnd()
    this._end.onPublish((serialized) => this._publishFromPage(serialized))
    this._end.onPublishBinary((data) => this._publish('binary', data))
    this._end.onSubscription((kind, on) => {
      this._peerSubscriptions[kind] = on
      this._syncSubscription(kind)
    })
    this._routes = {
      text: new RouteSubscription({ key: this.key, kind: 'text' }, (payload, rawInfo) =>
        this._deliverBroadcastMessage(payload, rawInfo),
      ),
      binary: new RouteSubscription({ key: this.key, kind: 'binary' }, (payload, rawInfo) =>
        this._deliverBroadcastBinaryMessage(payload, rawInfo),
      ),
    }
    this._end.onClose(() => {
      for (const route of Object.values(this._routes)) route.close()
    })
  }

  static isServerBroadcast(value: unknown): value is ServerBroadcast {
    return value !== null && typeof value === 'object' && SERVER_BROADCAST_BRAND in value
  }

  get id(): string {
    return this._end.id
  }

  get isClosed(): boolean {
    return this._end.isClosed
  }

  get isConnected(): boolean {
    return this._end.isConnected
  }

  // Channel methods that don't apply to broadcast: throw at runtime.
  send(): never {
    assertUsage(false, '`send()` is not available on a `BroadcastChannel` — use `publish()`.')
  }
  sendBinary(): never {
    assertUsage(false, '`sendBinary()` is not available on a `BroadcastChannel` — use `publishBinary()`.')
  }
  listen(): never {
    assertUsage(false, '`listen()` is not available on a `BroadcastChannel` — use `subscribe()`.')
  }
  listenBinary(): never {
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

  onClose(callback: ChannelCloseCallback): void {
    this._end.onClose(callback)
  }

  onOpen(callback: () => void): void {
    this._end.onOpen(callback)
  }

  onDisconnect(callback: () => void): void {
    this._end.onDisconnect(callback)
  }

  onReconnect(callback: () => void): void {
    this._end.onReconnect(callback)
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    return this._end.close(opts)
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    this._end.abort(abortValue, message)
  }

  _deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const data = parse(serialized) as ChannelData<T>
    const listeners = this._subscribers.text.list()
    if (!this._callListeners(listeners, rawInfo, (cb, info) => cb(data, info))) return
    if (!this._peerSubscriptions.text) return
    this._end.deliver(encodePublishText(serialized, rawInfo))
  }

  _deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    // Each listener gets bytes of its own: one may write into them or transfer their buffer.
    const listeners = this._subscribers.binary.list()
    if (!this._callListeners(listeners, rawInfo, (cb, info) => cb(new Uint8Array(data), info))) return
    if (!this._peerSubscriptions.binary) return
    this._end.deliverBinary(encodePublishBinary(data, rawInfo))
  }

  /** Calls each listener directly, as a channel's receive does, since this runs per subscriber per message; false once
   *  a listener's error ended the channel. */
  private _callListeners<CB>(
    listeners: readonly CB[],
    rawInfo: WirePublishInfo,
    call: (cb: CB, info: ChannelPublishInfo) => unknown,
  ): boolean {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of listeners) {
      try {
        const result = call(cb, info)
        if (isPromise(result)) void result.catch((error: unknown) => this._end._handleCallbackError(error))
      } catch (error) {
        if (this._end._handleCallbackError(error)) return false
      }
    }
    return true
  }

  // --- Internal broadcast helpers ---

  private _subscribe<K extends BroadcastKind>(kind: K, callback: BroadcastListenerByKind<T>[K]): BroadcastUnsubscribe {
    if (!this._end.isClosed) this._routes[kind].open()
    const remove = this._subscribers[kind].add(callback)
    return () => {
      if (remove()) this._syncSubscription(kind)
    }
  }

  private _syncSubscription(kind: BroadcastKind): void {
    if (this._end.isClosed || (!this._peerSubscriptions[kind] && this._subscribers[kind].size === 0)) {
      this._routes[kind].close()
      return
    }
    this._routes[kind].open()
  }

  private _publishTracked<Kind extends BroadcastKind>(
    kind: Kind,
    payload: BroadcastPayload<Kind>,
  ): Promise<ChannelPublishAck> {
    return markHandled(this._end._trackAck(Promise.resolve(this._publish(kind, payload))))
  }

  private _publish<Kind extends BroadcastKind>(
    kind: Kind,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    return getBroadcastBackend().publish({ key: this.key, kind }, payload)
  }

  /** Without a shield, the page's payload is never parsed here. */
  private _publishFromPage(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    if (this._publishShield) {
      const result = this._publishShield(parse(serialized))
      if (result !== true) throw new ShieldValidationError(result)
    }
    return this._publish('text', serialized)
  }
}

/** Public surface of a `BroadcastChannel` instance — same shape `Channel` uses to hide internal
 *  `_methods` from autocomplete on user-facing `chat.` etc. The underlying class is `ServerBroadcast`. */
type BroadcastChannel<T = unknown> = {
  readonly key: string
  readonly id: string
  readonly isClosed: boolean
  readonly isConnected: boolean
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
  onDisconnect(callback: () => void): void
  onReconnect(callback: () => void): void
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
    return markHandled(getBroadcastBackend().publish({ key, kind: 'text' }, stringify(data)))
  },
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe {
    return subscribeRoute({ key, kind: 'text' }, (payload) => parse(payload) as ChannelData<U>, callback)
  },
  publishBinary(key: string, data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return markHandled(getBroadcastBackend().publish({ key, kind: 'binary' }, data))
  },
  subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe {
    // Bytes of its own: every subscriber on the key gets the same delivery.
    return subscribeRoute({ key, kind: 'binary' }, (payload) => new Uint8Array(payload), callback)
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
      reportListenerError,
    )
  })
  subscription.open()
  return () => subscription.close()
}

/** A listener's `Abort` is no bug, and there is no channel for it to close. */
function reportListenerError(error: unknown): void {
  if (!isAbort(error)) reportServerChannelError(error)
}

/** A route's subscription while wanted; one that ends on its own is reported and replaced once, and a transport that
 *  replaces the plane gets it. */
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
        reportSubscriptionEnd(error, reportServerChannelError)
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

function assertBroadcastKey(key: unknown): void {
  assertUsage(typeof key === 'string' && key.isWellFormed(), 'The broadcast key should be a well-formed string')
}
