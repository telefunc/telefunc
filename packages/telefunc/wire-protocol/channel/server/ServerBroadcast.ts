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
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { getChannelLimits } from '../../../node/server/serverConfig.js'
import { broadcastMessages } from '../channelMessages.js'
import { ServerBroadcastEnd, type BroadcastEndOwner } from './ServerBroadcastEnd.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import type { BroadcastPayload, BroadcastRoute } from '../../backend/driver.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import { followBroadcastPlane, getBroadcastPlane, unfollowBroadcastPlane } from '../../backend/install.js'
import type { BackendSubscription } from '../../backend/SharedSubscriptions.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assertUsage } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { markHandled } from '../../../utils/markHandled.js'
import { Listeners } from '../../../utils/Listeners.js'
import { encodePublish, type WirePublishInfo } from '../../wire/publishInfo.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../../node/server/Abort.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
type BroadcastUnsubscribe = () => void

class ServerBroadcast<T = unknown> implements BroadcastEndOwner {
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
  /** @internal The `data` shield each publish from its page passes; set by values/server/response/broadcast.ts. */
  _publishShield: ShieldValidator | undefined

  private readonly subscribers: BroadcastListeners<T> = { text: new Listeners(), binary: new Listeners() }
  private readonly routes: { [Kind in BroadcastKind]: RouteSubscription<Kind> }
  private readonly peerSubscriptions: Record<BroadcastKind, boolean> = { text: false, binary: false }

  constructor(opts: { key: string }) {
    assertBroadcastKey(opts.key)
    this.key = opts.key
    this._end = new ServerBroadcastEnd()
    this._end.setOwner(this)
    this.routes = {
      text: new RouteSubscription({
        route: { key: this.key, kind: 'text' },
        receiver: (payload, rawInfo) => this.deliverBroadcastMessage(payload, rawInfo),
      }),
      binary: new RouteSubscription({
        route: { key: this.key, kind: 'binary' },
        receiver: (payload, rawInfo) => this.deliverBroadcastBinaryMessage(payload, rawInfo),
      }),
    }
    this._end.onClose(() => {
      for (const route of Object.values(this.routes)) route.close()
      this.subscribers.text.clear()
      this.subscribers.binary.clear()
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

  get opened(): Promise<void> {
    return this._end.opened
  }

  get closed(): Promise<void> {
    return this._end.closed
  }

  /** Holds what its loop falls behind by within the channel buffer limits a page's channel gets. */
  messages(options?: { signal?: AbortSignal }) {
    const { bufferLimit, bufferLimitBinary } = getChannelLimits()
    return broadcastMessages<ChannelData<T>>(this, { text: bufferLimit, binary: bufferLimitBinary }, options)
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
    return this.publishTracked('text', stringify(data))
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): () => void {
    return this.subscribeKind('text', callback, options)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    return this.publishTracked('binary', data)
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): () => void {
    return this.subscribeKind('binary', callback, options)
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): void {
    this._end.onClose(callback, options)
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): void {
    this._end.onOpen(callback, options)
  }

  onDisconnect(callback: () => void, options?: { signal?: AbortSignal }): void {
    this._end.onDisconnect(callback, options)
  }

  onReconnect(callback: () => void, options?: { signal?: AbortSignal }): void {
    this._end.onReconnect(callback, options)
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    return this._end.close(opts)
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    this._end.abort(abortValue, message)
  }

  /** Calls each listener directly, as a channel's receive does, since this runs per subscriber per message; a
   *  listener's error that ends the channel stops the delivery. */
  private deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = { key: this.key, seq: rawInfo.seq, timestamp: rawInfo.timestamp }
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this.subscribers.text.list()) {
      try {
        const result = cb(data, info)
        if (isPromise(result)) result.catch((err: unknown) => this._end._handleCallbackError(err))
      } catch (err) {
        if (this._end._handleCallbackError(err)) return
      }
    }
    if (!this.peerSubscriptions.text) return
    this._end.deliver(encodePublish(serialized, rawInfo))
  }

  private deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = { key: this.key, seq: rawInfo.seq, timestamp: rawInfo.timestamp }
    for (const cb of this.subscribers.binary.list()) {
      try {
        // Each listener gets bytes of its own: one may write into them or transfer their buffer.
        const result = cb(new Uint8Array(data), info)
        if (isPromise(result)) result.catch((err: unknown) => this._end._handleCallbackError(err))
      } catch (err) {
        if (this._end._handleCallbackError(err)) return
      }
    }
    if (!this.peerSubscriptions.binary) return
    this._end.deliverBinary(encodePublish(data, rawInfo))
  }

  // --- Internal broadcast helpers ---

  private subscribeKind<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListenerByKind<T>[K],
    options: { signal?: AbortSignal },
  ): BroadcastUnsubscribe {
    if (options.signal?.aborted) return () => {}
    if (!this._end.isClosed) this.routes[kind].open()
    return this.subscribers[kind].add(callback, { ...options, onRemove: () => this.syncSubscription(kind) })
  }

  private syncSubscription(kind: BroadcastKind): void {
    if (this._end.isClosed || (!this.peerSubscriptions[kind] && this.subscribers[kind].size === 0)) {
      this.routes[kind].close()
      return
    }
    this.routes[kind].open()
  }

  private publishTracked<Kind extends BroadcastKind>(
    kind: Kind,
    payload: BroadcastPayload<Kind>,
  ): Promise<ChannelPublishAck> {
    return markHandled(this._end.trackAck(Promise.resolve(this.publishPayload(kind, payload))))
  }

  private publishPayload<Kind extends BroadcastKind>(
    kind: Kind,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    return publishOnPlane({ key: this.key, kind }, payload)
  }

  /** @internal Without a shield, the page's payload is never parsed here. */
  _onPagePublish(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    if (this._publishShield) {
      const result = this._publishShield(parse(serialized))
      if (result !== true) throw new ShieldValidationError(result)
    }
    return this.publishPayload('text', serialized)
  }

  /** @internal */
  _onPagePublishBinary(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    return this.publishPayload('binary', data)
  }

  /** @internal */
  _onPageSubscription(kind: BroadcastKind, on: boolean): void {
    this.peerSubscriptions[kind] = on
    this.syncSubscription(kind)
  }
}

/** Public surface of a `BroadcastChannel` instance — same shape `Channel` uses to hide internal
 *  `_methods` from autocomplete on user-facing `chat.` etc. The underlying class is `ServerBroadcast`. */
type BroadcastChannel<T = unknown> = {
  readonly key: string
  readonly id: string
  readonly isClosed: boolean
  readonly isConnected: boolean
  readonly opened: Promise<void>
  readonly closed: Promise<void>
  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<
    ({ type: 'text'; data: ChannelData<T> } | { type: 'binary'; data: Uint8Array }) & { info: ChannelPublishInfo }
  >
  readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  publish(data: ChannelData<T>): Promise<ChannelPublishAck>
  subscribe(callback: BroadcastListener<T>, options?: { signal?: AbortSignal }): () => void
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  subscribeBinary(callback: BroadcastBinaryListener, options?: { signal?: AbortSignal }): () => void
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): void
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): void
  onDisconnect(callback: () => void, options?: { signal?: AbortSignal }): void
  onReconnect(callback: () => void, options?: { signal?: AbortSignal }): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

/** A failed publish rejects, also when the backend or its transport throws, so a caller's `.catch()` sees it. */
function publishOnPlane<Kind extends BroadcastKind>(
  route: BroadcastRoute<Kind>,
  payload: BroadcastPayload<Kind>,
): ChannelPublishAck | Promise<ChannelPublishAck> {
  try {
    return markHandled(getBroadcastPlane().publish(route, payload))
  } catch (err) {
    return markHandled(Promise.reject(err))
  }
}

const BroadcastChannel = ServerBroadcast as {
  new <T = unknown>(opts: { key: string }): BroadcastChannel<T>
}

const Broadcast = {
  publish<U = unknown>(key: string, data: ChannelData<U>): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return publishOnPlane({ key, kind: 'text' }, stringify(data))
  },
  subscribe<U = unknown>(
    key: string,
    callback: BroadcastListener<U>,
    options?: { signal?: AbortSignal },
  ): BroadcastUnsubscribe {
    return subscribeWithSignal(
      () => subscribeRoute({ key, kind: 'text' }, (payload) => parse(payload) as ChannelData<U>, callback),
      options,
    )
  },
  publishBinary(key: string, data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return publishOnPlane({ key, kind: 'binary' }, data)
  },
  subscribeBinary(
    key: string,
    callback: BroadcastBinaryListener,
    options?: { signal?: AbortSignal },
  ): BroadcastUnsubscribe {
    // Bytes of its own: every subscriber on the key gets the same delivery.
    return subscribeWithSignal(
      () => subscribeRoute({ key, kind: 'binary' }, (payload) => new Uint8Array(payload), callback),
      options,
    )
  },
}

function subscribeRoute<Kind extends BroadcastKind, Data>(
  route: BroadcastRoute<Kind>,
  decode: (payload: BroadcastPayload<Kind>) => Data,
  callback: (data: Data, info: ChannelPublishInfo) => unknown,
): BroadcastUnsubscribe {
  assertBroadcastKey(route.key)
  const subscription = new RouteSubscription({
    route,
    receiver: (payload, info) => {
      callStaticListener(callback, decode(payload), { key: route.key, seq: info.seq, timestamp: info.timestamp })
    },
  })
  subscription.open()
  return () => subscription.close()
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
  if (!isAbort(err)) handleCallbackBug(err)
}

function subscribeWithSignal(subscribe: () => () => void, { signal }: { signal?: AbortSignal } = {}): () => void {
  if (signal?.aborted) return () => {}
  let unsubscribe: (() => void) | undefined = subscribe()
  const remove = () => {
    const current = unsubscribe
    unsubscribe = undefined
    signal?.removeEventListener('abort', remove)
    current?.()
  }
  signal?.addEventListener('abort', remove, { once: true })
  if (signal?.aborted) remove()
  return remove
}

type RouteReceiver<Kind extends BroadcastKind> = (payload: BroadcastPayload<Kind>, ordering: OrderingInfo) => void

type RouteSubscriptionOptions<Kind extends BroadcastKind> = {
  route: BroadcastRoute<Kind>
  receiver: RouteReceiver<Kind>
}

/** A route's subscription while wanted; one that ends on its own is reported and replaced once, and a transport that
 *  replaces the plane gets it. */
class RouteSubscription<Kind extends BroadcastKind> {
  private readonly route: BroadcastRoute<Kind>
  private readonly receiver: RouteReceiver<Kind>
  private current: BackendSubscription | null = null

  constructor({ route, receiver }: RouteSubscriptionOptions<Kind>) {
    this.route = route
    this.receiver = receiver
  }

  /** Subscribes unless a subscription is live; throws outside a Cloudflare session. */
  open(): void {
    if (this.current === null) this.subscribe(false)
    followBroadcastPlane(this)
  }

  close(): void {
    unfollowBroadcastPlane(this)
    const current = this.current
    this.current = null
    void current?.unsubscribe()
  }

  /** Subscribes on the plane that replaced this subscription's. */
  planeReplaced(): void {
    this.close()
    this.open()
  }

  private subscribe(replacing: boolean): void {
    // The plane in effect now; the subscription keeps its own handle.
    const subscription = getBroadcastPlane().subscribe(this.route, this.receiver)
    this.current = subscription
    let wasReady = subscription.state() === 'ready'
    // Only a terminal end rejects `ready`, after the shared subscriptions retired and reported the subscription; an
    // unsubscribe resolves it.
    const ended = () =>
      void subscription.ready.catch(() => {
        if (this.current !== subscription) return
        this.current = null
        if (!replacing || wasReady) this.subscribe(true)
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
