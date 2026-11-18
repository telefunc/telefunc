export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  BroadcastListenerByKind,
  BroadcastListeners,
  BroadcastPublishOptions,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelPublishInfo,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
import { Readiness, neverInEffect, stringifyMessage, withReady, type Unsubscribe } from '../channel.js'
import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { broadcastMessages } from '../channelMessages.js'
import { binaryData, binaryPayload, isLossy, textPayload } from '../broadcastPayload.js'
import { ServerBroadcastEnd, type BroadcastEndOwner } from './ServerBroadcastEnd.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import type { BroadcastPayload, BroadcastRoute } from '../../backend/driver.js'
import type { OrderingInfo } from '../../backend/driverOrdering.js'
import { followBroadcastPlane, getBroadcastPlane, unfollowBroadcastPlane } from '../../backend/install.js'
import type { BackendSubscription } from '../../backend/SharedSubscriptions.js'
import { parse } from '@brillout/json-serializer/parse'
import { assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { queueMemory } from './channelMemory.js'
import { isPromise } from '../../../utils/isPromise.js'
import { markHandled } from '../../../utils/markHandled.js'
import { Listeners } from '../../../utils/Listeners.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { publishBody, type PublishBody } from './PublishBody.js'
import { EVERY_STREAM } from '../ownStream.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../../node/server/Abort.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')

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
  /** What waits for each kind's subscription to take effect: its subscribers' and its page's. */
  private readonly waiting: Record<BroadcastKind, Set<Readiness>> = { text: new Set(), binary: new Set() }
  /** The stream of each kind's lossy publishes: a text message doesn't take a binary one's place. */
  private readonly streams: Record<BroadcastKind, string>

  constructor(opts: BroadcastChannelOptions) {
    const { key } = opts
    assertBroadcastKey(key)
    this.key = key
    this.streams = { text: `text:${key}`, binary: `binary:${key}` }
    this._end = new ServerBroadcastEnd()
    this._end.setOwner(this)
    this.routes = {
      text: new RouteSubscription({
        route: { key: this.key, kind: 'text' },
        receiver: (payload, rawInfo) => this.deliverBroadcastMessage(payload, rawInfo),
        onReady: () => settleAll(this.waiting.text, true),
        onFailed: (err) => this._end.endNotInEffect(err),
        onEnded: (err) => this._end.endEnded(err),
      }),
      binary: new RouteSubscription({
        route: { key: this.key, kind: 'binary' },
        receiver: (payload, rawInfo) => this.deliverBroadcastBinaryMessage(payload, rawInfo),
        onReady: () => settleAll(this.waiting.binary, true),
        onFailed: (err) => this._end.endNotInEffect(err),
        onEnded: (err) => this._end.endEnded(err),
      }),
    }
    this._end.onClose(() => {
      // What waits gets the end the close ends with, once that is decided: one its close callbacks removed too.
      void this._end.closed.then(
        () => this.failWaiting(),
        () => this.failWaiting(),
      )
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

  messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options, queueMemory())
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

  publish(data: ChannelData<T>, options?: BroadcastPublishOptions): Promise<ChannelPublishAck> {
    return this.publishTracked('text', textPayload(stringifyMessage(data, 'publish'), options?.lossy))
  }

  /** In effect once its backend delivers what any server publishes to the key. */
  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.subscribeKind('text', callback, options)
  }

  publishBinary(data: Uint8Array, options?: BroadcastPublishOptions): Promise<ChannelPublishAck> {
    return this.publishTracked('binary', binaryPayload(data, options?.lossy))
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.subscribeKind('binary', callback, options)
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void {
    return this._end.onClose(callback, options)
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void {
    return this._end.onOpen(callback, options)
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

  private deliverBroadcastMessage(payload: string, rawInfo: WirePublishInfo): void {
    const info = { key: this.key, seq: rawInfo.seq, timestamp: rawInfo.timestamp }
    const data = parse(payload) as ChannelData<T>
    for (const { listener } of this.subscribers.text.list()) {
      if (!listener) continue
      if (this.callListener(listener, data, info)) return
    }
    // Asked before a body is made, which admits it against the budget: a page that began to close costs nothing.
    if (!this.peerSubscriptions.text || this._end.isClosed) return this._end._endOverflowed()
    this._end.deliver(bodyOf(payload, rawInfo, this.streams.text))
  }

  private deliverBroadcastBinaryMessage(payload: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = { key: this.key, seq: rawInfo.seq, timestamp: rawInfo.timestamp }
    for (const { listener } of this.subscribers.binary.list()) {
      if (!listener) continue
      // A copy of its own, which a Node.js Buffer's slice() isn't: a listener may write into it or transfer its buffer.
      if (this.callListener(listener, new Uint8Array(binaryData(payload)), info)) return
    }
    if (!this.peerSubscriptions.binary || this._end.isClosed) return this._end._endOverflowed()
    this._end.deliver(bodyOf(payload, rawInfo, this.streams.binary))
  }

  /** Calls `listener` with a publish; true where it threw `Abort`, which aborts the channel, so the rest get none. */
  private callListener<D>(
    listener: (data: D, info: ChannelPublishInfo) => unknown,
    data: D,
    info: ChannelPublishInfo,
  ): boolean {
    try {
      const result = listener(data, info)
      if (isPromise(result)) result.catch((err: unknown) => this._end._handleCallbackError(err))
    } catch (err) {
      return this._end._handleCallbackError(err)
    }
    return false
  }

  // --- Internal broadcast helpers ---

  /** Its `ready` settles as a page's does: once the backend delivers to it what any server publishes to the key, or once
   *  it is removed, and it rejects where the broadcast ends first. One added to a closed broadcast whose kind isn't in
   *  effect never takes effect. */
  private subscribeKind<K extends BroadcastKind>(
    kind: K,
    callback: BroadcastListenerByKind<T>[K],
    options: { signal?: AbortSignal },
  ): Unsubscribe {
    if (options.signal?.aborted) return withReady(() => {})
    const route = this.routes[kind]
    if (this._end.isClosed && !route.inEffect) return neverInEffect(this._end._endError())
    // Both before its backend starts: it may deliver a message before its subscribe returns, which the listener hears,
    // and to which a listener may end the broadcast, which settles what waits with that end.
    const readiness = this.waitFor(kind)
    const remove = this.subscribers[kind].add(callback, {
      ...options,
      onRemove: () => {
        // Once its close callbacks ran, one that waits gets the end, whatever they do.
        if (!this._end._closeFired) {
          this.waiting[kind].delete(readiness)
          readiness.settle(true)
        }
        this.syncSubscription(kind)
      },
    })
    if (!this._end.isClosed) this.openRoute(kind, remove)
    return withReady(remove, readiness)
  }

  /** What waits for `kind`'s subscription to take effect, settled at once where it is in effect. */
  private waitFor(kind: BroadcastKind): Readiness {
    const readiness = new Readiness()
    if (this.routes[kind].inEffect) readiness.settle(true)
    else this.waiting[kind].add(readiness)
    return readiness
  }

  /** Where it throws (outside a Cloudflare session), it keeps no subscriber of the call that threw. */
  private openRoute(kind: BroadcastKind, remove: () => void): void {
    try {
      this.routes[kind].open()
    } catch (err) {
      remove()
      throw err
    }
  }

  /** Its subscribers and its page waiting for their subscription to take effect get its end. */
  private failWaiting(): void {
    const endError = this._end._endError()
    settleAll(this.waiting.text, endError)
    settleAll(this.waiting.binary, endError)
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

  /** @internal `BroadcastEndOwner`: in effect once its kind's backend subscription is, at once to leave it. One its
   *  backend refuses at once fails as one it gives up later does. */
  _onPageSubscription(kind: BroadcastKind, on: boolean): Promise<void> | undefined {
    this.peerSubscriptions[kind] = on
    if (!on) this._end._dropHeldOf(kind)
    try {
      this.syncSubscription(kind)
    } catch (err) {
      if (!on) throw err
      handleCallbackBug(err)
      return Promise.reject(err)
    }
    return on ? this.routes[kind].ready : undefined
  }

  /** @internal `BroadcastEndOwner`: the kind `stream` is of, or either kind without one. */
  _pageReads(stream?: string): boolean {
    const { text, binary } = this.peerSubscriptions
    if (stream === undefined) return text || binary
    return stream === this.streams.text ? text : stream === this.streams.binary && binary
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
  /** Serializes `data` as JSON: bytes go through `publishBinary()`. */
  publish(data: ChannelData<T>, options?: BroadcastPublishOptions): Promise<ChannelPublishAck>
  /** Receive `(data, info)` for each publish to the key. The subscription is in effect once it gets what any server
   *  publishes to the key (`ready`): while the broadcast backend is unreachable, that waits, and where the backend gives
   *  it up, the broadcast ends: with its error on the server, and on its page with a `ChannelClosedError` that says its
   *  subscription didn't take effect. A telefunction the page calls after `subscribe()` runs once the
   *  subscription is in effect, or once the broadcast ended.
   *
   *  https://telefunc.com/channel#chat-example */
  subscribe(callback: BroadcastListener<T>, options?: { signal?: AbortSignal }): Unsubscribe
  publishBinary(data: Uint8Array, options?: BroadcastPublishOptions): Promise<ChannelPublishAck>
  /** As `subscribe()`, for binary publishes. */
  subscribeBinary(callback: BroadcastBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void
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

type BroadcastChannelOptions = {
  key: string
}

const BroadcastChannel = ServerBroadcast as {
  new <T = unknown>(opts: BroadcastChannelOptions): BroadcastChannel<T>
}

const globalObject = getGlobalObject('wire-protocol/channel/server/ServerBroadcast.ts', {
  /** One delivery hands every subscriber on this server the same `info`, so they share the body the first one makes. */
  publishBodies: new WeakMap<WirePublishInfo, PublishBody>(),
})

/** A lossy publish waits in its kind's `stream`, and a plain one goes after those of both: its key orders them all. */
function bodyOf(payload: string | Uint8Array, info: WirePublishInfo, stream: string): PublishBody {
  let body = globalObject.publishBodies.get(info)
  if (!body) {
    const lossy = isLossy(payload)
    body = publishBody(payload, info, lossy ? stream : EVERY_STREAM, lossy)
    globalObject.publishBodies.set(info, body)
  }
  return body
}

const Broadcast = {
  /** Serializes `data` as JSON: bytes go through `publishBinary()`. */
  publish<U = unknown>(
    key: string,
    data: ChannelData<U>,
    options?: BroadcastPublishOptions,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return publishOnPlane({ key, kind: 'text' }, textPayload(stringifyMessage(data, 'publish'), options?.lossy))
  },
  /** Receive `(data, info)` for each publish to `key`, in effect once it gets what any server publishes to `key`. */
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>, options?: { signal?: AbortSignal }): Unsubscribe {
    if (options?.signal?.aborted) return withReady(() => {})
    const decode = (payload: string) => parse(payload) as ChannelData<U>
    return subscribeRoute({ key, kind: 'text' }, decode, callback, options?.signal)
  },
  publishBinary(
    key: string,
    data: Uint8Array,
    options?: BroadcastPublishOptions,
  ): ChannelPublishAck | Promise<ChannelPublishAck> {
    assertBroadcastKey(key)
    return publishOnPlane({ key, kind: 'binary' }, binaryPayload(data, options?.lossy))
  },
  /** As `subscribe()`, for binary publishes. */
  subscribeBinary(key: string, callback: BroadcastBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe {
    if (options?.signal?.aborted) return withReady(() => {})
    // A copy of its own, as a `BroadcastChannel` listener gets.
    const decode = (payload: Uint8Array) => new Uint8Array(binaryData(payload))
    return subscribeRoute({ key, kind: 'binary' }, decode, callback, options?.signal)
  },
}

/** A static subscriber, removed once its backend ends its subscription for good, once `signal` aborts (at once if it
 *  aborted while subscribing), or when the returned function runs. Its `ready` resolves once the subscription is in
 *  effect or removed, and rejects where its backend failed it first. */
function subscribeRoute<Kind extends BroadcastKind, Data>(
  route: BroadcastRoute<Kind>,
  decode: (payload: BroadcastPayload<Kind>) => Data,
  callback: (data: Data, info: ChannelPublishInfo) => unknown,
  signal: AbortSignal | undefined,
): Unsubscribe {
  assertBroadcastKey(route.key)
  const readiness = new Readiness()
  const subscription = new RouteSubscription({
    route,
    receiver: (payload, info) => {
      callStaticListener(callback, decode(payload), { key: route.key, seq: info.seq, timestamp: info.timestamp })
    },
    onReady: () => readiness.settle(true),
    onFailed: (err) => {
      signal?.removeEventListener('abort', remove)
      readiness.settle(err)
    },
    onEnded: () => signal?.removeEventListener('abort', remove),
  })
  const remove = () => {
    signal?.removeEventListener('abort', remove)
    readiness.settle(true)
    subscription.close()
  }
  subscription.open()
  signal?.addEventListener('abort', remove, { once: true })
  if (signal?.aborted) remove()
  return withReady(remove, readiness)
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

function settleAll(waiting: Set<Readiness>, outcome: true | Error): void {
  for (const readiness of waiting) readiness.settle(outcome)
  waiting.clear()
}

type RouteReceiver<Kind extends BroadcastKind> = (payload: BroadcastPayload<Kind>, ordering: OrderingInfo) => void

type RouteSubscriptionOptions<Kind extends BroadcastKind> = {
  route: BroadcastRoute<Kind>
  receiver: RouteReceiver<Kind>
  /** A subscription it opened takes effect: its backend delivers to it what any server publishes to the route. */
  onReady: () => void
  /** A subscription it opened ended for good with `err` before it took effect: it is closed. */
  onFailed: (err: Error) => void
  /** A subscription it opened that took effect was ended for good with `err`: it is closed. */
  onEnded: (err: Error) => void
}

/** A route's subscription while wanted, until its backend ends it for good; a transport that replaces the plane gets
 *  it. */
class RouteSubscription<Kind extends BroadcastKind> {
  private readonly route: BroadcastRoute<Kind>
  private readonly receiver: RouteReceiver<Kind>
  private readonly onReady: () => void
  private readonly onFailed: (err: Error) => void
  private readonly onEnded: (err: Error) => void
  private opened = false
  /** Inside its backend's subscribe, which may deliver a message whose listener opens or closes it again. */
  private starting = false
  private current: BackendSubscription | null = null
  /** The start of `current`, until it took effect; null once it did. */
  private start: Readiness | null = null

  constructor({ route, receiver, onReady, onFailed, onEnded }: RouteSubscriptionOptions<Kind>) {
    this.route = route
    this.receiver = receiver
    this.onReady = onReady
    this.onFailed = onFailed
    this.onEnded = onEnded
  }

  /** Whether a subscription it opened is in effect. */
  get inEffect(): boolean {
    return this.current !== null && this.start === null
  }

  /** Settles as the start of the subscription it opened last does; `undefined` once that took effect. */
  get ready(): Promise<void> | undefined {
    return this.start?.promise
  }

  /** Subscribes unless a subscription is live; throws outside a Cloudflare session. */
  open(): void {
    this.opened = true
    if (this.current === null && !this.starting) this.subscribe()
    followBroadcastPlane(this)
  }

  close(): void {
    this.opened = false
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

  /** On the plane in effect now; the subscription keeps its own handle. */
  private startSubscription(): BackendSubscription {
    this.starting = true
    try {
      return getBroadcastPlane().subscribe(this.route, this.receiver)
    } finally {
      this.starting = false
    }
  }

  private subscribe(): void {
    const subscription = this.startSubscription()
    // A listener of a message its subscribe delivered closed it.
    if (!this.opened) return void subscription.unsubscribe()
    this.current = subscription
    const start = new Readiness()
    this.start = start
    let wasReady = false
    const tookEffect = () => {
      if (wasReady || this.current !== subscription) return
      wasReady = true
      this.start = null
      start.settle(true)
      this.onReady()
    }
    // Only a failure rejects `ready`, after the shared subscriptions retired and reported the subscription; an
    // unsubscribe resolves it. A backend renews a subscription it lost, so one it closed is over.
    const ended = async () => {
      try {
        await subscription.ready
      } catch (err) {
        if (this.current !== subscription) return
        this.close()
        const failure = err instanceof Error ? err : new Error(String(err))
        if (wasReady) return this.onEnded(failure)
        start.settle(failure)
        this.onFailed(failure)
      }
    }
    if (subscription.state() === 'closed') return void ended()
    if (subscription.state() === 'ready') tookEffect()
    subscription.onStateChange((state) => {
      if (state === 'ready') tookEffect()
      // Its own close unsubscribed it first: no end.
      else if (state === 'closed' && this.current === subscription) void ended()
    })
  }
}

function assertBroadcastKey(key: unknown): void {
  assertUsage(typeof key === 'string' && key.isWellFormed(), 'The broadcast key should be a well-formed string')
}
