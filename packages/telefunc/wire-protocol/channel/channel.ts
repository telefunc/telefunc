export { invokeChannelListener, stringifyMessage, withReady, neverInEffect, Readiness, LOSSY_WITH_ACK_TEXT }
export type {
  ChannelBase,
  ChannelShield,
  ClientChannel,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelAck,
  ChannelPublishAck,
  ChannelPublishInfo,
  BroadcastPublishOptions,
  ChannelListenReturn,
  ChannelListener,
  ChannelBinaryListener,
  BroadcastListener,
  BroadcastBinaryListener,
  BroadcastListenerByKind,
  BroadcastListeners,
  Unsubscribe,
}

import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shieldKey.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import type { Listeners } from '../../utils/Listeners.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'

type ChannelData<T> = [T] extends [never] ? never : T extends (data: infer D) => any ? D : T
type ChannelAck<T> = [T] extends [never] ? never : T extends (data: any) => infer R ? Awaited<R> : unknown
/** Metadata delivered to broadcast subscribers alongside each message. */
type ChannelPublishInfo = {
  key: string
  /** Per-key counter: 1 more per publish while the key has a subscriber. A jump means missed messages. It starts at 1 (in
   *  memory, again after a restart); on Redis a key starts at the Redis clock in microseconds, so a lost counter never
   *  goes backwards. */
  seq: number
  /** Server timestamp, Unix epoch milliseconds. */
  timestamp: number
}
type ChannelPublishAck = ChannelPublishInfo

/** The options of a broadcast's `publish()` and `publishBinary()`. */
type BroadcastPublishOptions = {
  /**
   * Where only the newest message matters: a client that keeps up gets every message, and one that falls behind skips
   * to the newest, with `info.seq` jumping over what it skipped. A message without it reaches every client, after the
   * lossy ones published on the key before it.
   *
   * https://telefunc.com/channel#lossy-broadcast
   *
   * @default false
   */
  lossy?: boolean
}

/** Invoke a fire-and-forget listener while routing both thrown and rejected failures. Returns what `handleError`
 *  returned for a synchronous throw only, which a caller can use to stop iterating; a rejection can't stop it. */
function invokeChannelListener<Args extends unknown[]>(
  listener: (...args: Args) => unknown,
  args: Args,
  handleError: (error: unknown) => boolean | void,
): boolean | void {
  try {
    const result = listener(...args)
    if (isPromise(result)) void result.catch(handleError)
  } catch (error) {
    return handleError(error)
  }
}

/** What `send(data, { lossy: true, ack: true })` throws, on either end. */
const LOSSY_WITH_ACK_TEXT =
  "`lossy` can't be combined with `ack: true`: a newer send may take this one's place, so it gets no answer. Send it without `lossy` to get its answer."

/** `data` as JSON, for `send()` and `publish()` on either end. Bytes are refused: JSON turns them into an object. */
function stringifyMessage(data: unknown, method: 'send' | 'publish'): string {
  if (ArrayBuffer.isView(data) || data instanceof ArrayBuffer) {
    assertUsage(
      false,
      `\`${method}()\` serializes its data as JSON, which turns this \`${data.constructor.name}\` into a plain object. Bytes go through \`${method}Binary()\`, which takes a \`Uint8Array\`.`,
    )
  }
  return stringify(data)
}

type ChannelListenReturn<T> = [T] extends [never]
  ? void
  : T extends (data: any) => infer R
    ? Awaited<R> | Promise<Awaited<R>> | void | Promise<void>
    : unknown | Promise<unknown> | void
type ChannelListener<T> = (data: ChannelData<T>) => ChannelListenReturn<T>
type ChannelBinaryListener = (data: Uint8Array) => unknown | Promise<unknown>
/** Callback for `Broadcast.subscribe()` — receives message data and publish info. */
type BroadcastListener<T> = (data: ChannelData<T>, info: ChannelPublishInfo) => ChannelListenReturn<T>
/** Callback for `Broadcast.subscribeBinary()` — receives raw binary data and publish info. */
type BroadcastBinaryListener = (data: Uint8Array, info: ChannelPublishInfo) => unknown | Promise<unknown>
type BroadcastListenerByKind<T> = { text: BroadcastListener<T>; binary: BroadcastBinaryListener }
/** A broadcast's local listeners, by kind. */
type BroadcastListeners<T> = { [K in keyof BroadcastListenerByKind<T>]: Listeners<BroadcastListenerByKind<T>[K]> }
/** Removes the listener. `ready` resolves once the listener is in effect, at once where nothing has to reach the other
 *  side first, or once the listener is removed; it rejects where it never will be in effect: its channel ended first,
 *  or before the listener was added, or, without a channel (`Broadcast.subscribe()`), the broadcast backend gave the
 *  subscription up. So on the page and on the server alike. */
type Unsubscribe = (() => void) & { readonly ready: Promise<void> }
type ChannelCloseOptions = {
  timeout?: number
}
type ChannelCloseResult = 0 | 1
type ChannelCloseCallback = (err?: Error) => void | Promise<void>

/**
 * Internal base — `TOut` is what this side sends, `TIn` is what this side receives.
 * Each can be a raw message type or a handler signature `(msg) => ack`.
 * Shared base for `Channel` and `ClientChannel`.
 */
type ChannelBase<TOut = unknown, TIn = unknown, TDefault extends boolean = false> = {
  readonly id: string
  readonly isClosed: boolean
  /** True from open until close, except while the other side can't be reached. */
  readonly isConnected: boolean
  readonly opened: Promise<void>
  readonly closed: Promise<void>
  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<
    { type: 'text'; data: ChannelData<TIn> } | { type: 'binary'; data: Uint8Array }
  >
  /** Default send. Returns `Promise<ack>` when `TDefault = true`, otherwise `Promise<void>`.
   *  Serializes `data` as JSON: bytes go through `sendBinary()`. */
  send(data: ChannelData<TOut>): TDefault extends true ? Promise<ChannelAck<TOut>> : Promise<void>
  /** Per-send ack opt-in — always returns `Promise<ack>`. */
  send(data: ChannelData<TOut>, opts: { ack: true }): Promise<ChannelAck<TOut>>
  /** Per-send ack opt-out — always returns `Promise<void>`. */
  send(data: ChannelData<TOut>, opts: { ack: false }): Promise<void>
  /** Send where only the newest matters: a receiver that keeps up gets every message, and one that falls behind skips to
   *  the newest. It resolves at once, and can't be combined with `ack: true`.
   *
   *  https://telefunc.com/channel#skip-to-the-newest */
  send(data: ChannelData<TOut>, opts: { lossy: true; ack?: false }): Promise<void>
  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  /** Send binary where only the newest matters: a receiver that keeps up gets every message, and one that falls behind
   *  skips to the newest. It resolves at once, and can't be combined with `ack: true`.
   *
   *  https://telefunc.com/channel#skip-to-the-newest */
  sendBinary(data: Uint8Array, opts: { lossy: true; ack?: false }): Promise<void>
  /** Receive messages. Return a value to ack the sender. Returns an unlisten function, whose `ready` resolves at once:
   *  the listener is in effect once `listen()` returns. On a channel that ended, it registers nothing, and `ready`
   *  rejects with the channel's end. */
  listen(callback: ChannelListener<TIn>, options?: { signal?: AbortSignal }): Unsubscribe
  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe
  /** Called when the channel closes. Returns a function that removes the callback. */
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void
  /** Called when the channel opens. Returns a function that removes the callback. */
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void
  /** The other side became unreachable: the channel stays open, and what it sends waits. */
  onDisconnect(callback: () => void, options?: { signal?: AbortSignal }): void
  /** The other side is back, and what waited is sent. */
  onReconnect(callback: () => void, options?: { signal?: AbortSignal }): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

/** Shield contract — read by the server to validate messages. `data` is incoming
 *  client→server data; `ack` is the ack the client returns from its `listen`
 *  handler in response to a server `send`. The same slot is used on both `Channel`
 *  (server-side instance) and `ClientChannel` (returned from a telefunction). */
type ChannelShield<ClientToServer, ServerToClient> = {
  readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<ClientToServer>
    ack: ChannelAck<ServerToClient>
  }
}

/** Client-side channel. `ClientToServer` = messages the client sends; `ServerToClient` = messages the client receives. */
type ClientChannel<ClientToServer = unknown, ServerToClient = unknown, TDefault extends boolean = false> = ChannelBase<
  ClientToServer,
  ServerToClient,
  TDefault
> &
  ChannelShield<ClientToServer, ServerToClient>

/** A listener's way to being in effect, settled at its first outcome: `true` once it is in effect or removed, or the
 *  error it never will be with. Its promise is made only where its `ready` is read. */
class Readiness {
  private outcome: true | Error | undefined = undefined
  private waiting: Deferred<void> | undefined = undefined
  /** Settles as `until` does: followed only once `ready` is read. */
  private readonly until: Readiness | undefined

  constructor(until?: Readiness) {
    this.until = until
  }

  /** One that settles as `promise` does, followed now: a backend's `ready`, observed once for every subscriber. */
  static following(promise: Promise<void>): Readiness {
    const readiness = new Readiness()
    void readiness.follow(promise)
    return readiness
  }

  /** The error it settled with, where it did. */
  get failure(): Error | undefined {
    return this.outcome === true ? undefined : this.outcome
  }

  settle(outcome: true | Error): void {
    if (this.outcome !== undefined) return
    this.outcome = outcome
    if (outcome === true) this.waiting?.resolve()
    else this.waiting?.reject(outcome)
  }

  get promise(): Promise<void> {
    if (this.outcome === true) return IN_EFFECT
    if (this.waiting === undefined) {
      this.waiting = createDeferred()
      // A failure nobody reads raises no unhandled rejection.
      this.waiting.promise.catch(() => {})
      if (this.outcome !== undefined) this.waiting.reject(this.outcome)
      else if (this.until) void this.follow(this.until.promise)
    }
    return this.waiting.promise
  }

  /** Settles as `promise` does: where a backend's `ready` is observed once, or a listener's once it is read. */
  async follow(promise: Promise<void>): Promise<void> {
    try {
      await promise
    } catch (err) {
      return this.settle(err instanceof Error ? err : new Error(String(err)))
    }
    this.settle(true)
  }
}

/** What the `ready` of every listener in effect at once is. */
const IN_EFFECT = Promise.resolve()

/** `remove` with `ready`: in effect at once without `readiness`. */
function withReady(remove: () => void, readiness?: Readiness): Unsubscribe {
  const unsubscribe = Object.assign(remove, { ready: IN_EFFECT })
  if (readiness) Object.defineProperty(unsubscribe, 'ready', { get: () => readiness.promise })
  return unsubscribe
}

/** The remover of a listener added once its channel ended, which registered nothing: its `ready` rejects with `err`. */
function neverInEffect(err: Error): Unsubscribe {
  const readiness = new Readiness()
  readiness.settle(err)
  return withReady(() => {}, readiness)
}
