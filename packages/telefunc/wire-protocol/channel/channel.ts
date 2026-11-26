export {
  makePublishInfo,
  stringifyValue,
  serializeAbortReason,
  serializeBoundedAbortReason,
  unsubscribeOf,
  neverOpened,
  Outcome,
  LOSSY_WITH_ACK_TEXT,
}
export type {
  AbortReason,
  AbortReasonLimit,
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
  ChannelPublishMeta,
  ChannelListenReturn,
  ChannelListener,
  ChannelBinaryListener,
  BroadcastListener,
  BroadcastBinaryListener,
  Unsubscribe,
}

import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shieldKey.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertUsage } from '../../utils/assert.js'
import { ChannelOverflowError } from './channelErrors.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'

type ChannelData<T> = [T] extends [never] ? never : T extends (data: infer D) => any ? D : T
type ChannelAck<T> = [T] extends [never] ? never : T extends (data: any) => infer R ? Awaited<R> : unknown
type ChannelPublishMeta = Record<string, unknown>
/** Metadata delivered to broadcast subscribers alongside each message. */
type ChannelPublishInfo = {
  key: string
  /** Per-key counter: 1 more per publish while the key has a subscriber. A jump means missed messages. */
  seq: number
  /** Server timestamp, Unix epoch milliseconds. */
  timestamp: number
}
type ChannelPublishAck = ChannelPublishInfo & { meta?: ChannelPublishMeta }

function makePublishInfo(key: string, seq: number, timestamp: number): ChannelPublishInfo {
  return { key, seq, timestamp }
}

/** What `send(data, { lossy: true, ack: true })` throws, on either end. */
const LOSSY_WITH_ACK_TEXT =
  "`lossy` can't be combined with `ack: true`: a newer send may take this one's place, so it gets no answer. Send it without `lossy` to get its answer."

/** What `stringifyValue()` serializes: `send()` and `publish()`'s data, a listener's answer, an abort reason. */
type ValueBy = 'send' | 'publish' | 'answer' | 'abort'
const REFUSED: Record<ValueBy, (which: string) => string> = {
  send: (which) =>
    `\`send()\` serializes its data as JSON, which turns ${which} into a plain object. Bytes go through \`sendBinary()\`, which takes a \`Uint8Array\`.`,
  publish: (which) =>
    `\`publish()\` serializes its data as JSON, which turns ${which} into a plain object. Bytes go through \`publishBinary()\`, which takes a \`Uint8Array\`.`,
  answer: (which) =>
    `A listener's answer is serialized as JSON, which turns ${which} into a plain object. Answer with a value JSON holds, or send the bytes with \`sendBinary()\` and answer with a reference to them.`,
  abort: (which) =>
    `An abort reason is serialized as JSON, which turns ${which} into a plain object. Abort with a plain value, such as a string or a code.`,
}

/** `value` as JSON, for what an app sends as text, on either end. Bytes are refused at any depth: JSON turns them into
 *  an object. The serializer's own pass over `value` checks each value, so one without bytes costs one call per value
 *  and a prototype read per object. */
function stringifyValue(value: unknown, by: ValueBy): string {
  return stringify(value, {
    replacer(key, slot) {
      if (typeof slot !== 'object' || slot === null) return undefined
      if (!ArrayBuffer.isView(slot) && !isBytes(slot)) return undefined
      let name = toTag.call(slot).slice('[object '.length, -1)
      let at: string | null = null
      // The refusal is what matters: naming the bytes, or where they are, may fail on an app's object.
      try {
        name = slot.constructor.name
        if (key !== '') at = pathOf(value, this, key)
      } catch {}
      const which =
        key === '' ? `this \`${name}\`` : at === null ? `a \`${name}\` in it` : `the \`${name}\` at \`${at}\``
      assertUsage(false, REFUSED[by](which))
    },
  })
}

/** An abort reason as JSON, and the value the abort ends with. A reason JSON can't hold gives an abort without one, and
 *  its `refusal`, which the end throws or reports once it ended: an abort always ends the channel. */
function serializeAbortReason(reason: unknown): AbortReason {
  try {
    return { value: reason, serialized: stringifyValue(reason, 'abort'), refusal: null }
  } catch (error) {
    return { value: undefined, serialized: stringifyValue(undefined, 'abort'), refusal: { error } }
  }
}
type AbortReason = { value: unknown; serialized: string; refusal: { error: unknown } | null }

/** The most an end takes of an abort reason: `max` of what `bytesOf` counts for it, and what that limit is. */
type AbortReasonLimit = { max: number; bytesOf: (serialized: string) => number; what: string }

/** As `serializeAbortReason`, and a reason over `limit` goes without its value too: the abort ends the channel on both
 *  ends without it, and its refusal is a `ChannelOverflowError`. */
function serializeBoundedAbortReason(value: unknown, limit: AbortReasonLimit): AbortReason {
  const reason = serializeAbortReason(value)
  const bytes = limit.bytesOf(reason.serialized)
  if (reason.refusal || bytes <= limit.max) return reason
  const error = new ChannelOverflowError(
    `abort(): the value is ${bytes} bytes, more than ${limit.what} (${limit.max} bytes), so the channel ended without it`,
  )
  return { value: undefined, serialized: serializeAbortReason(undefined).serialized, refusal: { error } }
}

const toTag = Object.prototype.toString
const BYTES_TAGS = new Set(['[object ArrayBuffer]', '[object SharedArrayBuffer]', '[object Blob]', '[object File]'])
/** Whether `value` is an `ArrayBuffer`, a `SharedArrayBuffer`, a `Blob` or a `File`, of any realm, by its tag. A plain
 *  object or an array of this realm is none, which one prototype read tells. */
function isBytes(value: object): boolean {
  const proto = Object.getPrototypeOf(value)
  if (proto === Object.prototype || proto === Array.prototype) return false
  return BYTES_TAGS.has(toTag.call(value))
}

/** Where `holder[key]` is in `value`, as `a.b[0]`; searched only for a value refused. It reads data properties only, so
 *  no getter or `toJSON` runs, and is null where that doesn't reach `holder`: behind a getter, or inside a `Map` or a
 *  `Set`, which the serializer copies. */
function pathOf(value: unknown, holder: object, key: string): string | null {
  const seen = new Set<object>()
  const search = (at: unknown, path: (string | number)[]): (string | number)[] | null => {
    if (typeof at !== 'object' || at === null || seen.has(at)) return null
    seen.add(at)
    const isArray = Array.isArray(at)
    if (at === holder) return [...path, isArray ? Number(key) : key]
    for (const k of Object.keys(at)) {
      const field = Object.getOwnPropertyDescriptor(at, k)
      if (!field || !('value' in field)) continue
      const found = search(field.value, [...path, isArray ? Number(k) : k])
      if (found) return found
    }
    return null
  }
  const path = search(value, [])
  if (!path) return null
  return path.map((step, i) => (typeof step === 'number' ? `[${step}]` : i === 0 ? step : `.${step}`)).join('')
}

type ChannelListenReturn<T> = [T] extends [never]
  ? void
  : T extends (data: any) => infer R
    ? Awaited<R> | void | Promise<Awaited<R> | void>
    : unknown | Promise<unknown> | void
type ChannelListener<T> = (data: ChannelData<T>) => ChannelListenReturn<T>
type ChannelBinaryListener = (data: Uint8Array) => unknown | Promise<unknown>
/** Callback for `Broadcast.subscribe()` — receives message data and publish info. */
type BroadcastListener<T> = (data: ChannelData<T>, info: ChannelPublishInfo) => ChannelListenReturn<T>
/** Callback for `Broadcast.subscribeBinary()` — receives raw binary data and publish info. */
type BroadcastBinaryListener = (data: Uint8Array, info: ChannelPublishInfo) => unknown | Promise<unknown>
/** Removes the listener, as aborting its `signal` does. `opened` resolves once the listener is in effect, at once for
 *  `listen()`, and rejects where it never will be, as where its channel closes first; each case is on the page.
 *
 *  https://telefunc.com/channel#broadcastchannel-methods */
type Unsubscribe = (() => void) & { readonly opened: Promise<void> }
type ChannelCloseOptions = {
  timeout?: number
}
/** How the channel ended, as `close()` and `closed` give it: `null` where it closed gracefully on both ends, else the
 *  error it ended with.
 *
 *  https://telefunc.com/channel#errors */
type ChannelCloseResult = Error | null
/** Cleanup, called once with what is known of the end when it is called: `null` where the channel is closing
 *  gracefully, as a close's cleanup starts before its outcome is known, else the error it ended with. `closed` and
 *  `close()` give the final outcome. */
type ChannelCloseCallback = (err: Error | null) => void | Promise<void>

/** The channel surface both ends share: `TOut` is what this end sends, `TIn` what it receives, each a message type or a
 *  `(message) => answer` signature. */
type ChannelBase<TOut = unknown, TIn = unknown, TDefault extends boolean = false> = {
  readonly id: string
  readonly isClosed: boolean
  readonly opened: Promise<void>
  /** Resolves once the channel has ended, never rejects: with `null` where it closed gracefully on both ends, else the
   *  error it ended with. */
  readonly closed: Promise<ChannelCloseResult>
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
   *  https://telefunc.com/channel#channel-methods */
  send(data: ChannelData<TOut>, opts: { lossy: true; ack?: false }): Promise<void>
  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  /** Send binary where only the newest matters: a receiver that keeps up gets every message, and one that falls behind
   *  skips to the newest. It resolves at once, and can't be combined with `ack: true`.
   *
   *  https://telefunc.com/channel#channel-methods */
  sendBinary(data: Uint8Array, opts: { lossy: true; ack?: false }): Promise<void>
  /** Receive messages. Return a value to ack the sender. Returns an unlisten function, whose `opened` resolves at once:
   *  the listener is in effect once `listen()` returns. On a channel that ended, it registers nothing, and `opened`
   *  rejects with the channel's end. */
  listen(callback: ChannelListener<TIn>, options?: { signal?: AbortSignal }): Unsubscribe
  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe
  /** Called once as the channel closes, with `null` for a graceful close (its outcome is `closed`'s) and else the error
   *  it ends with; a graceful close waits, within its timeout, for the promise it returns. Returns a function that
   *  removes the callback. */
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void
  /** Called when the channel opens. Returns a function that removes the callback. */
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void
  /** Closes gracefully, within `timeout` (5 s by default), and resolves with how the channel ended, as `closed` does. */
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
 *  error it never will be with. Its promise is made only where its `opened` is read. */
class Outcome {
  private outcome: true | Error | undefined = undefined
  private waiting: Deferred<void> | undefined = undefined
  /** Settles as `until` does: followed only once `opened` is read. */
  private readonly until: Outcome | undefined

  constructor(until?: Outcome) {
    this.until = until
  }

  /** One that failed with `err`. */
  static failed(err: Error): Outcome {
    const outcome = new Outcome()
    outcome.settle(err)
    return outcome
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
    if (this.outcome === true) return OPENED
    if (this.waiting === undefined) {
      this.waiting = createDeferred()
      // A failure nobody reads raises no unhandled rejection.
      this.waiting.promise.catch(() => {})
      if (this.outcome !== undefined) this.waiting.reject(this.outcome)
      else if (this.until) void this.follow(this.until.promise)
    }
    return this.waiting.promise
  }

  /** Settles as `promise` does: where a backend's `opened` is observed once, or a listener's once it is read. */
  async follow(promise: Promise<void>): Promise<void> {
    try {
      await promise
    } catch (err) {
      return this.settle(err instanceof Error ? err : new Error(String(err)))
    }
    this.settle(true)
  }
}

/** What the `opened` of every listener in effect at once is. */
const OPENED = Promise.resolve()

/** `remove` with `opened`, read through a getter: in effect at once without `outcome`. */
function unsubscribeOf(remove: () => void, outcome?: Outcome): Unsubscribe {
  const opened = outcome ? () => outcome.promise : () => OPENED
  return Object.defineProperty(Object.assign(remove, { opened: OPENED }), 'opened', { get: opened })
}

/** The remover of a listener added once its channel ended, which registered nothing: its `opened` rejects with `err`. */
function neverOpened(err: Error): Unsubscribe {
  return unsubscribeOf(() => {}, Outcome.failed(err))
}
