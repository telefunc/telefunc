export {
  makePublishInfo,
  stringifyValue,
  serializeAbortReason,
  unsubscribeOf,
  neverOpened,
  Outcome,
  LOSSY_WITH_ACK_TEXT,
}
export type {
  AbortReason,
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
 *  and a tag read per object. */
function stringifyValue(value: unknown, by: ValueBy): string {
  return stringify(value, {
    replacer(key, slot) {
      if (typeof slot !== 'object' || slot === null) return undefined
      if (!ArrayBuffer.isView(slot) && !isBuffer(slot)) return undefined
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

const toTag = Object.prototype.toString
/** A buffer's own `byteLength` getter, which throws for anything without the buffer's internal slot. A page that isn't
 *  cross-origin isolated has no `SharedArrayBuffer`, and no realm of it makes one. */
const readByteLength = {
  '[object ArrayBuffer]': Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength')?.get,
  '[object SharedArrayBuffer]':
    typeof SharedArrayBuffer === 'undefined'
      ? undefined
      : Object.getOwnPropertyDescriptor(SharedArrayBuffer.prototype, 'byteLength')?.get,
}

/** Whether `value` is an `ArrayBuffer` or a `SharedArrayBuffer`, of any realm. Its tag names a candidate, one call per
 *  object, and the buffer's getter confirms it: an object that only claims the tag isn't one. Confirming every object
 *  that way would cost a throw each. */
function isBuffer(value: object): value is ArrayBufferLike {
  const tag = toTag.call(value)
  const read = tag === '[object ArrayBuffer]' || tag === '[object SharedArrayBuffer]' ? readByteLength[tag] : undefined
  if (!read) return false
  try {
    read.call(value)
    return true
  } catch {
    return false
  }
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
 *  https://telefunc.com/channel#when-opened-settles */
type Unsubscribe = (() => void) & { readonly opened: Promise<void> }
type ChannelCloseOptions = {
  timeout?: number
}
/** `0`: this end received the close acknowledgement; `1`: it did not. `closed` gives the final outcome. */
type ChannelCloseResult = 0 | 1
type ChannelCloseCallback = (err?: Error) => void | Promise<void>

/** The channel surface both ends share: `TOut` is what this end sends, `TIn` what it receives, each a message type or a
 *  `(message) => answer` signature. */
type ChannelBase<TOut = unknown, TIn = unknown, TDefault extends boolean = false> = {
  readonly id: string
  readonly isClosed: boolean
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
  /** Receive messages. Return a value to ack the sender. Returns an unlisten function, whose `opened` resolves at once:
   *  the listener is in effect once `listen()` returns. On a channel that ended, it registers nothing, and `opened`
   *  rejects with the channel's end. */
  listen(callback: ChannelListener<TIn>, options?: { signal?: AbortSignal }): Unsubscribe
  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe
  /** Called when the channel closes. Returns a function that removes the callback. */
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void
  /** Called when the channel opens. Returns a function that removes the callback. */
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void
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
