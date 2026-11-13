export { makePublishInfo, stringifyMessage, LOSSY_WITH_ACK_TEXT }
export type {
  ChannelBase,
  ChannelShield,
  ClientChannel,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelJson,
  ChannelAck,
  ChannelPublishAck,
  ChannelPublishInfo,
  ChannelPublishMeta,
  ChannelListenReturn,
  ChannelListener,
  ChannelBinaryListener,
  BroadcastListener,
  BroadcastBinaryListener,
}

import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shieldKey.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertUsage } from '../../utils/assert.js'

type ChannelData<T> = [T] extends [never] ? never : T extends (data: infer D) => any ? D : T
/** What `send()` and `publish()` take: the channel's data, on an untyped channel the argument's type; never bytes. */
type ChannelJson<T, D, Method extends 'send' | 'publish'> = unknown extends ChannelData<T>
  ? RefuseBytes<D, Method>
  : RefuseBytes<ChannelData<T>, Method>
type RefuseBytes<D, Method extends 'send' | 'publish'> = D extends ArrayBuffer | ArrayBufferView
  ? `${Method}() serializes its data as JSON: bytes go through ${Method}Binary()`
  : D
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
  readonly opened: Promise<void>
  readonly closed: Promise<void>
  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<
    { type: 'text'; data: ChannelData<TIn> } | { type: 'binary'; data: Uint8Array }
  >
  /** Default send. Returns `Promise<ack>` when `TDefault = true`, otherwise `Promise<void>`. */
  send<D>(data: ChannelJson<TOut, D, 'send'>): TDefault extends true ? Promise<ChannelAck<TOut>> : Promise<void>
  /** Per-send ack opt-in — always returns `Promise<ack>`. */
  send<D>(data: ChannelJson<TOut, D, 'send'>, opts: { ack: true }): Promise<ChannelAck<TOut>>
  /** Per-send ack opt-out — always returns `Promise<void>`. */
  send<D>(data: ChannelJson<TOut, D, 'send'>, opts: { ack: false }): Promise<void>
  /** Send where only the newest matters: a receiver that keeps up gets every message, and one that falls behind skips to
   *  the newest. It resolves at once, and can't be combined with `ack: true`.
   *
   *  https://telefunc.com/channel#skip-to-the-newest */
  send<D>(data: ChannelJson<TOut, D, 'send'>, opts: { lossy: true; ack?: false }): Promise<void>
  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  /** Send binary where only the newest matters: a receiver that keeps up gets every message, and one that falls behind
   *  skips to the newest. It resolves at once, and can't be combined with `ack: true`.
   *
   *  https://telefunc.com/channel#skip-to-the-newest */
  sendBinary(data: Uint8Array, opts: { lossy: true; ack?: false }): Promise<void>
  /** Receive messages. Return a value to ack the sender. Returns an unlisten function. */
  listen(callback: ChannelListener<TIn>, options?: { signal?: AbortSignal }): () => void
  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): () => void
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): void
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): void
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
