export { makePublishInfo }
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
  ChannelPublishMeta,
  ChannelListenReturn,
  ChannelListener,
  ChannelBinaryListener,
  BroadcastListener,
  BroadcastBinaryListener,
}

type ChannelData<T> = [T] extends [never] ? never : T extends (data: infer D) => any ? D : T
type ChannelAck<T> = [T] extends [never] ? never : T extends (data: any) => infer R ? Awaited<R> : unknown
type ChannelPublishMeta = Record<string, unknown>
/** Metadata delivered to pub/sub subscribers alongside each message. */
type ChannelPublishInfo = {
  key: string
  /** Strict per-key counter (1, 2, 3…). Resets if the authority restarts. Use for gap detection. */
  seq: number
  ts: number
}
type ChannelPublishAck = ChannelPublishInfo & { meta?: ChannelPublishMeta }

function makePublishInfo(key: string, seq: number, ts: number): ChannelPublishInfo {
  return { key, seq, ts }
}
type ChannelListenReturn<T> = [T] extends [never]
  ? void
  : T extends (data: any) => infer R
    ? Awaited<R> | Promise<Awaited<R>>
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
  /** Default send. Returns `Promise<ack>` when `TDefault = true`, otherwise `Promise<void>`. */
  send(data: ChannelData<TOut>): TDefault extends true ? Promise<ChannelAck<TOut>> : Promise<void>
  /** Per-send ack opt-in — always returns `Promise<ack>`. */
  send(data: ChannelData<TOut>, opts: { ack: true }): Promise<ChannelAck<TOut>>
  /** Per-send ack opt-out — always returns `Promise<void>`. */
  send(data: ChannelData<TOut>, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  /** Receive messages. Return a value to ack the sender. Returns an unlisten function. */
  listen(callback: ChannelListener<TIn>): () => void
  listenBinary(callback: ChannelBinaryListener): () => void
  onClose(callback: ChannelCloseCallback): void
  onOpen(callback: () => void): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

/** Shield contract — read by the server to validate messages. `data` is incoming
 *  client→server data; `ack` is the ack the client returns from its `listen`
 *  handler in response to a server `send`. The same slot is used on both `Channel`
 *  (server-side instance) and `ClientChannel` (returned from a telefunction). */
type ChannelShield<ClientToServer, ServerToClient> = {
  readonly __DEFINE_TELEFUNC_SHIELDS: {
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
