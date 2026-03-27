export { makePublishInfo }
export type {
  Channel,
  ChannelClient,
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
  PubSubListener,
  PubSubBinaryListener,
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
/** Callback for `channel.subscribe()` — receives message data and publish info. */
type PubSubListener<T> = (data: ChannelData<T>, info: ChannelPublishInfo) => ChannelListenReturn<T>
/** Callback for `channel.subscribeBinary()` — receives raw binary data and publish info. */
type PubSubBinaryListener = (data: Uint8Array, info: ChannelPublishInfo) => void | Promise<void>
type ChannelCloseOptions = {
  timeout?: number
}
type ChannelCloseResult = 0 | 1
type ChannelCloseCallback = (err?: Error) => void | Promise<void>
type KeyedChannelMethods<TOut, TIn, TKeyed extends boolean> = TKeyed extends true
  ? {
      /** Publish to other members sharing the same key and await the authority receipt. */
      publish(data: ChannelData<TOut>): Promise<ChannelPublishAck>
      /** Subscribe to broadcasts for the same key. Only valid when the channel was created with a key. */
      subscribe(callback: PubSubListener<TIn>): () => void
      /** Publish raw binary data to other members sharing the same key and await the authority receipt. */
      publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
      /** Subscribe to binary broadcasts for the same key. Only valid when the channel was created with a key. */
      subscribeBinary(callback: PubSubBinaryListener): () => void
    }
  : {}

/**
 * Internal base — `TOut` is what this side sends, `TIn` is what this side receives.
 * Each can be a raw message type or a handler signature `(msg) => ack`.
 * Not exported — use `Channel` or `ChannelClient`.
 */
type ChannelBase<TOut = unknown, TIn = unknown, TDefault extends boolean = false, TKeyed extends boolean = false> = {
  readonly id: string
  readonly key?: string
  readonly isClosed: boolean
  /** Default send. Returns `Promise<ack>` when `TDefault = true`, otherwise `void`. */
  send(data: ChannelData<TOut>): TDefault extends true ? Promise<ChannelAck<TOut>> : void
  /** Per-send ack opt-in — always returns `Promise<ack>`. */
  send(data: ChannelData<TOut>, opts: { ack: true }): Promise<ChannelAck<TOut>>
  /** Per-send ack opt-out — always returns `void`. */
  send(data: ChannelData<TOut>, opts: { ack: false }): void
  sendBinary(data: Uint8Array): void
  /** Receive messages. Return a value to ack the sender. */
  listen(callback: ChannelListener<TIn>): void
  listenBinary(callback: (data: Uint8Array) => void): void
  onClose(callback: ChannelCloseCallback): void
  onOpen(callback: () => void): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
} & KeyedChannelMethods<TOut, TIn, TKeyed>

/** Server-side channel. `ServerToClient` = messages the server sends; `ClientToServer` = messages the server receives. */
type Channel<
  ServerToClient = unknown,
  ClientToServer = unknown,
  TDefault extends boolean = false,
  TKeyed extends boolean = false,
> = ChannelBase<ServerToClient, ClientToServer, TDefault, TKeyed> & {
  /** The client-side end of the channel — return this from a telefunction. */
  readonly client: ChannelClient<ClientToServer, ServerToClient, TDefault, TKeyed>
  abort(abortValue?: unknown): void
}

/** Client-side channel. `ClientToServer` = messages the client sends; `ServerToClient` = messages the client receives. */
type ChannelClient<
  ClientToServer = unknown,
  ServerToClient = unknown,
  TDefault extends boolean = false,
  TKeyed extends boolean = false,
> = ChannelBase<ClientToServer, ServerToClient, TDefault, TKeyed> & {
  abort(): void
}
