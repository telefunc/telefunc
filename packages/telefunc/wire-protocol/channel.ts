export type {
  Channel,
  ChannelClient,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelAck,
  ChannelPublishAck,
  ChannelPublishMeta,
  ChannelListenReturn,
  ChannelListener,
}

type ChannelData<T> = [T] extends [never] ? never : T extends (data: infer D) => any ? D : T
type ChannelAck<T> = [T] extends [never] ? never : T extends (data: any) => infer R ? Awaited<R> : unknown
type ChannelPublishMeta = Record<string, unknown>
type ChannelPublishAck = {
  key: string
  seq: number
  ts: number
  meta?: ChannelPublishMeta
}
type ChannelListenReturn<T> = [T] extends [never]
  ? void
  : T extends (data: any) => infer R
    ? Awaited<R> | Promise<Awaited<R>>
    : unknown | Promise<unknown> | void
type ChannelListener<T> = (data: ChannelData<T>) => ChannelListenReturn<T>
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
      subscribe(callback: ChannelListener<TIn>): () => void
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
