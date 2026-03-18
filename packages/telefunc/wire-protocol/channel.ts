export type {
  Channel,
  ChannelClient,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelAck,
  ChannelListenReturn,
  ChannelListener,
}

type ChannelData<T> = [T] extends [never] ? never : T extends (data: infer D) => any ? D : T
type ChannelAck<T> = [T] extends [never] ? never : T extends (data: any) => infer R ? Awaited<R> : unknown
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

/**
 * Internal base — `TOut` is what this side sends, `TIn` is what this side receives.
 * Each can be a raw message type or a handler signature `(msg) => ack`.
 * Not exported — use `Channel` or `ChannelClient`.
 */
interface ChannelBase<TOut = unknown, TIn = unknown, TDefault extends boolean = false> {
  readonly id: string
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
}

/** Server-side channel. `ServerToClient` = messages the server sends; `ClientToServer` = messages the server receives. */
interface Channel<ServerToClient = unknown, ClientToServer = unknown, TDefault extends boolean = false>
  extends ChannelBase<ServerToClient, ClientToServer, TDefault> {
  /** The client-side end of the channel — return this from a telefunction. */
  readonly client: ChannelClient<ClientToServer, ServerToClient, TDefault>
  abort(abortValue?: unknown): void
}

/** Client-side channel. `ClientToServer` = messages the client sends; `ServerToClient` = messages the client receives. */
interface ChannelClient<ClientToServer = unknown, ServerToClient = unknown, TDefault extends boolean = false>
  extends ChannelBase<ClientToServer, ServerToClient, TDefault> {
  abort(): void
}
