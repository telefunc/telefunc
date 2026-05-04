export { makePublishInfo }
export type {
  ChannelBase,
  Channel,
  ClientChannel,
  Broadcast,
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
  readonly __DEFINE_TELEFUNC_SHIELDS: {
    /** Data that flows through `send` (TOut). When the server inspects the `ClientChannel` returned
     *  by a telefunction, TOut = client→server messages — the shield validates those on arrival. */
    data: ChannelData<TOut>
    /** Ack response for what this side listens to (TIn). For a `ClientChannel` returned to the client,
     *  the client's `listen` acks back to the server's send — the shield validates the arriving ack. */
    ack: ChannelAck<TIn>
  }
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

/** Server-side channel. `ServerToClient` = messages the server sends; `ClientToServer` = messages the server receives. */
type Channel<ServerToClient = unknown, ClientToServer = unknown, TDefault extends boolean = false> = ChannelBase<
  ServerToClient,
  ClientToServer,
  TDefault
> & {
  /** The client-side end of the channel — return this from a telefunction. */
  readonly client: ClientChannel<ClientToServer, ServerToClient, TDefault>
}

/** Client-side channel. `ClientToServer` = messages the client sends; `ServerToClient` = messages the client receives. */
type ClientChannel<ClientToServer = unknown, ServerToClient = unknown, TDefault extends boolean = false> = ChannelBase<
  ClientToServer,
  ServerToClient,
  TDefault
>

/** Pub/sub instance — publish and subscribe to a keyed topic. Returnable from telefunctions. */
type Broadcast<T = unknown> = {
  /** Shield contract for incoming client→server publishes; ack is unused (publish receipts are server-generated). */
  readonly __DEFINE_TELEFUNC_SHIELDS: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly id: string
  readonly key: string
  readonly isClosed: boolean
  publish(data: ChannelData<T>): Promise<ChannelPublishAck>
  subscribe(callback: BroadcastListener<T>): () => void
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  subscribeBinary(callback: BroadcastBinaryListener): () => void
  onClose(callback: ChannelCloseCallback): void
  onOpen(callback: () => void): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
}
