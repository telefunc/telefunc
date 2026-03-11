export type { Channel, ChannelClient }

/**
 * Bidirectional channel for real-time communication.
 *
 * `Channel<TSend, TReceive>` means "I send TSend, I receive TReceive".
 * Both server and client implement this interface.
 *
 * Optional type parameters for acknowledgements:
 * - `TAckSend`   — type the sender receives back when the other side's listener resolves.
 * - `TAckReceive` — type the listener must resolve with when receiving from the other side.
 * - `TDefault`   — when `true`, every `send()` returns a `Promise<TAckSend>` automatically
 *                  (i.e. `createChannel({ ack: true })` mode). Default: `false`.
 *
 * The `.client` value has the send/receive and ack types flipped.
 */
interface ChannelBase<
  TSend = unknown,
  TReceive = unknown,
  TAckSend = unknown,
  TAckReceive = unknown,
  TDefault extends boolean = false,
> {
  readonly id: string
  readonly isClosed: boolean
  /** Default send. Returns `Promise<TAckSend>` when `TDefault = true`, otherwise `void`. */
  send(data: TSend): TDefault extends true ? Promise<TAckSend> : void
  /** Per-send ack opt-in — always returns `Promise<TAckSend>`. */
  send(data: TSend, opts: { ack: true }): Promise<TAckSend>
  /** Per-send ack opt-out — always returns `void`. */
  send(data: TSend, opts: { ack: false }): void
  sendBinary(data: Uint8Array): void
  /** Receive messages. Return a value to ack the sender. */
  listen(callback: (data: TReceive) => TAckReceive | Promise<TAckReceive> | void): void
  listenBinary(callback: (data: Uint8Array) => void): void
  onClose(callback: (err?: Error) => void): void
  onOpen(callback: () => void): void
  close(): void
}

interface Channel<
  TSend = unknown,
  TReceive = unknown,
  TAckSend = unknown,
  TAckReceive = unknown,
  TDefault extends boolean = false,
> extends ChannelBase<TSend, TReceive, TAckSend, TAckReceive, TDefault> {
  /** The client-side end of the channel with all types flipped. */
  readonly client: ChannelClient<TReceive, TSend, TAckReceive, TAckSend, TDefault>
  abort(abortValue?: unknown): void
}

interface ChannelClient<
  TSend = unknown,
  TReceive = unknown,
  TAckSend = unknown,
  TAckReceive = unknown,
  TDefault extends boolean = false,
> extends ChannelBase<TSend, TReceive, TAckSend, TAckReceive, TDefault> {
  /** The server-side end of the channel with all types flipped. */
  readonly client: Channel<TReceive, TSend, TAckReceive, TAckSend, TDefault>
  abort(): void
}
