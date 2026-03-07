export type { Channel, AckChannel }

/** Bidirectional channel for real-time communication.
 *
 *  `Channel<TSend, TReceive>` means "I send TSend, I receive TReceive".
 *  Both server and client implement this interface. */
interface Channel<TSend = unknown, TReceive = unknown> {
  readonly id: string
  /** The other end of the channel with flipped types. */
  readonly client: Channel<TReceive, TSend>
  readonly isClosed: boolean
  send(data: TSend): void
  sendBinary(data: Uint8Array): void
  listen(callback: (data: TReceive) => void): void
  listenBinary(callback: (data: Uint8Array) => void): void
  onClose(callback: (err?: Error) => void): void
  onOpen(callback: () => void): void
  close(): void
  abort(abortValue?: unknown): void
}

/** Channel with acknowledgement support.
 *
 *  `TAckSend` — type the sender gets back when the listener resolves.
 *  `TAckReceive` — type the listener must resolve with.
 *  `TDefault = false` — ack is opt-in per-send via `send(data, { ack: true })`.
 *  `TDefault = true`  — ack is on by default; `send(data, { ack: false })` opts out.
 *
 *  Both sides flip: `.client` has TAckSend ↔ TAckReceive swapped. */
interface AckChannel<TSend, TReceive, TAckSend, TAckReceive, TDefault extends boolean = false>
  extends Omit<Channel<TSend, TReceive>, 'client' | 'send' | 'listen'> {
  readonly client: AckChannel<TReceive, TSend, TAckReceive, TAckSend, TDefault>
  /** Default behaviour: returns a Promise when `TDefault = true`, void when `TDefault = false`. */
  send(data: TSend): TDefault extends true ? Promise<TAckSend> : void
  /** Explicitly request an ack. */
  send(data: TSend, opts: { ack: true }): Promise<TAckSend>
  /** Explicitly opt out of ack — always returns void. */
  send(data: TSend, opts: { ack: false }): void
  /** Listener must return the ack value. */
  listen(callback: (data: TReceive) => TAckReceive | Promise<TAckReceive>): void
}
