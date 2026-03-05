export type { Channel }

/** Bidirectional channel for real-time communication.
 *
 *  `Channel<TSend, TReceive>` means "I send TSend, I receive TReceive".
 *  Both server and client implement this interface. */
interface Channel<TSend = unknown, TReceive = unknown> {
  readonly id: string
  /** The other end of the channel with flipped types. */
  readonly client: Channel<TReceive, TSend>
  readonly isOpen: boolean
  send(data: TSend): void
  sendBinary(data: Uint8Array): void
  listen(callback: (data: TReceive) => void): void
  listenBinary(callback: (data: Uint8Array) => void): void
  onClose(callback: () => void): void
  onOpen(callback: () => void): void
  close(): void
}
