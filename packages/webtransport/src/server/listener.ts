export type { Listener, ListenOptions, ListenerSession, SendStream, SessionHandlers, TlsCertificate }

/** A stream this side writes, in order. */
interface SendStream {
  write(bytes: Uint8Array): void
  /** `onSent` runs once every byte it was handed left this process, on a stream opened `unsentOnly`. */
  end(onSent?: () => void): void
  /** Bytes it was handed that haven't left yet. */
  readonly unsentBytes: number
}

/** A page's WebTransport session, from its control stream on: the bidirectional stream the page opened first. */
interface ListenerSession {
  readonly control: SendStream
  /** A unidirectional stream to the page. `unsentOnly`: it hands QUIC a write only once the one before left. */
  openStream(options?: { unsentOnly?: boolean }): SendStream
  close(): void
}

/** What hears a session: the page's streams, each named by its identity, the control stream among them. */
type SessionHandlers = {
  onData(stream: object, chunk: Uint8Array): void
  onStreamEnd(stream: object): void
  onClose(): void
}

type TlsCertificate = { key: string; cert: string }

type ListenOptions = {
  /** The UDP port; 0 for any free one. */
  port: number
  certificate: TlsCertificate
  onSession(session: ListenerSession): SessionHandlers
}

/** The runtime seam: a WebTransport server on one UDP port, which a native addon can fill as `listeners/quico.ts` does. */
type Listener = {
  readonly port: number
  /** Serves `certificate` from the next handshake on. */
  setCertificate(certificate: TlsCertificate): void
}
