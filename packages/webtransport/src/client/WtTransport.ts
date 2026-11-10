export { WtTransport }

import {
  Downlink,
  TAG,
  Uplink,
  WIRE_MAX_RAW_FRAME_BYTES,
  WS_PROBE_TIMEOUT_MS,
  decode,
  encode,
  isWireCtrlTag,
  pairPublishes,
  readU16,
  type DecodedFrame,
  type Heartbeat,
  type OutboundFrame,
  type ProbeOwner,
  type ProbeWire,
  type TransportOwner,
  type UpgradeTarget,
} from 'telefunc/__internal'
import { FrameReader, lengthOf } from '../shared/streamFraming.js'

/** Where the server's RECONCILED says this process takes WebTransport. */
type Offer = { port: number; certificateHash: string }

type WtTransportOptions = { telefuncUrl: string; owner: TransportOwner; offer: unknown }

/** A channel wire on the browser's WebTransport, which only an upgrade reaches: its first stream is the control stream,
 *  each channel the page sends on gets a stream of its own, and every stream the server opens is read as it arrives. */
class WtTransport implements UpgradeTarget {
  readonly type = 'webtransport'
  readonly carriesReconcileInOpen = false
  readonly batched = false
  readonly uplink = new Uplink()
  readonly downlink = new Downlink(this.uplink)
  heartbeat: Heartbeat | null = null
  private readonly telefuncUrl: string
  private readonly owner: TransportOwner
  private readonly offer: unknown
  /** The probed session, till it is adopted or ends. */
  private probed: WtSession | null = null
  private session: WtSession | null = null
  /** The session the page left, still read till the next takes over. */
  private abandoned: WtSession | null = null

  constructor({ telefuncUrl, owner, offer }: WtTransportOptions) {
    this.telefuncUrl = telefuncUrl
    this.owner = owner
    this.offer = offer
  }

  async probe(owner: ProbeOwner): Promise<ProbeWire | null> {
    if (!isOffer(this.offer)) return null
    const url = new URL(this.telefuncUrl, typeof window === 'undefined' ? undefined : window.location.href)
    const session = new WtSession({
      url: `https://${url.hostname}:${this.offer.port}${url.pathname}`,
      certificateHash: this.offer.certificateHash,
      downlink: this.downlink,
      onFrame: (frame, byteLength) => this.takeFrame(session, owner, frame, byteLength),
      onClosed: () => this.onClosed(session, owner),
    })
    if (!(await session.proven())) return null
    this.probed = session
    return {
      ping: () => session.send(encode.ping()),
      send: (frame) => session.send(frame),
      close: () => {
        if (this.probed === session) this.probed = null
        session.close()
      },
      uplink: this.uplink,
    }
  }

  adoptProbe(): void {
    this.session = this.probed
    this.probed = null
  }

  /** Only an upgrade reaches it: the connection falls back a rung, which reconnects without it. */
  start(): void {
    this.owner.onTransportClosed(this)
  }

  hasWire(): boolean {
    return this.session !== null
  }

  isConnecting(): boolean {
    return false
  }

  sendPing(frame: Uint8Array<ArrayBuffer>): void {
    this.session?.send(frame)
  }

  sendFrame({ frame }: OutboundFrame): void {
    this.session?.send(frame)
  }

  /** Each channel's frames go to its stream at once, so none waits behind another's: its credit bounds them. */
  headroom(): number {
    return Number.POSITIVE_INFINITY
  }

  readonly behindAfter = Number.POSITIVE_INFINITY

  bufferedAmount(): number {
    return this.session?.unsentBytes ?? 0
  }

  abandonActiveTransport(): void {
    this.closeAbandonedTransport()
    this.abandoned = this.session
    this.session = null
  }

  closeAbandonedTransport(): void {
    this.abandoned?.close()
    this.abandoned = null
  }

  applyReconciledSettings(): void {}

  attachHeartbeat(heartbeat: Heartbeat): void {
    this.heartbeat = heartbeat
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  async drained(): Promise<void> {
    await this.session?.drained()
  }

  dispose(): void {
    for (const session of [this.probed, this.session, this.abandoned]) session?.close()
    this.probed = this.session = this.abandoned = null
  }

  /** A session's frames go to the upgrade while it is the probe, to the connection once adopted; while it proves itself,
   *  only its PONG counts, which `proven` reads. */
  private takeFrame(session: WtSession, probeOwner: ProbeOwner, frame: DecodedFrame, byteLength: number): void {
    if (session === this.probed) {
      if (frame.tag === TAG.PONG) probeOwner.onProbePong()
      else probeOwner.onProbeFrame(frame, byteLength)
      return
    }
    if (session !== this.session && session !== this.abandoned) return
    this.heartbeat?.noteReceived(byteLength)
    if (frame.tag === TAG.PONG) {
      this.heartbeat?.resetPong()
      this.owner.onTransportPong(frame.ended)
      return
    }
    this.owner.onTransportFrame(frame, this, byteLength, session === this.abandoned)
  }

  private onClosed(session: WtSession, probeOwner: ProbeOwner): void {
    if (session === this.probed) {
      this.probed = null
      probeOwner.onProbeClosed()
    } else if (session === this.session) {
      this.session = null
      this.owner.onTransportClosed(this)
    }
  }
}

function isOffer(offer: unknown): offer is Offer {
  const { port, certificateHash } = (offer ?? {}) as Partial<Offer>
  return typeof port === 'number' && typeof certificateHash === 'string'
}

type WtSessionOptions = {
  url: string
  certificateHash: string
  downlink: Downlink
  onFrame(frame: DecodedFrame, byteLength: number): void
  onClosed(): void
}

/** One WebTransport session: its control stream, a stream per channel the page sends on, and the server's streams. */
class WtSession {
  private readonly wt: WebTransport
  private readonly options: WtSessionOptions
  private readonly control: Promise<WritableStreamDefaultWriter<Uint8Array>>
  private readonly streams = new Map<number, Promise<WritableStreamDefaultWriter<Uint8Array>>>()
  /** What it was handed whose write hasn't settled, which `drained` waits for. */
  private writes = new Set<Promise<void>>()
  unsentBytes = 0
  private closed = false
  private settleProof: (alive: boolean) => void = () => {}
  private readonly proof = new Promise<boolean>((resolve) => (this.settleProof = resolve))

  constructor(options: WtSessionOptions) {
    this.options = options
    this.wt = new WebTransport(options.url, {
      serverCertificateHashes: [{ algorithm: 'sha-256', value: base64Bytes(options.certificateHash) }],
    })
    this.control = this.openControl()
    void this.readIncoming()
    void this.wt.closed.catch(() => {}).finally(() => this.end())
  }

  /** Whether the server answered its first PING within `WS_PROBE_TIMEOUT_MS`: a UDP path that drops it fails here. */
  async proven(): Promise<boolean> {
    const timer = setTimeout(() => this.settleProof(false), WS_PROBE_TIMEOUT_MS)
    this.send(encode.ping())
    const alive = await this.proof
    clearTimeout(timer)
    if (!alive) this.close()
    return alive && !this.closed
  }

  /** Channel frames go on their channel's stream; the wire's own on the control stream. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    if (this.closed) return
    const tag = frame[0]!
    const index = isWireCtrlTag(tag) ? null : readU16(frame, 1)
    const writer = index === null ? this.control : this.streamOf(index)
    const write = this.write(writer, frame)
    this.writes.add(write)
    void write.finally(() => this.writes.delete(write))
    // The channel sends nothing after these, and the server's limit on the streams it lets the page open gets this back.
    if (index !== null && (tag === TAG.CLOSE_ACK || tag === TAG.ABORT || tag === TAG.ERROR)) {
      this.streams.delete(index)
      void writer.then((stream) => stream.close()).catch(() => {})
    }
  }

  async drained(): Promise<void> {
    await Promise.all(this.writes)
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    this.settleProof(false)
    try {
      this.wt.close()
    } catch {}
  }

  private async write(writer: Promise<WritableStreamDefaultWriter<Uint8Array>>, frame: Uint8Array): Promise<void> {
    this.unsentBytes += frame.byteLength
    try {
      const stream = await writer
      void stream.write(lengthOf(frame)).catch(() => {})
      await stream.write(frame)
    } catch {
      // The session went: the connection's replay sends the frame again on the next wire.
    } finally {
      this.unsentBytes -= frame.byteLength
    }
  }

  private streamOf(index: number): Promise<WritableStreamDefaultWriter<Uint8Array>> {
    let writer = this.streams.get(index)
    if (writer === undefined) {
      writer = this.openStream()
      this.streams.set(index, writer)
    }
    return writer
  }

  private async openStream(): Promise<WritableStreamDefaultWriter<Uint8Array>> {
    const stream = await this.wt.createUnidirectionalStream()
    return stream.getWriter()
  }

  private async openControl(): Promise<WritableStreamDefaultWriter<Uint8Array>> {
    await this.wt.ready
    const { readable, writable } = await this.wt.createBidirectionalStream()
    void this.read(readable)
    return writable.getWriter()
  }

  private async readIncoming(): Promise<void> {
    const reader = this.wt.incomingUnidirectionalStreams.getReader()
    try {
      for (;;) {
        const { value, done } = await reader.read()
        if (done) return
        void this.read(value)
      }
    } catch {
      // The session went, which `closed` reports.
    }
  }

  /** Each stream's frames in order, a publish's body paired with the head before it on that stream. */
  private async read(stream: ReadableStream<Uint8Array>): Promise<void> {
    const frames = new FrameReader({ maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES })
    const pair = pairPublishes()
    const reader = stream.getReader()
    try {
      for (;;) {
        const { value, done } = await reader.read()
        if (done) return
        const now = performance.now()
        this.options.downlink.delivered(value.byteLength, now)
        const read = frames.read(value)
        if (read === null) return this.close()
        for (const raw of read) {
          if (raw[0] === TAG.PONG) this.settleProof(true)
          const paired = pair(decode(raw))
          if (paired) this.options.onFrame(paired, raw.byteLength)
        }
      }
    } catch {
      // The session or the stream went.
    }
  }

  private end(): void {
    this.closed = true
    this.settleProof(false)
    this.options.onClosed()
  }
}

function base64Bytes(base64: string): Uint8Array<ArrayBuffer> {
  return Uint8Array.from(atob(base64), (char) => char.charCodeAt(0))
}
