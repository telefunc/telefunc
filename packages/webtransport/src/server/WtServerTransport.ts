export { WtWire }

import {
  PathEstimate,
  WIRE_MAX_RAW_FRAME_BYTES,
  getChannelMux,
  type PeerSender,
  type ServerTransport,
  type WireQueue,
} from 'telefunc/__internal'
import { FrameReader, lengthOf } from '../shared/streamFraming.js'
import type { ListenerSession, SessionHandlers } from './listener.js'
import { StreamCarrier } from './StreamCarrier.js'

/** A page's WebTransport session, which is a channel wire: its control stream carries the wire's own frames, and each
 *  channel's frames go on a stream of their own (`StreamCarrier`). */
class WtWire implements SessionHandlers {
  readonly session: ListenerSession
  sessionId: string | undefined
  /** Nothing waits here for the page: QUIC takes each frame as it goes. */
  readonly queue: WireQueue = {
    waitingPublishes: () => 0,
    dropWaitingPublishes: () => {},
    dropWaitingFromPublish: () => {},
    unhandedBytes: 0,
    drainRate: () => 0,
    path: new PathEstimate({ reportGap: 0 }),
    acknowledged: () => false,
  }
  private readonly carriers = new Map<number, StreamCarrier>()
  /** The frames of each stream the page sends on. */
  private readonly readers = new Map<object, FrameReader>()

  constructor(session: ListenerSession) {
    this.session = session
    getChannelMux().onWireOpen(this, transport)
  }

  carrierOf(index: number, wireSender: PeerSender): StreamCarrier {
    let carrier = this.carriers.get(index)
    if (carrier === undefined) {
      carrier = new StreamCarrier({ session: this.session, wireSender })
      this.carriers.set(index, carrier)
    }
    return carrier
  }

  onData(stream: object, chunk: Uint8Array): void {
    let reader = this.readers.get(stream)
    if (reader === undefined) {
      reader = new FrameReader({ maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES })
      this.readers.set(stream, reader)
    }
    const frames = reader.read(chunk)
    const mux = getChannelMux()
    if (frames === null) return mux.terminateWire(this)
    for (const frame of frames) void mux.onWireMessage(this, frame)
  }

  onStreamEnd(stream: object): void {
    this.readers.delete(stream)
  }

  onClose(): void {
    const mux = getChannelMux()
    mux.onWireClosed(this, { permanent: mux.readPermanentTermination(this) })
  }
}

const transport: ServerTransport<WtWire> = {
  getSessionId: (wire) => wire.sessionId,
  setSessionId: (wire, sessionId) => {
    wire.sessionId = sessionId
  },
  // Every frame of the page's lands on its session.
  getConnId: () => null,
  sendNow: (wire, frame) => {
    wire.session.control.write(lengthOf(frame))
    wire.session.control.write(frame)
  },
  wholeMessages: false,
  bufferedAmount: (wire) => wire.session.control.unsentBytes,
  queueOf: (wire) => wire.queue,
  terminateWire: (wire) => {
    const mux = getChannelMux()
    const permanent = mux.readPermanentTermination(wire)
    wire.session.close()
    mux.onWireClosed(wire, { permanent })
  },
  peerSender: (wire, index, wireSender) => wire.carrierOf(index, wireSender),
}
