export { StreamCarrier }

import {
  TAG,
  type IndexedPeer,
  type LossyLane,
  type PeerSender,
  type PublishBody,
  type RoomWaiter,
} from 'telefunc/__internal'
import { lengthOf } from '../shared/streamFraming.js'
import type { ListenerSession, SendStream } from './listener.js'

type StreamCarrierOptions = {
  session: ListenerSession
  /** Its wire's sender, whose delivery count and send wait it shares. */
  wireSender: PeerSender
}

/** One channel's frames on a WebTransport session: in order on a stream of its own, opened by its first frame and ended
 *  by its last; a lossy publish on a stream of its own (see `StreamLane`). */
class StreamCarrier implements PeerSender {
  /** Nothing waits here: each frame goes to QUIC as it is sent, and QUIC schedules it among the session's streams. */
  readonly queue = { waitingPublishes: () => 0, dropWaitingFromPublish: () => {}, drainRate: () => 0 }
  readonly lossy: StreamLane
  private readonly session: ListenerSession
  private readonly wireSender: PeerSender
  private stream: SendStream | null = null
  /** The stream of the lossy publish being handed, which takes what is sent meanwhile. */
  private unit: SendStream | null = null

  constructor({ session, wireSender }: StreamCarrierOptions) {
    this.session = session
    this.wireSender = wireSender
    this.lossy = new StreamLane({ carrier: this, session })
  }

  send(frame: Uint8Array): void {
    if (this.unit !== null) return writeFrame(this.unit, frame)
    const stream = (this.stream ??= this.session.openStream())
    writeFrame(stream, frame)
    // The channel sends nothing after these, and the page's limit on the streams it lets the server open gets this back.
    if (frame[0] === TAG.CLOSE_ACK || frame[0] === TAG.ABORT || frame[0] === TAG.ERROR) {
      stream.end()
      this.stream = null
    }
  }

  /** Nothing is batched into a next message on a stream. */
  carry(frame: Uint8Array): void {
    this.send(frame)
  }

  bufferedAmount(): number {
    return this.stream?.unsentBytes ?? 0
  }

  sendWait(): ReturnType<PeerSender['sendWait']> {
    return this.wireSender.sendWait()
  }

  deliveredBefore(ms: number): number {
    return this.wireSender.deliveredBefore(ms)
  }

  /** Sends what `write` sends on `unit`. */
  sendOn(unit: SendStream, write: () => void): void {
    this.unit = unit
    try {
      write()
    } finally {
      this.unit = null
    }
  }
}

/** A carrier's lossy lane: each publish on a stream of its own, handed once the one before it left. While the path takes
 *  each before the next is published, every one goes; once it doesn't, the newest replaces the one waiting. QUIC's
 *  congestion controller is the path estimate. */
class StreamLane implements LossyLane {
  private readonly carrier: StreamCarrier
  private readonly session: ListenerSession
  /** The publish handed before is still unsent. */
  private unsent = false
  private waiter: RoomWaiter | null = null

  constructor({ carrier, session }: { carrier: StreamCarrier; session: ListenerSession }) {
    this.carrier = carrier
    this.session = session
  }

  offer(): void {}

  get pathBound(): boolean {
    return this.unsent
  }

  mayHand(_now: number, waiter: RoomWaiter): boolean {
    if (!this.unsent) return true
    this.waiter = waiter
    return false
  }

  hand(peer: IndexedPeer, body: PublishBody): void {
    this.unsent = true
    const unit = this.session.openStream({ unsentOnly: true })
    this.carrier.sendOn(unit, () => peer.sendPublish(body))
    unit.end(() => this.sent())
  }

  private sent(): void {
    this.unsent = false
    const { waiter } = this
    this.waiter = null
    waiter?.roomMayHaveOpened()
  }
}

function writeFrame(stream: SendStream, frame: Uint8Array): void {
  stream.write(lengthOf(frame))
  stream.write(frame)
}
