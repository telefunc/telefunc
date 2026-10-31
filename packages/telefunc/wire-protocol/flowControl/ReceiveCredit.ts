export { ReceiveCredit }
export type { AttachedDownlink, CreditWire }

import { BdpEstimator } from './BdpEstimator.js'
import type { CreditOwner, WindowKind } from './credit.js'
import type { RoundTrips } from './RoundTrips.js'
import type { SelfTime } from './SelfTime.js'
import type { SendWait } from './SendWait.js'
import { CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_INITIAL_BYTES_BATCH, FC_SELF_UTIL_THRESHOLD } from '../constants.js'
import type { BdpPingAck } from '../wire/frame.js'

type ReceiveCreditOptions = {
  owner: CreditOwner
  selfTime: SelfTime
}

/** The wire a channel's credit attaches on: what frames that go at once wait behind, and on a page its round trips and downlink. */
type CreditWire = {
  readonly sendWait: SendWait
  /** A page's uplink's: its round trip, which an answer to a `BDP_PING` says. */
  readonly roundTrips?: RoundTrips
  readonly downlink?: AttachedDownlink
}

/** The page's downlink, as a channel's credit tells it what it granted and what arrived. */
type AttachedDownlink = {
  granted(bytes: number): void
  creditArrived(bytes: number): void
}

/** What a channel lets its peer send: the byte and message windows it grants as cumulative limits, sized by BDP probes. */
class ReceiveCredit {
  private readonly owner: CreditOwner
  private readonly selfTime: SelfTime
  private bdp = new BdpEstimator()
  /** The wire of the last `attach`. */
  private wire: CreditWire | null = null
  private downlink: AttachedDownlink | null = null
  /** `fitReplay` said the replay buffer that bounds the window. */
  private fitted = false
  // What arrived, what was consumed, and what had been consumed when each limit last went out.
  private consumedBytes = 0
  private consumedMessages = 0
  private advertisedBytes = 0
  private advertisedMessages = 0
  /** The byte limit this side granted, and the bytes that arrived against it. */
  private grantedBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private receivedBytes = 0
  /** What had been consumed when the last urgent limit was emitted. */
  private urgentBytes = 0
  private urgentMessages = 0
  /** Bytes of the frames credit doesn't count that arrived since the byte limit last went out. */
  private uncountedBytes = 0
  /** A frame arrived since the byte limit last went out, which acknowledges what arrived. */
  private arrived = false
  /** It grants the largest byte window its peer's replay allows (see `widenByteWindow`). */
  private widened = false
  /** It attached on a wire. */
  private wired = false

  constructor({ owner, selfTime }: ReceiveCreditOptions) {
    this.owner = owner
    this.selfTime = selfTime
  }

  /** The byte window granted. */
  get byteWindow(): number {
    return this.widened ? this.bdp.byteWindowMax : this.bdp.byteWindow
  }

  get msgWindow(): number {
    return this.bdp.msgWindow
  }

  /** The last `attach`'s wire's path round trip, `Infinity` where unmeasured: what an answer to a `BDP_PING` says. */
  pathRtt(): number {
    return this.wire?.roundTrips?.pathRtt ?? Infinity
  }

  /** Keeps what it lets be in flight within the replay buffer that holds what its peer sends (see `replayWindow`). */
  fitReplay(window: number): void {
    this.fitted = true
    this.bdp.capByteWindow(window)
  }

  /** Account one received frame off the wire, emitting a `BDP_PING` iff the estimator opens a probe. */
  onReceived(bytes: number): void {
    this.arrived = true
    this.receivedBytes += bytes
    this.downlink?.creditArrived(bytes)
    if (this.bdp.onReceive(bytes, this.wireWait())) this.owner._sendBdpPing(this.bdp.probe)
  }

  /** How long the frames that go at once wait behind the wire's own data. */
  private wireWait(): SendWait | null {
    return this.wire?.sendWait ?? null
  }

  /** A probe number for an attach's RECONCILE entry on `roundTrips`' wire; `undefined` where it is measured. */
  probeAttach(roundTrips: RoundTrips): number | undefined {
    return this.bdp.probeAttach(roundTrips)
  }

  /** The RECONCILED answered the probe of an attach's RECONCILE entry (see `probeAttach`). */
  onProbeAnswered(probe: number): void {
    this.bdp.settleAttachProbe(probe)
  }

  /** Account post-callback consumption of one frame. */
  onConsumed(bytes: number): void {
    this.consume(bytes, 1)
  }

  /** A frame credit doesn't count, an ack request or its answer, arrived; a `WINDOW` acknowledges it. */
  onReceivedUncounted(bytes: number): void {
    this.arrived = true
    this.uncountedBytes += bytes
    if (this.uncountedBytes >= this.bdp.byteWindow >> 2) this.advertiseBytes('acknowledgement')
  }

  /** At each heartbeat: a `WINDOW` for what arrived since the last, which the sender's replay frees. */
  acknowledge(): void {
    if (this.arrived) this.advertiseBytes('acknowledgement')
  }

  /** This side answered the newest frame that arrived, so no `WINDOW` is owed for what arrived. */
  answered(): void {
    this.uncountedBytes = 0
    this.arrived = false
  }

  /** Settles a `BDP_PING_ACK`; `deliveredBefore(ms)`: what the wire delivered in the `ms` before the ack arrived. */
  onPingAck(ack: BdpPingAck, deliveredBefore: (ms: number) => number): void {
    const suggest = this.bdp.onPingAck(ack, deliveredBefore, this.wireWait())
    if (!suggest.acknowledged) return
    const wantBytes = suggest.bytes === 'grow'
    const wantMsgs = suggest.msgs === 'grow'
    if (!wantBytes && !wantMsgs) return
    if (this.selfTime.utilisation() > FC_SELF_UTIL_THRESHOLD) return
    if (wantBytes) {
      this.bdp.growBytes()
      this.advertiseBytes('flow-control')
    }
    if (wantMsgs) {
      this.bdp.growMsgs()
      this.advertiseMessages('flow-control')
    }
  }

  /** Attach on `wire`. The wire of the last attach lost nothing to repair, and still answers the probe in flight. */
  attach(wire: CreditWire): void {
    if (wire.sendWait !== this.wire?.sendWait) {
      this.wire = wire
      this.downlink = wire.downlink ?? null
      wire.downlink?.granted(this.grantedBytes - this.receivedBytes)
      this.advertiseOnNewWire()
    }
    if (wire.roundTrips) this.bdp.attach(wire.roundTrips)
  }

  /** Frames held back arrived at once: the round in flight doesn't count them as delivered within its round trip. */
  restartRound(): void {
    this.bdp.reset()
  }

  /** Attach on another wire than the last, or the first: the limits go out again, which repairs what the prior lost. */
  private advertiseOnNewWire(): void {
    this.bdp.reset()
    if (!this.wired) {
      this.wired = true
      if (this.arrived || this.consumedBytes + this.byteWindow > this.grantedBytes) {
        this.advertiseBytes('flow-control')
      }
      return
    }
    this.advertiseMessages('flow-control')
    this.advertiseBytes('flow-control')
  }

  /** The largest byte window its peer's replay allows, advertised with the next limit. */
  widenByteWindow(): void {
    this.widened = true
  }

  /** A publish arrived, which starts no BDP probe. */
  onPublishArrived(): void {
    this.arrived = true
  }

  /** A publish of `bytes` was consumed, which takes no message credit. */
  onPublishConsumed(bytes: number): void {
    this.consume(bytes, 0)
  }

  /** Bump to the batch-POST initial window and advertise it. Grow-only / idempotent. */
  useBatchTransportInitial(): void {
    const prev = this.byteWindow
    this.bdp.raiseByteWindow(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    if (this.byteWindow > prev) this.advertiseBytes('flow-control')
  }

  /** Back to the window before `useBatchTransportInitial`, where the estimator never grew the batch-POST one. */
  undoBatchTransportInitial(): void {
    this.bdp.undoByteWindowRaise()
  }

  private consume(bytes: number, messages: number): void {
    this.consumedBytes += bytes
    this.consumedMessages += messages
    this.refresh()
  }

  private refresh(): void {
    if (this.consumedBytes - this.advertisedBytes >= this.bdp.byteWindow >> 2) this.advertiseBytes('flow-control')
    if (this.consumedMessages - this.advertisedMessages >= this.bdp.msgWindow >> 2) {
      this.advertiseMessages('flow-control')
    }
  }

  private advertiseBytes(kind: 'acknowledgement' | 'flow-control'): void {
    if (!this.fitted) return
    this.advertisedBytes = this.consumedBytes
    const limit = this.consumedBytes + this.byteWindow
    if (limit > this.grantedBytes) {
      this.downlink?.granted(limit - this.grantedBytes)
      this.grantedBytes = limit
    }
    this.uncountedBytes = 0
    this.arrived = false
    let sent: WindowKind = kind
    if ((this.consumedBytes - this.urgentBytes) * 2 > this.byteWindow) {
      this.urgentBytes = this.consumedBytes
      sent = 'urgent-flow-control'
    }
    // A message limit behind rides with a byte limit that doesn't ride itself.
    if (sent !== 'acknowledgement' && this.consumedMessages > this.advertisedMessages) {
      this.advertiseMessages('acknowledgement')
    }
    this.owner._sendByteLimit(limit >>> 0, sent)
  }

  /** An `acknowledgement` rides the next message, so it is never urgent. */
  private advertiseMessages(kind: 'acknowledgement' | 'flow-control'): void {
    this.advertisedMessages = this.consumedMessages
    let sent: WindowKind = kind
    if (kind === 'flow-control' && (this.consumedMessages - this.urgentMessages) * 2 > this.bdp.msgWindow) {
      this.urgentMessages = this.consumedMessages
      sent = 'urgent-flow-control'
    }
    this.owner._sendMessageLimit((this.consumedMessages + this.bdp.msgWindow) >>> 0, sent)
  }
}
