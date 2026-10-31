export { ReceiveCredit }
export type { AttachedDownlink, CreditWire }

import { BdpEstimator } from './BdpEstimator.js'
import type { CreditOwner, WindowKind } from './credit.js'
import type { Link, SendWait } from './Link.js'
import type { SelfTime } from './SelfTime.js'
import { CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_INITIAL_BYTES_BATCH, FC_SELF_UTIL_THRESHOLD } from '../constants.js'
import type { BdpPingAck } from '../wire/frame.js'

type ReceiveCreditOptions = {
  owner: CreditOwner
  selfTime: SelfTime
}

/** The wire a channel's credit attaches on: what frames that go at once wait behind, and on a page its link and downlink. */
type CreditWire = {
  readonly sendWait: SendWait
  /** A page's uplink's: its round trip, which an answer to a `BDP_PING` says. */
  readonly link?: Link
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
  private _bdp = new BdpEstimator()
  /** The wire of the last `attach`. */
  private wire: CreditWire | null = null
  private _downlink: AttachedDownlink | null = null
  /** `fitReplay` said the replay buffer that bounds the window. */
  private _fitted = false
  // What arrived, what was consumed, and what had been consumed when each limit last went out.
  private _consumedBytes = 0
  private _consumedMessages = 0
  private _advertisedBytes = 0
  private _advertisedMessages = 0
  /** The byte limit this side granted, and the bytes that arrived against it. */
  private _grantedBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private _receivedBytes = 0
  /** What had been consumed when the last urgent limit was emitted. */
  private _urgentBytes = 0
  private _urgentMessages = 0
  /** Bytes of the frames credit doesn't count that arrived since the byte limit last went out. */
  private _uncountedBytes = 0
  /** A frame arrived since the byte limit last went out, which acknowledges what arrived. */
  private _arrived = false
  /** It grants the largest byte window its peer's replay allows (see `widenByteWindow`). */
  private _widened = false
  /** It attached on a wire. */
  private _wired = false

  constructor({ owner, selfTime }: ReceiveCreditOptions) {
    this.owner = owner
    this.selfTime = selfTime
  }

  /** The byte window granted. */
  get byteWindow(): number {
    return this._widened ? this._bdp.byteWindowMax : this._bdp.byteWindow
  }

  get msgWindow(): number {
    return this._bdp.msgWindow
  }

  /** The last `attach`'s wire's path round trip, `Infinity` where unmeasured: what an answer to a `BDP_PING` says. */
  pathRtt(): number {
    return this.wire?.link?.pathRtt ?? Infinity
  }

  /** Keeps what it lets be in flight within the replay buffer that holds what its peer sends (see `replayWindow`). */
  fitReplay(window: number): void {
    this._fitted = true
    this._bdp.capByteWindow(window)
  }

  /** Account one received frame off the wire, emitting a `BDP_PING` iff the estimator opens a probe. */
  onReceived(bytes: number): void {
    this._arrived = true
    this._receivedBytes += bytes
    this._downlink?.creditArrived(bytes)
    if (this._bdp.onReceive(bytes, this._wireWait())) this.owner._sendBdpPing(this._bdp.probe)
  }

  /** How long the frames that go at once wait behind the wire's own data. */
  private _wireWait(): SendWait | null {
    return this.wire?.sendWait ?? null
  }

  /** A probe number for an attach on `link`'s RECONCILE entry; `undefined` where `link` is measured. */
  probeAttach(link: Link): number | undefined {
    return this._bdp.probeAttach(link)
  }

  /** The RECONCILED answered the probe of an attach's RECONCILE entry (see `probeAttach`). */
  onProbeAnswered(probe: number): void {
    this._bdp.settleAttachProbe(probe)
  }

  /** Account post-callback consumption of one frame. */
  onConsumed(bytes: number): void {
    this._consume(bytes, 1)
  }

  /** A frame credit doesn't count, an ack request or its answer, arrived; a `WINDOW` acknowledges it. */
  onReceivedUncounted(bytes: number): void {
    this._arrived = true
    this._uncountedBytes += bytes
    if (this._uncountedBytes >= this._bdp.byteWindow >> 2) this._advertiseBytes('acknowledgement')
  }

  /** At each heartbeat: a `WINDOW` for what arrived since the last, which the sender's replay frees. */
  acknowledge(): void {
    if (this._arrived) this._advertiseBytes('acknowledgement')
  }

  /** This side answered the newest frame that arrived, so no `WINDOW` is owed for what arrived. */
  answered(): void {
    this._uncountedBytes = 0
    this._arrived = false
  }

  /** Settles a `BDP_PING_ACK`; `deliveredBefore(ms)`: what the wire delivered in the `ms` before the ack arrived. */
  onPingAck(ack: BdpPingAck, deliveredBefore: (ms: number) => number): void {
    const suggest = this._bdp.onPingAck(ack, deliveredBefore, this._wireWait())
    if (!suggest.acknowledged) return
    const wantBytes = suggest.bytes === 'grow'
    const wantMsgs = suggest.msgs === 'grow'
    if (!wantBytes && !wantMsgs) return
    if (this.selfTime.utilisation() > FC_SELF_UTIL_THRESHOLD) return
    if (wantBytes) {
      this._bdp.growBytes()
      this._advertiseBytes('flow-control')
    }
    if (wantMsgs) {
      this._bdp.growMsgs()
      this._advertiseMessages('flow-control')
    }
  }

  /** Attach on `wire`. The wire of the last attach lost nothing to repair, and still answers the probe in flight. */
  attach(wire: CreditWire): void {
    if (wire.sendWait !== this.wire?.sendWait) {
      this.wire = wire
      this._downlink = wire.downlink ?? null
      wire.downlink?.granted(this._grantedBytes - this._receivedBytes)
      this.advertiseOnNewWire()
    }
    if (wire.link) this._bdp.attach(wire.link)
  }

  /** Frames held back arrived at once: the round in flight doesn't count them as delivered within its round trip. */
  restartRound(): void {
    this._bdp.reset()
  }

  /** Attach on another wire than the last, or the first: the limits go out again, which repairs what the prior lost. */
  private advertiseOnNewWire(): void {
    this._bdp.reset()
    if (!this._wired) {
      this._wired = true
      if (this._arrived || this._consumedBytes + this.byteWindow > this._grantedBytes) {
        this._advertiseBytes('flow-control')
      }
      return
    }
    this._advertiseMessages('flow-control')
    this._advertiseBytes('flow-control')
  }

  /** The largest byte window its peer's replay allows, advertised with the next limit. */
  widenByteWindow(): void {
    this._widened = true
  }

  /** A publish arrived, which starts no BDP probe. */
  onPublishArrived(): void {
    this._arrived = true
  }

  /** A publish of `bytes` was consumed, which takes no message credit. */
  onPublishConsumed(bytes: number): void {
    this._consume(bytes, 0)
  }

  /** Bump to the batch-POST initial window and advertise it. Grow-only / idempotent. */
  useBatchTransportInitial(): void {
    const prev = this.byteWindow
    this._bdp.raiseByteWindow(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    if (this.byteWindow > prev) this._advertiseBytes('flow-control')
  }

  /** Back to the window before `useBatchTransportInitial`, where the estimator never grew the batch-POST one. */
  undoBatchTransportInitial(): void {
    this._bdp.undoByteWindowRaise()
  }

  private _consume(bytes: number, messages: number): void {
    this._consumedBytes += bytes
    this._consumedMessages += messages
    this._refresh()
  }

  private _refresh(): void {
    if (this._consumedBytes - this._advertisedBytes >= this._bdp.byteWindow >> 2) this._advertiseBytes('flow-control')
    if (this._consumedMessages - this._advertisedMessages >= this._bdp.msgWindow >> 2) {
      this._advertiseMessages('flow-control')
    }
  }

  private _advertiseBytes(kind: 'acknowledgement' | 'flow-control'): void {
    if (!this._fitted) return
    this._advertisedBytes = this._consumedBytes
    const limit = this._consumedBytes + this.byteWindow
    if (limit > this._grantedBytes) {
      this._downlink?.granted(limit - this._grantedBytes)
      this._grantedBytes = limit
    }
    this._uncountedBytes = 0
    this._arrived = false
    let sent: WindowKind = kind
    if ((this._consumedBytes - this._urgentBytes) * 2 > this.byteWindow) {
      this._urgentBytes = this._consumedBytes
      sent = 'urgent-flow-control'
    }
    // A message limit behind rides with a byte limit that doesn't ride itself.
    if (sent !== 'acknowledgement' && this._consumedMessages > this._advertisedMessages) {
      this._advertiseMessages('acknowledgement')
    }
    this.owner._sendByteLimit(limit >>> 0, sent)
  }

  /** An `acknowledgement` rides the next message, so it is never urgent. */
  private _advertiseMessages(kind: 'acknowledgement' | 'flow-control'): void {
    this._advertisedMessages = this._consumedMessages
    let sent: WindowKind = kind
    if (kind === 'flow-control' && (this._consumedMessages - this._urgentMessages) * 2 > this._bdp.msgWindow) {
      this._urgentMessages = this._consumedMessages
      sent = 'urgent-flow-control'
    }
    this.owner._sendMessageLimit((this._consumedMessages + this._bdp.msgWindow) >>> 0, sent)
  }
}
