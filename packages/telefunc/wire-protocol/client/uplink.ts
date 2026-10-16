export { Uplink }

import { WIRE_ACK_SHARE, WIRE_REFRESH_SHARE } from '../constants.js'
import { DRAIN_SPAN_MS, Link, Waits, newestFastest, type SendWait } from '../link.js'
import { TAG } from '../shared-ws.js'
import { Fifo } from '../../utils/Fifo.js'

/** A page's uplink: the rate the server takes data at, what the uplink holds, and the shares its control frames take. */
class Uplink implements SendWait {
  /** In bytes per ms: the fastest of the newest rates the peer took data at, 0 before any. */
  drainRate = 0
  private readonly drainRates: number[] = []
  /** In bytes per ms, what the newest acknowledgement that spanned any time says data arrived at. */
  private arrivalRate = 0
  /** The bytes and ms of arrivals not yet in a rate. */
  private spanBytes = 0
  private spanMs = 0
  /** What the uplink holds of the bytes handed to the socket, as of `queuedAt`. */
  private queued = 0
  private queuedAt = 0
  /** How long the frames that go at once waited behind the uplink, and data did as the peer acknowledged it. */
  private readonly waits = new Waits()
  /** The credit the channels on this wire granted the peer that reached it. */
  private grantedBytes = 0
  private readonly grants = new Fifo<{ bytes: number; at: number }>()
  private creditArrivedBytes = 0
  /** In bytes per ms: the fastest of the newest rates the peer's messages arrived at while they kept coming. */
  private deliveryRate = 0
  private readonly deliveryRates: number[] = []
  private deliveredBytes = 0
  private deliveredFrom = 0
  private deliveredAt = Number.NEGATIVE_INFINITY
  /** The span being measured started as the one before it ended, not after a pause. */
  private deliveryGoesOn = false
  readonly link = new Link()
  /** The acknowledgements and the window refreshes of what this side receives, each within its share of the link. */
  readonly acks = new Share(this.link, WIRE_ACK_SHARE)
  readonly refreshes = new Share(this.link, WIRE_REFRESH_SHARE)

  /** The channels on this wire granted the peer `bytes` more credit: it reaches it once what the uplink holds drained. */
  granted(bytes: number): void {
    const rate = this.uplinkRate()
    this.grantsReached()
    this.grants.push({ bytes, at: performance.now() + (rate > 0 ? this.inUplink() / rate : 0) })
  }

  /** `bytes` arrived against the credit the channels on this wire granted. */
  creditArrived(bytes: number): void {
    this.creditArrivedBytes += bytes
  }

  /** A message of `bytes` from the peer arrived at `at`; after a pause a new span starts. */
  delivered(bytes: number, at: number): void {
    if (at - this.deliveredAt > DRAIN_SPAN_MS) {
      this.deliveredFrom = this.deliveredAt = at
      this.deliveredBytes = 0
      this.deliveryGoesOn = false
      return
    }
    this.deliveredAt = at
    this.deliveredBytes += bytes
    const span = at - this.deliveredFrom
    if (span < DRAIN_SPAN_MS) return
    this.deliveryRate = newestFastest(this.deliveryRates, this.deliveredBytes / span)
    this.deliveredBytes = 0
    this.deliveredFrom = at
    this.deliveryGoesOn = true
  }

  /** How long a frame the peer sends now waits behind what it sent before, while its messages keep coming. */
  peerQueueWait(): number {
    const now = performance.now()
    if (this.deliveryRate === 0 || now - this.deliveredAt > DRAIN_SPAN_MS) return 0
    // The span so far, or where it just started as the one before it ended, that one.
    const elapsed = now - this.deliveredFrom
    const lately = Math.max(
      this.deliveredBytes / Math.max(1, elapsed),
      this.deliveryGoesOn && elapsed < DRAIN_SPAN_MS ? this.deliveryRates.at(-1)! : 0,
    )
    if (lately < this.deliveryRate / 2) return 0
    this.grantsReached()
    return Math.max(0, this.grantedBytes - this.creditArrivedBytes) / this.deliveryRate
  }

  private grantsReached(): void {
    const now = performance.now()
    while (this.grants.length > 0 && this.grants.peek()!.at <= now) this.grantedBytes += this.grants.shift()!.bytes
  }

  /** In bytes per ms, the slower of the drain rate and the rate the newest acknowledgement says data arrived at. */
  uplinkRate(): number {
    return this.arrivalRate > 0 ? Math.min(this.drainRate, this.arrivalRate) : this.drainRate
  }

  /** Takes `bytes` that arrived over `spanMs`, the newest, and takes a rate once they span `DRAIN_SPAN_MS`. */
  arrived(bytes: number, spanMs: number): void {
    if (spanMs > 0) this.arrivalRate = bytes / spanMs
    this.spanBytes += bytes
    this.spanMs += spanMs
    if (this.spanMs < DRAIN_SPAN_MS) return
    this.drainRate = newestFastest(this.drainRates, this.spanBytes / this.spanMs)
    this.spanBytes = this.spanMs = 0
  }

  /** What the uplink holds of the bytes handed to the socket. */
  inUplink(): number {
    const now = performance.now()
    this.queued = Math.max(0, this.queued - this.drainRate * (now - this.queuedAt))
    this.queuedAt = now
    return this.queued
  }

  /** Notes `bytes` handed to the socket, and returns what the uplink held ahead of them. */
  handed(bytes: number): number {
    if (this.drainRate === 0) return 0
    const ahead = this.inUplink()
    this.queued = ahead + bytes
    return ahead
  }

  /** The peer acknowledged all but `unacknowledged` bytes: those left the uplink, whatever the rate says. */
  acknowledged(unacknowledged: number): void {
    this.queued = Math.min(this.inUplink(), unacknowledged)
  }

  /** `SendWait`. */
  waitNow(): number {
    return this.drainRate > 0 ? this.inUplink() / this.drainRate : 0
  }

  /** `SendWait`. */
  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.waitNow())
  }

  /** Notes a frame of `tag` and `bytes` sent: a PIECES_ACK or a window refresh takes its share. */
  sentFrame(tag: number, bytes: number): void {
    if (tag === TAG.PIECES_ACK) this.acks.sent(bytes)
    else if (tag === TAG.WINDOW || tag === TAG.MSG_WINDOW) this.refreshes.sent(bytes)
  }

  /** Notes a wait seen now: what a frame that goes at once waits, or what data waited. */
  noteWait(wait: number): void {
    this.waits.note(wait, performance.now())
  }
}

/** Frames of one kind that take at most `share` of the rate their link carries, none before a rate is measured. */
class Share {
  private at = 0

  constructor(
    private readonly link: Link,
    private readonly share: number,
  ) {}

  /** How long a frame of the kind sent now waits. */
  wait(): number {
    return Math.max(0, this.at - performance.now())
  }

  /** Notes a frame of the kind, of `bytes`, sent. */
  sent(bytes: number): void {
    if (this.link.fastest === 0) return
    this.at = Math.max(performance.now(), this.at) + bytes / (this.share * this.link.fastest)
  }
}
