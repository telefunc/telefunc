export { Uplink }

import { WIRE_ACK_SHARE, WIRE_REFRESH_SHARE } from '../constants.js'
import { DRAIN_SPAN_MS, Link, Waits, newestFastest, type SendWait } from '../link.js'
import { TAG } from '../shared-ws.js'

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
  readonly link = new Link()
  /** The acknowledgements and the window refreshes of what this side receives, each within its share of the link. */
  readonly acks = new Share(this.link, WIRE_ACK_SHARE)
  readonly refreshes = new Share(this.link, WIRE_REFRESH_SHARE)

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

  waitNow(): number {
    return this.drainRate > 0 ? this.inUplink() / this.drainRate : 0
  }

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
