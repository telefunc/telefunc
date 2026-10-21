export { Uplink }

import { DrainRate, Link, Waits, type SendWait } from '../Link.js'

/** A page's uplink: the rate the server takes data at, and what the uplink holds. */
class Uplink implements SendWait {
  /** The rate the peer takes data at. */
  readonly drain = new DrainRate()
  /** In bytes per ms, what the newest acknowledgement that spanned any time says data arrived at. */
  private arrivalRate = 0
  /** What the uplink holds of the bytes handed to the socket, as of `queuedAt`. */
  private queued = 0
  private queuedAt = 0
  /** How long the frames that go at once waited behind the uplink, and data did as the peer acknowledged it. */
  private readonly waits = new Waits()
  readonly link = new Link()

  /** In bytes per ms, the slower of the drain rate and the rate the newest acknowledgement says data arrived at. */
  uplinkRate(): number {
    return this.arrivalRate > 0 ? Math.min(this.drain.rate, this.arrivalRate) : this.drain.rate
  }

  /** Takes `bytes` that arrived over `spanMs`, the newest, and takes a rate once they span `DRAIN_SPAN_MS`. */
  arrived(bytes: number, spanMs: number): void {
    if (spanMs > 0) this.arrivalRate = bytes / spanMs
    this.drain.spanned(bytes, spanMs)
  }

  /** What the uplink holds of the bytes handed to the socket. */
  inUplink(): number {
    const now = performance.now()
    this.queued = Math.max(0, this.queued - this.drain.rate * (now - this.queuedAt))
    this.queuedAt = now
    return this.queued
  }

  /** Notes `bytes` handed to the socket. */
  handed(bytes: number): void {
    if (this.drain.rate === 0) return
    this.queued = this.inUplink() + bytes
  }

  /** The peer acknowledged all but `unacknowledged` bytes: those left the uplink, whatever the rate says. */
  acknowledged(unacknowledged: number): void {
    this.queued = Math.min(this.inUplink(), unacknowledged)
  }

  waitNow(): number {
    return this.drain.rate > 0 ? this.inUplink() / this.drain.rate : 0
  }

  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.waitNow())
  }

  /** Notes a wait seen now: what a frame that goes at once waits, or what data waited. */
  noteWait(wait: number): void {
    this.waits.note(wait, performance.now())
  }
}
