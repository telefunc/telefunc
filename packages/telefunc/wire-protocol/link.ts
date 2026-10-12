export { Link, SocketWait }
export type { SendWait }

import { WIRE_ACK_SHARE, WIRE_PIECE_RATE_WINDOW_MS, WIRE_REFRESH_SHARE } from './constants.js'
import { Fifo } from '../utils/Fifo.js'
import { DOUBLE_ZERO } from '../utils/DOUBLE_ZERO.js'

/** The least span of arrivals a rate the peer took data at is measured over: a frame arrives whole, so a shorter span
 *  can count one that took longer. */
const DRAIN_SPAN_MS = 100
/** The uplink drains at the fastest of this many of the newest such rates. */
const DRAIN_RATES = 8
/** How long a wait stays in the history a download's window reads (see `SendWait`). */
const WAITS_KEPT_MS = 60_000

/** How long the frames this side sends at once wait behind its own data on its wire, where it can tell. */
type SendWait = {
  /** What a frame sent now waits. */
  waitNow(): number
  /** The longest such a frame waited since `at`, on `performance.now()`'s clock, or one sent now waits. */
  longestWaitSince(at: number): number
}

/** One wire, as this side knows it, and every channel on it reads the same: the path's round trip, which an attach's
 *  probe measures; the rate the link carries, as the piece cutter measures it, and the least it carries; and on a page's
 *  WebSocket, the rate the server takes data at, what the uplink holds at that rate, how long the frames that go at
 *  once waited behind it, how long what the server sends waits behind what the page's credit let it send, and when the
 *  page's next acknowledgement or window refresh may go. */
class Link implements SendWait {
  /** The least round trip an attach's probe or a later ping took on this wire, `Infinity` before a probe's answer. */
  pathRtt = Number.POSITIVE_INFINITY
  /** In bytes per ms, from a frame sent to its arrival: never faster than it and what was queued ahead of it crossed.
   *  Past `WIRE_PIECE_RATE_WINDOW_MS` after it last rose, the next measure replaces it. 0 before any. */
  fastest = DOUBLE_ZERO
  private roseAt = DOUBLE_ZERO
  /** In bytes per ms, a rate the link carries at least, by what crossed by an answer, a heartbeat or an
   *  acknowledgement. */
  floor = DOUBLE_ZERO
  /** In bytes per ms: the fastest of the newest rates the peer took data at, 0 before any. */
  drainRate = DOUBLE_ZERO
  private readonly drainRates: number[] = []
  /** In bytes per ms, what the newest acknowledgement that spanned any time says data arrived at. */
  private arrivalRate = DOUBLE_ZERO
  /** The bytes and ms of arrivals not yet in a rate. */
  private spanBytes = 0
  private spanMs = DOUBLE_ZERO
  /** What the uplink holds of the bytes handed to the socket, as of `queuedAt`. */
  private queued = DOUBLE_ZERO
  private queuedAt = DOUBLE_ZERO
  /** How long the frames that go at once waited behind the uplink, and data did as the peer acknowledged it. */
  private readonly waits = new Waits()
  /** The credit the channels on this wire granted the peer that reached it, each grant on its way and when it reaches
   *  it, and the bytes that arrived against it. */
  private grantedBytes = 0
  private readonly grants = new Fifo<{ bytes: number; at: number }>()
  private creditArrivedBytes = 0
  /** In bytes per ms: the fastest of the newest rates the peer's messages arrived at while they kept coming, 0 before
   *  any. The bytes of the span being measured, since when, and when the newest arrived. */
  private deliveryRate = DOUBLE_ZERO
  private readonly deliveryRates: number[] = []
  private deliveredBytes = 0
  private deliveredFrom = DOUBLE_ZERO
  private deliveredAt = Number.NEGATIVE_INFINITY
  /** The span being measured started as the one before it ended, not after a pause. */
  private deliveryGoesOn = false
  /** The acknowledgements and the window refreshes of what this side receives, each kind within its share of the rate
   *  the link carries. */
  readonly acks = new Share(this, WIRE_ACK_SHARE)
  readonly refreshes = new Share(this, WIRE_REFRESH_SHARE)

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

  /** A message of `bytes` from the peer arrived at `at`. After a pause a new span starts: the rate is the link's while
   *  messages keep coming. */
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
    this.deliveryRates.push(this.deliveredBytes / span)
    if (this.deliveryRates.length > DRAIN_RATES) this.deliveryRates.shift()
    this.deliveryRate = Math.max(...this.deliveryRates)
    this.deliveredBytes = 0
    this.deliveredFrom = at
    this.deliveryGoesOn = true
  }

  /** How long a frame the peer sends now waits behind what it sent before: while its messages keep coming at half the
   *  rate they came at or more, what the credit it has lets it have sent and not yet arrived, at that rate; else none. */
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

  /** Takes a round trip a probe on this wire took. Returns whether it is the first. */
  measured(rtt: number): boolean {
    const first = this.pathRtt === Number.POSITIVE_INFINITY
    if (rtt < this.pathRtt) this.pathRtt = rtt
    return first
  }

  /** Takes a rate `rate` measured at `at`: it stands if faster, or once the one before lapsed. Returns whether it stood. */
  rise(rate: number, at: number): boolean {
    if (!(rate > this.fastest) && !this.lapsed(at)) return false
    this.fastest = rate
    this.roseAt = at
    return true
  }

  /** Whether no rate stands at `at`. */
  lapsed(at: number): boolean {
    return this.fastest === 0 || at - this.roseAt > WIRE_PIECE_RATE_WINDOW_MS
  }

  /** Takes a rate the link carried at least. Returns whether it raised the floor. */
  raiseFloor(floor: number): boolean {
    if (!(floor > this.floor)) return false
    this.floor = floor
    return true
  }

  /** In bytes per ms, the slower of the drain rate and the rate the newest acknowledgement that spanned any time says
   *  data arrived at. */
  uplinkRate(): number {
    return this.arrivalRate > 0 ? Math.min(this.drainRate, this.arrivalRate) : this.drainRate
  }

  /** Takes `bytes` that arrived over `spanMs`, the newest, and takes a rate once they span `DRAIN_SPAN_MS`. */
  arrived(bytes: number, spanMs: number): void {
    if (spanMs > 0) this.arrivalRate = bytes / spanMs
    this.spanBytes += bytes
    this.spanMs += spanMs
    if (this.spanMs < DRAIN_SPAN_MS) return
    this.drainRates.push(this.spanBytes / this.spanMs)
    if (this.drainRates.length > DRAIN_RATES) this.drainRates.shift()
    this.drainRate = Math.max(...this.drainRates)
    this.spanBytes = this.spanMs = 0
  }

  /** What the uplink holds of the bytes handed to the socket. */
  inUplink(): number {
    const now = performance.now()
    this.queued = Math.max(0, this.queued - this.drainRate * (now - this.queuedAt))
    this.queuedAt = now
    return this.queued
  }

  /** Notes `bytes` handed to the socket, and returns what the uplink held ahead of them. Before a rate is
   *  measured, what the peer acknowledges alone bounds what the uplink holds. */
  handed(bytes: number): number {
    if (this.drainRate === 0) return 0
    const ahead = this.inUplink()
    this.queued = ahead + bytes
    return ahead
  }

  /** The peer acknowledged all but `unacknowledged` bytes: those left the uplink, though the rate measured so far says
   *  they could still be there. */
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

  /** Notes a wait seen now: what a frame that goes at once waits, or what data waited. */
  noteWait(wait: number): void {
    this.waits.note(wait, performance.now())
  }
}

/** On the server, how long the frames that go at once wait behind what its socket holds (see `SendWait`): what the
 *  runtime says the socket holds, over the fastest of the newest rates it drained at while it held data. Where the
 *  runtime hides it, none. */
class SocketWait implements SendWait {
  private drainRate = DOUBLE_ZERO
  private readonly drainRates: number[] = []
  /** The span being measured: when it started, what the socket held then, and the bytes handed since. */
  private spanFrom = DOUBLE_ZERO
  private spanHeld = 0
  private spanHanded = 0
  private readonly waits = new Waits()

  constructor(private readonly held: () => number | undefined) {}

  /** Notes `bytes` about to be handed to the socket, of a frame that goes at once if `atOnce`. */
  handed(bytes: number, atOnce: boolean): void {
    const now = performance.now()
    if (now - this.spanFrom >= DRAIN_SPAN_MS) this.measure(now)
    if (atOnce) this.waits.note(this.waitNow(), now)
    this.spanHanded += bytes
  }

  /** `SendWait`. */
  waitNow(): number {
    const held = this.held()
    return held === undefined || this.drainRate === 0 ? 0 : held / this.drainRate
  }

  /** `SendWait`. */
  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.waitNow())
  }

  /** Ends the span: a rate where the socket held data at both its ends. */
  private measure(now: number): void {
    const held = this.held() ?? 0
    if (this.spanHeld > 0 && held > 0) {
      this.drainRates.push((this.spanHeld + this.spanHanded - held) / (now - this.spanFrom))
      if (this.drainRates.length > DRAIN_RATES) this.drainRates.shift()
      this.drainRate = Math.max(...this.drainRates)
    }
    this.spanFrom = now
    this.spanHeld = held
    this.spanHanded = 0
  }
}

/** Frames of one kind that take at most `share` of the rate their link carries (`Link.fastest`): each waits its turn, none
 *  before a rate is measured. */
class Share {
  private at = DOUBLE_ZERO

  constructor(
    private readonly link: Link,
    private readonly share: number,
  ) {}

  /** How long a frame of the kind sent now waits. */
  wait(): number {
    return Math.max(0, this.at - performance.now())
  }

  /** Notes a frame of the kind, of `bytes`, handed to the socket. */
  handed(bytes: number): void {
    if (this.link.fastest === 0) return
    this.at = Math.max(performance.now(), this.at) + bytes / (this.share * this.link.fastest)
  }
}

/** Waits by when they were seen, for `WAITS_KEPT_MS`. Each drops the earlier ones no longer than it, so the longest since
 *  an instant is the first at or after it, found by halving. */
class Waits {
  private at: number[] = []
  private wait: number[] = []
  private head = 0

  note(wait: number, at: number): void {
    while (this.at.length > this.head && this.wait[this.wait.length - 1]! <= wait) {
      this.at.pop()
      this.wait.pop()
    }
    this.at.push(at)
    this.wait.push(wait)
    while (this.at[this.head]! < at - WAITS_KEPT_MS) this.head++
    if (this.head > 16 && this.head >= this.at.length >>> 1) {
      this.at = this.at.slice(this.head)
      this.wait = this.wait.slice(this.head)
      this.head = 0
    }
  }

  /** The longest wait seen at or after `at`, 0 for none. */
  since(at: number): number {
    let lo = this.head
    let hi = this.at.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.at[mid]! < at) lo = mid + 1
      else hi = mid
    }
    return lo < this.at.length ? this.wait[lo]! : 0
  }
}
