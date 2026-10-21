export { Downlink }

import { DRAIN_SPAN_MS, DrainRate } from '../Link.js'
import { Fifo } from '../../../utils/Fifo.js'
import type { Uplink } from './Uplink.js'

/** A page's downlink: the credit its channels granted the server, and the rate the server's messages arrive at. */
class Downlink {
  private readonly uplink: Uplink
  /** The credit the channels on this wire granted the peer that reached it. */
  private grantedBytes = 0
  private readonly grants = new Fifo<{ bytes: number; at: number }>()
  private creditArrivedBytes = 0
  /** The rate the peer's messages arrived at while they kept coming. */
  private readonly delivery = new DrainRate()
  private deliveredBytes = 0
  private deliveredFrom = 0
  private deliveredAt = Number.NEGATIVE_INFINITY
  /** The span being measured started as the one before it ended, not after a pause. */
  private deliveryGoesOn = false

  constructor(uplink: Uplink) {
    this.uplink = uplink
  }

  /** The channels on this wire granted the peer `bytes` more credit: it reaches it once what the uplink holds drained. */
  granted(bytes: number): void {
    const rate = this.uplink.uplinkRate()
    this.grantsReached()
    this.grants.push({ bytes, at: performance.now() + (rate > 0 ? this.uplink.inUplink() / rate : 0) })
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
    this.delivery.measured(this.deliveredBytes / span)
    this.deliveredBytes = 0
    this.deliveredFrom = at
    this.deliveryGoesOn = true
  }

  /** How long a frame the peer sends now waits behind what it sent before, while its messages keep coming. */
  peerQueueWait(): number {
    const now = performance.now()
    if (this.delivery.rate === 0 || now - this.deliveredAt > DRAIN_SPAN_MS) return 0
    // The span so far, or where it just started as the one before it ended, that one.
    const elapsed = now - this.deliveredFrom
    const lately = Math.max(
      this.deliveredBytes / Math.max(1, elapsed),
      this.deliveryGoesOn && elapsed < DRAIN_SPAN_MS ? this.delivery.newest : 0,
    )
    if (lately < this.delivery.rate / 2) return 0
    this.grantsReached()
    return Math.max(0, this.grantedBytes - this.creditArrivedBytes) / this.delivery.rate
  }

  private grantsReached(): void {
    const now = performance.now()
    while (this.grants.length > 0 && this.grants.peek()!.at <= now) this.grantedBytes += this.grants.shift()!.bytes
  }
}
