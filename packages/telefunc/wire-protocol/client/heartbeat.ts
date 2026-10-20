export { Heartbeat }

import { Deliveries } from '../deliveries.js'

type HeartbeatOptions = {
  intervalMs: number
  pongTimeoutMs: number
  send: () => void
  onDead: () => void
}

/** Ping-then-pong-deadline loop. Each transport owns one for its wire; the upgrade probe
 *  flow constructs a transient instance for the probed wire until the swap commits. */
class Heartbeat {
  readonly intervalMs: number
  readonly pongTimeoutMs: number
  private readonly send: () => void
  private readonly onDead: () => void
  private pingTimer: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  /** When the wire last delivered a frame, or the heartbeat started or got a PONG. */
  private lastReceivedAt = 0
  private readonly deliveries = new Deliveries()
  /** Until when the wire may deliver nothing: what it still carries takes that long. */
  private expectedBy = () => 0

  constructor({ intervalMs, pongTimeoutMs, send, onDead }: HeartbeatOptions) {
    this.intervalMs = intervalMs
    this.pongTimeoutMs = pongTimeoutMs
    this.send = send
    this.onDead = onDead
  }

  /** `pingNow`: its first PING goes at once, rather than once its interval passed. */
  start(pingNow: boolean): void {
    if (this.pingTimer) return
    if (pingNow) this.send()
    this.resetPong()
    this.pingTimer = setInterval(this.send, this.intervalMs)
  }

  resetPong(): void {
    this.lastReceivedAt = performance.now()
    this.armPongDeadline(this.pongTimeoutMs)
  }

  noteReceived(bytes: number, at = performance.now()): void {
    this.deliveries.note(at, bytes)
    this.lastReceivedAt = at
  }

  /** The bytes the wire delivered in the `ms` before it last delivered. */
  deliveredBefore(ms: number): number {
    return this.deliveries.before(this.deliveries.last, ms)
  }

  expectFrom(expectedBy: () => number): void {
    this.expectedBy = expectedBy
  }

  /** How long the wire delivered nothing, less what it may still take past the deadline to deliver what it carries. */
  quietFor(): number {
    return performance.now() - Math.max(this.lastReceivedAt, this.expectedBy() - this.pongTimeoutMs)
  }

  private armPongDeadline(ms: number): void {
    if (this.pongTimer) clearTimeout(this.pongTimer)
    this.pongTimer = setTimeout(this.onPongDeadline, ms)
  }

  /** A PONG waits behind what the server queued: the wire is dead once it delivered nothing for the deadline. */
  private readonly onPongDeadline = (): void => {
    const quiet = this.quietFor()
    if (quiet < this.pongTimeoutMs) this.armPongDeadline(this.pongTimeoutMs - quiet)
    else this.onDead()
  }

  stop(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
    if (this.pongTimer) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
  }
}
