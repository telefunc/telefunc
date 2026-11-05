export { SendCredit }

import type { CreditOwner } from './credit.js'
import { macrotaskYield } from './macrotaskYield.js'
import type { SelfTime } from './SelfTime.js'
import {
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
  FC_SELF_UTIL_THRESHOLD,
} from '../constants.js'
import { Fifo } from '../../utils/Fifo.js'

type SendCreditOptions = {
  owner: CreditOwner
  selfTime: SelfTime
  /** A page's: Chromium takes what a page queued on a socket only between tasks, so its sender yields past this. */
  yieldBytes?: number
}

/** What a channel may send: the byte and message limits its peer granted, as cumulative limits, as QUIC's MAX_DATA. */
class SendCredit {
  private readonly owner: CreditOwner
  private readonly selfTime: SelfTime
  // What this side has sent, and the peer's limits.
  private sentBytes = 0
  private sentMessages = 0
  private limitBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private limitMessages: number = CREDIT_MSG_WINDOW_INITIAL
  /** The largest window the peer grants: what this side's replay holds (see `fitReplay`). */
  private peerByteWindowCap: number = CREDIT_WINDOW_MAX_BYTES
  /** A `WINDOW` raised the byte limit past the one this side assumed the peer starts with. */
  private limitRaised = false
  /** Senders waiting on credit, oldest first, from the first that waited on. */
  private waiters: Fifo<{ resolve: () => void; reject: (err: Error) => void }> | null = null
  /** A waiter was handed the credit and no send was counted since. */
  private released = false
  private releases = 0
  /** Bytes sent since this side's sender last yielded or waited. */
  private unyieldedBytes = 0
  private readonly yieldBytes: number
  private isShutdown = false
  private shutdownError: Error | undefined
  /** Since it last answered a `BDP_PING`, the least its wire held as credit ran out; 0: unknown, `Infinity`: none. */
  private leastBacklog = Infinity

  constructor({ owner, selfTime, yieldBytes }: SendCreditOptions) {
    this.owner = owner
    this.selfTime = selfTime
    this.yieldBytes = yieldBytes ?? Infinity
    macrotaskYield.assertSupported()
  }

  get waitingCount(): number {
    return this.waiters?.length ?? 0
  }

  /** The largest window the peer grants. */
  get peerByteWindowMax(): number {
    return this.peerByteWindowCap
  }

  /** Keeps what it lets be in flight within the replay buffer that holds what it sends (see `replayWindow`). */
  fitReplay(peerWindow: number): void {
    this.peerByteWindowCap = Math.min(CREDIT_WINDOW_MAX_BYTES, peerWindow)
    if (!this.limitRaised) this.limitBytes = Math.min(CREDIT_WINDOW_INITIAL_BYTES, this.peerByteWindowCap)
  }

  /** Bytes sent past the byte limit and still past it. */
  get bytesSentPastCredit(): number {
    return Math.max(0, this.sentBytes - this.limitBytes)
  }

  /** Counts a frame of `bytes` against credit; a Promise where the sender waits for credit or yields. The frame already
   *  went (`sendSequenced`): awaiting the Promise throttles the sender's next send, not awaiting it bypasses credit. */
  decrement(bytes: number): void | Promise<void> {
    this.countSent(bytes)
    this.released = false
    if (this.isOutOfCredit()) return this.waitForCredit()
    if (this.waiters !== null && this.waiters.length > 0) {
      this.releaseOne()
      return this.waitForCredit()
    }
    this.unyieldedBytes += bytes
    if (this.selfTime.utilisation() > FC_SELF_UTIL_THRESHOLD || this.unyieldedBytes >= this.yieldBytes) {
      this.unyieldedBytes = 0
      return macrotaskYield.yield()
    }
  }

  /** Counts a frame that went out without a credit gate: a reply, which no sender waits on. */
  countSent(bytes: number): void {
    this.sentBytes += bytes
    this.sentMessages += 1
  }

  /** Returns the bytes `limit` raised the byte limit by. A limit that doesn't raise the current one is stale, and
   *  ignored, as in QUIC. */
  onPeerByteWindow(limit: number): number {
    const raise = (limit - this.limitBytes) | 0
    if (raise <= 0) return 0
    this.noteBacklog()
    this.limitBytes += raise
    this.limitRaised = true
    this.tryWakeCreditWaiters()
    return raise
  }

  onPeerMessageWindow(limit: number): void {
    const raise = (limit - this.limitMessages) | 0
    if (raise <= 0) return
    this.noteBacklog()
    this.limitMessages += raise
    this.tryWakeCreditWaiters()
  }

  /** Answers a `BDP_PING` with the least its wire held whenever its credit ran out since the last. */
  onPing(): number {
    this.noteBacklog()
    const backlog = this.leastBacklog
    this.leastBacklog = Infinity
    return backlog
  }

  shutdown(err?: Error): void {
    this.shutdownError = err
    this.isShutdown = true
    this.tryWakeCreditWaiters()
  }

  private noteBacklog(): void {
    if (this.leastBacklog === 0 || !this.isOutOfCredit()) return
    this.leastBacklog = Math.min(this.leastBacklog, this.owner.backlogBytes() ?? 0)
  }

  private isOutOfCredit(): boolean {
    return this.limitBytes - this.sentBytes <= 0 || this.limitMessages - this.sentMessages <= 0
  }

  private tryWakeCreditWaiters(): void {
    const waiters = this.waiters
    if (waiters === null) return
    if (this.isShutdown) {
      for (let waiter = waiters.shift(); waiter; waiter = waiters.shift()) {
        if (this.shutdownError) waiter.reject(this.shutdownError)
        else waiter.resolve()
      }
      return
    }
    if (this.released || this.isOutOfCredit()) return
    this.releaseOne()
  }

  /** The oldest waiter's turn. One that hasn't sent by the next macrotask, as one that is done sending, passes it on. */
  private releaseOne(): void {
    const waiters = this.waiters
    if (waiters === null) return
    const waiter = waiters.shift()
    if (!waiter) return
    this.released = true
    const release = ++this.releases
    waiter.resolve()
    if (waiters.length === 0) return
    void macrotaskYield.yield().then(() => {
      if (!this.released || this.releases !== release) return
      this.released = false
      this.tryWakeCreditWaiters()
    })
  }

  private waitForCredit(): Promise<void> {
    if (this.isShutdown) return this.shutdownError ? Promise.reject(this.shutdownError) : resolvedPromise
    this.unyieldedBytes = 0
    const waiters = (this.waiters ??= new Fifo())
    return new Promise<void>((resolve, reject) => waiters.push({ resolve, reject }))
  }
}

const resolvedPromise = Promise.resolve()
