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
  private _sentBytes = 0
  private _sentMessages = 0
  private _limitBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private _limitMessages: number = CREDIT_MSG_WINDOW_INITIAL
  /** The largest window the peer grants: what this side's replay holds (see `fitReplay`). */
  private _peerByteWindowMax: number = CREDIT_WINDOW_MAX_BYTES
  /** A `WINDOW` raised the byte limit past the one this side assumed the peer starts with. */
  private _limitRaised = false
  /** `_sentBytes` after the last frame sent while the byte limit was ahead of it. */
  private _sentWithCredit = 0
  /** Senders waiting on credit, oldest first, from the first that waited on. */
  private _waiters: Fifo<() => void> | null = null
  /** A waiter was handed the credit and no send was counted since. */
  private _released = false
  private _releases = 0
  /** Bytes sent since this side's sender last yielded or waited. */
  private _unyieldedBytes = 0
  private readonly _yieldBytes: number
  private _shutdown = false
  /** Since it last answered a `BDP_PING`, the least its wire held as credit ran out; 0: unknown, `Infinity`: none. */
  private _leastBacklog = Infinity

  constructor({ owner, selfTime, yieldBytes }: SendCreditOptions) {
    this.owner = owner
    this.selfTime = selfTime
    this._yieldBytes = yieldBytes ?? Infinity
    macrotaskYield.assertSupported()
  }

  /** The largest window the peer grants. */
  get peerByteWindowMax(): number {
    return this._peerByteWindowMax
  }

  /** Keeps what it lets be in flight within the replay buffer that holds what it sends (see `replayWindow`). */
  fitReplay(peerWindow: number): void {
    this._peerByteWindowMax = Math.min(CREDIT_WINDOW_MAX_BYTES, peerWindow)
    if (!this._limitRaised) this._limitBytes = Math.min(CREDIT_WINDOW_INITIAL_BYTES, this._peerByteWindowMax)
  }

  /** Its peer grants the largest window from the start: the limit is that window, at once. */
  takePeerWindowMax(): void {
    this.onPeerByteWindow(this._peerByteWindowMax)
  }

  /** No byte credit is left. */
  get isPastByteCredit(): boolean {
    return this._limitBytes - this._sentBytes <= 0
  }

  /** Bytes sent past the byte limit and still past it, but the one frame that crossed it with credit. */
  get bytesSentPastCredit(): number {
    return Math.max(0, this._sentBytes - Math.max(this._limitBytes, this._sentWithCredit))
  }

  /** Counts a frame of `bytes` against credit; a Promise where the sender waits for credit or yields. */
  decrement(bytes: number): void | Promise<void> {
    this.countSent(bytes)
    this._released = false
    if (this._isOutOfCredit()) return this._waitForCredit()
    if (this._waiters !== null && this._waiters.length > 0) {
      this._releaseOne()
      return this._waitForCredit()
    }
    this._unyieldedBytes += bytes
    if (this.selfTime.utilisation() > FC_SELF_UTIL_THRESHOLD || this._unyieldedBytes >= this._yieldBytes) {
      this._unyieldedBytes = 0
      return macrotaskYield.yield()
    }
  }

  /** Count a frame that went out without a credit gate, one buffered while no peer was attached. */
  countSent(bytes: number): void {
    this._countSentBytes(bytes)
    this._sentMessages += 1
  }

  /** A limit that doesn't raise the current one is stale, and ignored, as in QUIC. */
  onPeerByteWindow(limit: number): void {
    const raise = (limit - this._limitBytes) | 0
    if (raise <= 0) return
    this._noteBacklog()
    this._limitBytes += raise
    this._limitRaised = true
    this._tryWakeCreditWaiters()
  }

  onPeerMessageWindow(limit: number): void {
    const raise = (limit - this._limitMessages) | 0
    if (raise <= 0) return
    this._noteBacklog()
    this._limitMessages += raise
    this._tryWakeCreditWaiters()
  }

  /** Answers a `BDP_PING` with the least its wire held whenever its credit ran out since the last. */
  onPing(): number {
    this._noteBacklog()
    const backlog = this._leastBacklog
    this._leastBacklog = Infinity
    return backlog
  }

  /** Counts a broadcast's publish, in bytes only, which nothing waits on. */
  countPublishSent(bytes: number): void {
    this._countSentBytes(bytes)
  }

  shutdown(): void {
    if (this._shutdown) return
    this._shutdown = true
    this._tryWakeCreditWaiters()
  }

  private _countSentBytes(bytes: number): void {
    const hadCredit = !this.isPastByteCredit
    this._sentBytes += bytes
    if (hadCredit) this._sentWithCredit = this._sentBytes
  }

  private _noteBacklog(): void {
    if (this._leastBacklog === 0 || !this._isOutOfCredit()) return
    this._leastBacklog = Math.min(this._leastBacklog, this.owner._backlogBytes() ?? 0)
  }

  private _isOutOfCredit(): boolean {
    return this._limitBytes - this._sentBytes <= 0 || this._limitMessages - this._sentMessages <= 0
  }

  private _tryWakeCreditWaiters(): void {
    const waiters = this._waiters
    if (waiters === null) return
    if (this._shutdown) {
      for (let resolve = waiters.shift(); resolve; resolve = waiters.shift()) resolve()
      return
    }
    if (this._released || this._isOutOfCredit()) return
    this._releaseOne()
  }

  /** The oldest waiter's turn. One that hasn't sent by the next macrotask, as one that is done sending, passes it on. */
  private _releaseOne(): void {
    const waiters = this._waiters
    if (waiters === null) return
    const resolve = waiters.shift()
    if (!resolve) return
    this._released = true
    const release = ++this._releases
    resolve()
    if (waiters.length === 0) return
    void macrotaskYield.yield().then(() => {
      if (!this._released || this._releases !== release) return
      this._released = false
      this._tryWakeCreditWaiters()
    })
  }

  private _waitForCredit(): Promise<void> {
    if (this._shutdown) return resolvedPromise
    this._unyieldedBytes = 0
    const waiters = (this._waiters ??= new Fifo())
    return new Promise<void>((resolve) => waiters.push(resolve))
  }
}

const resolvedPromise = Promise.resolve()
