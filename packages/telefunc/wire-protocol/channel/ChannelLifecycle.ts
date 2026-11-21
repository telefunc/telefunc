export { ChannelLifecycle }
export type { ChannelLifecycleOwner }

import type { ChannelCloseCallback, ChannelCloseResult } from './channel.js'
import { ChannelClosedError } from './channelErrors.js'
import { ChannelEnding, type EndingFrame, type EndingSide } from './ChannelEnding.js'
import { Abort, type AbortError } from '../../shared/Abort.js'
import { Listeners } from '../../utils/Listeners.js'
import { isPromise } from '../../utils/isPromise.js'
import { DeadlineTimer } from './DeadlineTimer.js'

/** The channel end, which carries its ending's frames and answers what its callbacks throw. */
interface ChannelLifecycleOwner {
  /** It takes no more sends, as it begins or follows a close: the newest lossy send goes first, ahead of the stop. */
  _stopSending(): void
  /** Sends `frame` behind every message this end still owes. */
  _sendEnding(frame: EndingFrame): void
  /** The channel ended, with `err` where it didn't close gracefully: the end lets go of what it holds. */
  _finalize(err: Error | undefined): void
  /** A callback threw `Abort` before the channel ended. */
  _abortFromCallback(err: AbortError): void
}

type ChannelLifecycleOptions = {
  owner: ChannelLifecycleOwner
  side: EndingSide
  /** How long the other end's frames may take to arrive across reconnects. */
  reconnectWindowMs: () => number
  /** Reports what a callback throws or rejects with, other than `Abort` before the end and the channel's own end. */
  reportError: (err: unknown) => void
}

/** A channel end's open and close callbacks and how it ends (`ChannelEnding`): its timer, its `onClose()` cleanup, the
 *  acknowledged sends its close waits for, and the promises that tell the outcome. */
class ChannelLifecycle {
  private readonly owner: ChannelLifecycleOwner
  private readonly reportError: (err: unknown) => void
  private readonly ending: ChannelEnding
  private openListeners = new Listeners<() => void>()
  private didFireOpen = false
  private closeListeners = new Listeners<ChannelCloseCallback>()
  /** The cleanup's callbacks whose promises haven't settled, and the run that calls them. */
  private runningCleanup = 0
  private readonly timer = new DeadlineTimer()
  /** The close callback being called, and whether it read this channel's outcome (`closed` or `close()`) as it ran: a
   *  graceful close doesn't wait for the promise of a cleanup that waits for the close. */
  private callingCleanup = false
  private cleanupAwaitsOutcome = false
  /** It ended with a terminal word of its own, sent behind everything it sent before. */
  private sentOwnEnd = false
  /** What a send made after the end throws, made at the first one. */
  private sendAfterEndError: ChannelClosedError | undefined = undefined

  private openedPromise?: Promise<void>
  private resolveOpened?: () => void
  private rejectOpened?: (err: Error) => void
  private closedPromise?: Promise<ChannelCloseResult>
  private resolveClosed?: (outcome: ChannelCloseResult) => void

  get opened(): Promise<void> {
    if (!this.openedPromise) {
      this.openedPromise = new Promise((resolve, reject) => {
        this.resolveOpened = resolve
        this.rejectOpened = reject
        if (this.didFireOpen) resolve()
        else if (this.ending.ended) reject(this.ending.error ?? new ChannelClosedError())
      })
      this.openedPromise.catch(() => {})
    }
    return this.openedPromise
  }

  /** Resolves with how the channel ended, and never rejects: the channel is closed either way. */
  get closed(): Promise<ChannelCloseResult> {
    if (this.callingCleanup) this.cleanupAwaitsOutcome = true
    if (!this.closedPromise) {
      this.closedPromise = new Promise((resolve) => {
        this.resolveClosed = resolve
        if (this.ending.ended) resolve(this.outcome)
      })
    }
    return this.closedPromise
  }

  constructor({ owner, side, reconnectWindowMs, reportError }: ChannelLifecycleOptions) {
    this.owner = owner
    this.reportError = reportError
    this.ending = new ChannelEnding({
      side,
      port: {
        send: (frame) => this.owner._sendEnding(frame),
        stop: () => this.owner._stopSending(),
        startCleanup: (err) => this.fireClose(err),
        end: (err) => this.onEnded(err),
        armTimer: (at) => this.timer.arm(at, () => this.ending.timerFired()),
        disarmTimer: () => this.timer.disarm(),
        now: () => performance.now(),
        reconnectWindowMs,
      },
    })
  }

  get ended(): boolean {
    return this.ending.ended
  }

  /** It takes no more sends: it began or follows a close, it sends its own end, or it ended. */
  get stopped(): boolean {
    return this.ending.stopped
  }

  /** Its close callbacks have been called. */
  get closeFired(): boolean {
    return this.ending.cleanupStarted
  }

  /** What the channel ended with, where it didn't close gracefully. */
  get closeError(): Error | undefined {
    return this.ending.error
  }

  /** How it ended: `null` where it closed gracefully. */
  private get outcome(): ChannelCloseResult {
    return this.ending.error ?? null
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void {
    if (!this.didFireOpen) {
      // Ended unopened, it never opens: nothing would let go of the registration, or of its signal's listener.
      if (this.ending.ended) return () => {}
      return this.openListeners.add(callback, options)
    }
    if (!options?.signal?.aborted) this.invokeOpenCallback(callback)
    return () => {}
  }

  fireOpen(): void {
    if (this.didFireOpen) return
    this.didFireOpen = true
    this.resolveOpened?.()
    for (const { listener } of this.openListeners.list()) {
      if (!listener) continue
      if (this.invokeOpenCallback(listener)) break
    }
    this.openListeners.clear()
  }

  private invokeOpenCallback(callback: () => void): boolean {
    try {
      const result = callback()
      if (isPromise(result)) result.catch((err: unknown) => this.handleCallbackError(err))
    } catch (err) {
      return this.handleCallbackError(err)
    }
    return false
  }

  /** A callback's `Abort` aborts the channel: true where `err` is one. A send the callback made after the end raced
   *  the end, as a send it awaited does, so what that send threw isn't reported either. */
  handleCallbackError(err: unknown): boolean {
    if (err instanceof Abort) {
      this.owner._abortFromCallback(err)
      return true
    }
    if (this.sendAfterEndError === undefined || err !== this.sendAfterEndError) this.report(err)
    return false
  }

  /** What `send()` throws once the channel takes no more sends: after the end, one error for the channel, with what it
   *  ended with as `cause`. */
  sendRefusal(): ChannelClosedError {
    if (!this.ending.ended) return new ChannelClosedError()
    this.sendAfterEndError ??= new ChannelClosedError(undefined, this.ending.error && { cause: this.ending.error })
    return this.sendAfterEndError
  }

  /** Reports what a callback threw or rejected with, unless it is the error this channel ended with, as a send it
   *  awaited rejects with: that is no bug, and `closed` gives it already. */
  private report(err: unknown): void {
    if (this.ending.error !== undefined && err === this.ending.error) return
    this.reportError(err)
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void {
    if (!this.ending.cleanupStarted) return this.closeListeners.add(callback, options)
    if (!options?.signal?.aborted) void this.invokeCloseCallback(callback, this.outcome)
    return () => {}
  }

  /** The app's `close()`: begins the close, or asks the one under way for `timeoutMs` at most. Every call resolves with
   *  the one outcome, `closed`'s. `peerAbsent`: no page will ever attach. */
  close(timeoutMs: number, { peerAbsent = false } = {}): Promise<ChannelCloseResult> {
    const closed = this.closed
    this.ending.localClose(timeoutMs, { peerAbsent })
    return closed
  }

  peerClose(timeoutMs: number): void {
    this.ending.peerClose(timeoutMs)
  }

  peerCloseAck(): void {
    this.ending.peerCloseAck()
  }

  peerCloseDone(): void {
    this.ending.peerCloseDone()
  }

  /** The connection ended for good with `err`: during a close, neither end can learn its outcome. */
  connectionLost(err: Error): void {
    this.ending.connectionLost(err)
  }

  /** Ends the channel with `err`, after `send` sends its terminal word, unless it ended already; true where it did. */
  endWith(err: Error | undefined, send?: () => void): boolean {
    if (!send) return this.ending.endWith(err)
    return this.ending.endWith(err, () => {
      send()
      this.sentOwnEnd = true
    })
  }

  /** What a send still waiting for credit settles with as the channel ends. Its frame already went: it resolves where
   *  that frame is ahead of an end the other side reads, a graceful close or this end's own terminal word (`abort()`),
   *  and rejects with the channel's outcome otherwise, a page that left included. */
  get waitingSendOutcome(): Error | undefined {
    return this.sentOwnEnd ? undefined : this.ending.error
  }

  /** The close waits for `promise`, an ack request's answer. */
  trackAck<T>(promise: Promise<T>): Promise<T> {
    this.ending.owedStarted()
    return promise.finally(() => this.ending.owedSettled())
  }

  private onEnded(err: Error | undefined): void {
    this.owner._finalize(err)
    if (!this.didFireOpen) this.rejectOpened?.(err ?? new ChannelClosedError())
    this.resolveClosed?.(this.outcome)
    this.openListeners.clear()
  }

  /** Calls the close callbacks with `err`; a graceful close (`undefined`, which they receive as `null`) waits for the
   *  promises they return, but for one that read this channel's outcome as it ran, which would wait for itself. */
  private fireClose(err: Error | undefined): void {
    this.runningCleanup = 1
    for (const { listener } of this.closeListeners.list()) {
      if (!listener) continue
      this.callingCleanup = true
      this.cleanupAwaitsOutcome = false
      let pending: Promise<void> | undefined
      try {
        pending = this.invokeCloseCallback(listener, err ?? null)
      } finally {
        this.callingCleanup = false
      }
      if (pending !== undefined && !this.cleanupAwaitsOutcome) void this.trackCloseListener(pending)
    }
    this.closeListeners.clear()
    this.cleanupStepDone()
  }

  private async trackCloseListener(pending: Promise<void>): Promise<void> {
    this.runningCleanup++
    try {
      await pending
    } finally {
      this.cleanupStepDone()
    }
  }

  private cleanupStepDone(): void {
    if (--this.runningCleanup === 0) this.ending.cleanupSettled()
  }

  /** Where `callback` returned a Promise, returns one that settles after it. What it throws or rejects with is
   *  reported, and an `Abort` before the channel ended aborts it. */
  private invokeCloseCallback(callback: ChannelCloseCallback, err: Error | null): Promise<void> | undefined {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return undefined
      return pending.catch((e: unknown) => this.closeCallbackFailed(e))
    } catch (callbackErr) {
      this.closeCallbackFailed(callbackErr)
      return undefined
    }
  }

  private closeCallbackFailed(err: unknown): void {
    if (err instanceof Abort && !this.ending.ended) this.owner._abortFromCallback(err)
    else this.report(err)
  }
}
