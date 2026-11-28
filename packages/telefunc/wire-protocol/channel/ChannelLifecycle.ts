export { ChannelLifecycle }
export type { ChannelLifecycleOwner, EndingFrame, EndingSide }

import type { ChannelCloseCallback, ChannelCloseResult } from './channel.js'
import { ChannelClosedError, ChannelCloseUnknownError } from './channelErrors.js'
import { END_CAUSE } from './endCause.js'
import { assertProtocol } from '../wire/protocolViolation.js'
import { Abort, type AbortError } from '../../shared/Abort.js'
import { Listeners } from '../../utils/Listeners.js'
import { isPromise } from '../../utils/isPromise.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { DeadlineTimer } from './DeadlineTimer.js'

/** The channel end, which carries its ending's frames and answers what its callbacks throw. */
interface ChannelLifecycleOwner {
  /** It takes no more sends, as it begins or follows a close: the newest lossy send goes first, ahead of the stop. */
  _stopSending(): void
  /** Sends `frame` behind every message this end still owes. */
  _sendEnding(frame: EndingFrame): void
  /** The channel ended, with `err` where it didn't close gracefully: the end lets go of what it holds. */
  _finalize(err: Error | undefined): void
  /** The other end's verdict ended the close, and that end with it: it needs nothing more of what this end sent. */
  _otherEnded(): void
  /** A callback threw `Abort` before the channel ended. */
  _abortFromCallback(err: AbortError): void
}

type EndingSide = 'page' | 'server'

/** The frames a graceful close sends, each sequenced behind every message its end still owes:
 *  - `close`: CLOSE, "I send nothing more; close within `timeoutMs`". The end that begins the close sends it once; the
 *    end that follows sends it again only to ask for a shorter deadline, always after its `closeAck`.
 *  - `closeAck`: CLOSE_ACK, the follower's own "I send nothing more", sent the moment the CLOSE arrives.
 *  - `closeDone`: CLOSE_DONE, "my side of the close is done": its acknowledged sends are answered and its `onClose()`
 *    cleanup settled. The deciding end sends its own only once it has the other's, so from it, it means "closed".
 *  - `timedOut`: ERROR CLOSE_TIMED_OUT, the deciding end's deadline passed first. */
type EndingFrame =
  | { kind: 'close'; timeoutMs: number }
  | { kind: 'closeAck' }
  | { kind: 'closeDone' }
  | { kind: 'timedOut' }

/** Its close, once it began or follows one. `timerAt`: the deadline, or the follower's bound. */
type Closing =
  /** Sent its CLOSE and hasn't heard the other end yet. */
  | { role: 'initiator'; timerAt: number }
  /** Owns the deadline and alone sends a graceful verdict: the end whose CLOSE went first, or the server on a cross.
   *  The other end stopped sending; `peerDone`: its CLOSE_DONE arrived. */
  | { role: 'decider'; timerAt: number; peerDone: boolean }
  /** Waits for the decider's verdict, at most the close's timeout and the reconnect window past the CLOSE. The other end
   *  stopped sending; `sentDone`: this end sent its CLOSE_DONE. */
  | { role: 'follower'; timerAt: number; sentDone: boolean }

type Terminal =
  /** `endWith()` sends its own end, as an abort does. */
  | { ended: false }
  /** It ended: with `error`, or gracefully. */
  | { ended: true; error: Error | undefined }

const SENDING_OWN_END: Terminal = { ended: false }

type ChannelLifecycleOptions = {
  owner: ChannelLifecycleOwner
  side: EndingSide
  /** How long the other end's frames may take to arrive across reconnects. */
  reconnectWindowMs: () => number
  /** Reports what a callback throws or rejects with, other than `Abort` before the end and the channel's own end. */
  reportError: (err: unknown) => void
}

/** A channel end's open and close callbacks and how it ends: its timer, its `onClose()` cleanup, the
 *  acknowledged sends its close waits for, and the promises that tell the outcome. */
class ChannelLifecycle {
  private readonly owner: ChannelLifecycleOwner
  private readonly reportError: (err: unknown) => void
  private readonly side: EndingSide
  private readonly reconnectWindowMs: () => number
  private closing: Closing | null = null
  private terminal: Terminal | null = null
  private owed = 0
  private cleanup: 'idle' | 'running' | 'settled' = 'idle'
  private openListeners = new Listeners<() => void>()
  private didFireOpen = false
  private closeListeners = new Listeners<ChannelCloseCallback>()
  /** The cleanup's callbacks whose promises haven't settled, and the run that calls them. */
  private runningCleanup = 0
  private readonly timer = new DeadlineTimer()
  /** Whether a close callback is being called, and whether it read this channel's outcome (`closed` or `close()`) as
   *  it ran: a graceful close doesn't wait for the promise of a cleanup that waits for the close. */
  private cleanupCall: 'none' | 'running' | 'readOutcome' = 'none'
  /** It ended with a terminal word of its own, sent behind everything it sent before. */
  private sentOwnEnd = false
  /** What a send made after the end throws, made at the first one. */
  private sendAfterEndError: ChannelClosedError | undefined = undefined

  /** Made at the first read of `opened` and `closed`. */
  private openedDeferred: Deferred<void> | null = null
  private closedDeferred: Deferred<ChannelCloseResult> | null = null

  get opened(): Promise<void> {
    if (this.openedDeferred === null) {
      const opened = createDeferred()
      this.openedDeferred = opened
      if (this.didFireOpen) opened.resolve()
      else if (this.ended) opened.reject(this.closeError ?? new ChannelClosedError())
      opened.promise.catch(() => {})
    }
    return this.openedDeferred.promise
  }

  /** Resolves with how the channel ended, and never rejects: the channel is closed either way. */
  get closed(): Promise<ChannelCloseResult> {
    if (this.cleanupCall === 'running') this.cleanupCall = 'readOutcome'
    if (this.closedDeferred === null) {
      const closed = createDeferred<ChannelCloseResult>()
      this.closedDeferred = closed
      if (this.ended) closed.resolve(this.outcome)
    }
    return this.closedDeferred.promise
  }

  constructor({ owner, side, reconnectWindowMs, reportError }: ChannelLifecycleOptions) {
    this.owner = owner
    this.reportError = reportError
    this.side = side
    this.reconnectWindowMs = reconnectWindowMs
  }

  get ended(): boolean {
    return this.terminal?.ended === true
  }

  /** It takes no more sends: it began or follows a close, it sends its own end, or it ended. */
  get stopped(): boolean {
    return this.closing !== null || this.terminal !== null
  }

  /** Its close callbacks have been called. */
  get closeFired(): boolean {
    return this.cleanup !== 'idle'
  }

  /** What the channel ended with, where it didn't close gracefully. */
  get closeError(): Error | undefined {
    return this.terminal?.ended ? this.terminal.error : undefined
  }

  /** How it ended: `null` where it closed gracefully. */
  private get outcome(): ChannelCloseResult {
    return this.closeError ?? null
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void {
    if (!this.didFireOpen) {
      // Ended unopened, it never opens: nothing would let go of the registration, or of its signal's listener.
      if (this.ended) return () => {}
      return this.openListeners.add(callback, options)
    }
    if (!options?.signal?.aborted) this.invokeOpenCallback(callback)
    return () => {}
  }

  fireOpen(): void {
    if (this.didFireOpen) return
    this.didFireOpen = true
    this.openedDeferred?.resolve()
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
    if (!this.ended) return new ChannelClosedError()
    this.sendAfterEndError ??= new ChannelClosedError(undefined, this.closeError && { cause: this.closeError })
    return this.sendAfterEndError
  }

  /** Reports what a callback threw or rejected with, unless it is the error this channel ended with, as a send it
   *  awaited rejects with: that is no bug, and `closed` gives it already. */
  private report(err: unknown): void {
    if (this.closeError !== undefined && err === this.closeError) return
    this.reportError(err)
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void {
    if (this.cleanup === 'idle') return this.closeListeners.add(callback, options)
    if (!options?.signal?.aborted) void this.invokeCloseCallback(callback, this.outcome)
    return () => {}
  }

  /** The app's `close()`: begins the close, or asks the one under way for `timeoutMs` at most. Every call resolves with
   *  the one outcome, `closed`'s. `peerAbsent`: no page will ever attach. */
  close(timeoutMs: number, { peerAbsent = false } = {}): Promise<ChannelCloseResult> {
    const closed = this.closed
    this.beginClose(timeoutMs, peerAbsent)
    return closed
  }

  /** The app closes this end within `timeoutMs`. `peerAbsent`: no other end is there to wait for, so it decides alone;
   *  its frames still go, for one that attaches later. */
  private beginClose(timeoutMs: number, peerAbsent: boolean): void {
    if (this.ended) return
    const at = performance.now() + timeoutMs
    const { closing } = this
    if (closing === null) {
      const begun: Closing = peerAbsent
        ? { role: 'decider', timerAt: Number.POSITIVE_INFINITY, peerDone: true }
        : { role: 'initiator', timerAt: Number.POSITIVE_INFINITY }
      this.closing = begun
      this.owner._stopSending()
      this.owner._sendEnding({ kind: 'close', timeoutMs })
      this.setTimer(begun, at)
    } else if (closing.role === 'follower') {
      // Only the decider's deadline ends the close: this end asks it for a shorter one.
      if (at + this.reconnectWindowMs() >= closing.timerAt) return
      this.owner._sendEnding({ kind: 'close', timeoutMs })
      this.setTimer(closing, at + this.reconnectWindowMs())
      return
    } else if (at < closing.timerAt) this.setTimer(closing, at)
    this.progress()
  }

  /** The other end's CLOSE arrived. This and the two after it come only before it ends: an ended channel hands its end
   *  no frame of the other end's. */
  peerClose(timeoutMs: number): void {
    const at = performance.now() + timeoutMs
    const { closing } = this
    if (closing === null) {
      const follower: Closing = { role: 'follower', timerAt: Number.POSITIVE_INFINITY, sentDone: false }
      this.closing = follower
      this.owner._stopSending()
      this.owner._sendEnding({ kind: 'closeAck' })
      this.setTimer(follower, at + this.reconnectWindowMs())
      return this.progress()
    }
    switch (closing.role) {
      case 'initiator':
        // A cross: each end sent its CLOSE before it had the other's, and each CLOSE is its end's stop boundary.
        if (this.side === 'server') {
          const decider: Closing = { role: 'decider', timerAt: closing.timerAt, peerDone: false }
          this.closing = decider
          if (at < decider.timerAt) this.setTimer(decider, at)
        } else {
          const follower: Closing = { role: 'follower', timerAt: closing.timerAt, sentDone: false }
          this.closing = follower
          this.setTimer(follower, Math.min(follower.timerAt, at) + this.reconnectWindowMs())
        }
        break
      case 'decider':
        // The follower asks for a shorter deadline: its CLOSE_ACK, or its crossing CLOSE, came first.
        if (at < closing.timerAt) this.setTimer(closing, at)
        break
      case 'follower':
        assertProtocol(false, 'a second CLOSE from the deciding end')
    }
    this.progress()
  }

  peerCloseAck(): void {
    const { closing } = this
    assertProtocol(closing?.role === 'initiator', 'CLOSE_ACK without a CLOSE')
    this.closing = { role: 'decider', timerAt: closing.timerAt, peerDone: false }
    this.progress()
  }

  peerCloseDone(): void {
    const { closing } = this
    assertProtocol(closing !== null && closing.role !== 'initiator', 'CLOSE_DONE before its boundary')
    if (closing.role === 'follower') {
      this.owner._otherEnded()
      return this.finish(undefined)
    }
    closing.peerDone = true
    this.progress()
  }

  /** Ends it with `err` once `send` sent its terminal word, unless it ended already: the first terminal word, its own or
   *  one that arrived (no `send`), wins. It ends also where `send` throws. True where it ended it. */
  endWith(err: Error | undefined, send?: () => void): boolean {
    if (this.ended) return false
    if (send) this.terminal = SENDING_OWN_END
    try {
      if (send) {
        send()
        this.sentOwnEnd = true
      }
    } finally {
      this.finish(err)
    }
    return true
  }

  /** The connection ended for good with `err`. During a close, the verdict can no longer cross, on either end: it ends
   *  with that unknown, `err` its cause. A decider that has the other end's CLOSE_DONE still chooses the verdict, once
   *  its own side is done. */
  connectionLost(err: Error): void {
    const { closing } = this
    if (closing?.role === 'decider' && closing.peerDone) return
    this.endWith(closing === null ? err : new ChannelCloseUnknownError({ cause: err }))
  }

  /** What a send still waiting for credit settles with as the channel ends. Its frame already went: it resolves where
   *  that frame is ahead of an end the other side reads, a graceful close or this end's own terminal word (`abort()`),
   *  and rejects with the channel's outcome otherwise, a page that left included. */
  get waitingSendOutcome(): Error | undefined {
    return this.sentOwnEnd ? undefined : this.closeError
  }

  /** The close waits for `promise`, an ack request's answer. */
  trackAck<T>(promise: Promise<T>): Promise<T> {
    this.owed++
    return promise.finally(() => {
      this.owed--
      this.progress()
    })
  }

  /** Calls the close callbacks with `err`; a graceful close (`undefined`, which they receive as `null`) waits for the
   *  promises they return, but for one that read this channel's outcome as it ran, which would wait for itself. */
  private fireClose(err: Error | undefined): void {
    this.runningCleanup = 1
    for (const { listener } of this.closeListeners.list()) {
      if (!listener) continue
      this.cleanupCall = 'running'
      let pending: Promise<void> | undefined
      let awaitsOutcome: boolean
      try {
        pending = this.invokeCloseCallback(listener, err ?? null)
      } finally {
        awaitsOutcome = this.endCleanupCall()
      }
      if (pending !== undefined && !awaitsOutcome) void this.trackCloseListener(pending)
    }
    this.closeListeners.clear()
    this.cleanupStepDone()
  }

  /** Ends the close callback's call: true where it read this channel's outcome as it ran. */
  private endCleanupCall(): boolean {
    const readOutcome = this.cleanupCall === 'readOutcome'
    this.cleanupCall = 'none'
    return readOutcome
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
    if (--this.runningCleanup !== 0) return
    if (this.cleanup === 'running') this.cleanup = 'settled'
    this.progress()
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
    if (err instanceof Abort && !this.ended) this.owner._abortFromCallback(err)
    else this.report(err)
  }

  private timerFired(): void {
    if (this.ended) return
    if (this.closing?.role === 'follower') return this.finish(new ChannelCloseUnknownError())
    this.progress()
  }

  private setTimer(closing: Closing, at: number): void {
    closing.timerAt = at
    if (at !== Number.POSITIVE_INFINITY) this.timer.arm(at, () => this.timerFired())
  }

  private progress(): void {
    const { closing } = this
    if (this.ended || closing === null) return
    // Past the initiator, the other end's CLOSE or CLOSE_ACK arrived: it sends nothing more.
    if (this.cleanup === 'idle' && closing.role !== 'initiator' && this.owed === 0) {
      this.cleanup = 'running'
      this.fireClose(undefined)
      if (this.ended) return
    }
    if (closing.role === 'follower') {
      if (this.cleanup === 'settled' && !closing.sentDone) {
        closing.sentDone = true
        this.owner._sendEnding({ kind: 'closeDone' })
      }
      return
    }
    // The deadline is checked where the verdict is chosen: a timer that fires late never turns into a graceful close.
    if (performance.now() >= closing.timerAt) {
      this.owner._sendEnding({ kind: 'timedOut' })
      // The channel may end with a terminal word of its own in the timeout's place.
      if (this.ended) return
      return this.finish(END_CAUSE.closeTimedOut.error())
    }
    if (closing.role === 'decider' && closing.peerDone && this.cleanup === 'settled') {
      this.owner._sendEnding({ kind: 'closeDone' })
      this.finish(undefined)
    }
  }

  private finish(err: Error | undefined): void {
    this.terminal = { ended: true, error: err }
    this.timer.disarm()
    const runCleanup = this.cleanup === 'idle'
    if (runCleanup) this.cleanup = 'settled'
    this.owner._finalize(err)
    if (!this.didFireOpen) this.openedDeferred?.reject(err ?? new ChannelClosedError())
    this.closedDeferred?.resolve(this.outcome)
    this.openListeners.clear()
    if (runCleanup) this.fireClose(err)
  }
}
