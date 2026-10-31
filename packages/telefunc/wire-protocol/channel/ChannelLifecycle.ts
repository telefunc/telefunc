export { ChannelLifecycle }
export type { ChannelLifecycleOwner }

import type { ChannelCloseCallback, ChannelCloseResult } from './channel.js'
import { ChannelClosedError } from './channelErrors.js'
import { isPromise } from '../../utils/isPromise.js'

/** The channel end, which ends itself and reports what its callbacks throw. */
interface ChannelLifecycleOwner {
  /** Ends the channel; `err` where it didn't close gracefully. */
  _end(err?: Error): void
  _reportCallbackError(err: unknown): void
}

type ChannelLifecycleOptions = {
  owner: ChannelLifecycleOwner
}

/** A channel end's close handshake: its onClose callbacks run once its CLOSE is acknowledged and no ack is in flight,
 *  and it ends once they settled, or at the deadline. */
class ChannelLifecycle {
  private readonly owner: ChannelLifecycleOwner
  private closeCallbacks: ChannelCloseCallback[] = []
  private didFireClose = false
  private pendingCloseCallbacks = 0
  private inflightAcks = 0
  private awaitingCloseAck = false
  private didReceiveCloseAck = false
  private didEnd = false
  private endError: Error | undefined
  private deadline = 0
  private deadlineTimer: ReturnType<typeof setTimeout> | null = null
  /** The deadline `deadlineTimer` wakes the close at. */
  private timerDeadline = 0
  private wake: (() => void) | null = null
  /** The close's one run, whichever end began it. */
  private run: Promise<ChannelCloseResult> | null = null
  private closePromise: Promise<ChannelCloseResult> | null = null

  constructor({ owner }: ChannelLifecycleOptions) {
    this.owner = owner
  }

  get ended(): boolean {
    return this.didEnd
  }

  /** What the channel ended with, where it didn't close gracefully. */
  get closeError(): Error | undefined {
    return this.endError
  }

  /** What `close()` returned. */
  get closing(): Promise<ChannelCloseResult> | null {
    return this.closePromise
  }

  get closeAcked(): boolean {
    return this.didReceiveCloseAck
  }

  get result(): ChannelCloseResult {
    return this.didReceiveCloseAck ? 0 : 1
  }

  onClose(callback: ChannelCloseCallback): void {
    if (this.didFireClose) {
      this.invokeCloseCallback(callback, this.endError, false)
      return
    }
    this.closeCallbacks.push(callback)
  }

  /** This end closes, within `timeoutMs`; `awaitingCloseAck` where its peer acknowledges the CLOSE. */
  startClose(timeoutMs: number, awaitingCloseAck: boolean): void {
    this.deadline = performance.now() + timeoutMs
    this.awaitingCloseAck = awaitingCloseAck
  }

  /** Runs the close `startClose` began until the channel ended, for `close()` to return. */
  runClose(): Promise<ChannelCloseResult> {
    this.closePromise = this.finish()
    return this.closePromise
  }

  /** Runs the close until the channel ended. */
  finish(): Promise<ChannelCloseResult> {
    this.run ??= this.runUntilEnded()
    return this.run
  }

  /** The peer closes within `timeoutMs`, which only shortens the deadline. */
  shortenDeadline(timeoutMs: number): void {
    const deadline = performance.now() + timeoutMs
    if (this.deadline && deadline >= this.deadline) return
    this.deadline = deadline
    this.notifyProgress()
  }

  /** How long the close has left. */
  timeLeft(): number {
    return Math.max(0, this.deadline - performance.now())
  }

  onCloseAck(): void {
    this.didReceiveCloseAck = true
    this.awaitingCloseAck = false
    this.notifyProgress()
  }

  /** The close waits for `promise`, an ack request's answer. */
  trackAck<T>(promise: Promise<T>): Promise<T> {
    this.inflightAcks++
    return promise.finally(() => {
      this.inflightAcks--
      this.notifyProgress()
    })
  }

  /** The channel ended, with `err` where it didn't close gracefully: its close waits no more. */
  end(err: Error | undefined): void {
    this.didEnd = true
    this.endError = err
    this.awaitingCloseAck = false
    if (this.deadlineTimer !== null) clearTimeout(this.deadlineTimer)
    this.deadlineTimer = null
    this.notifyProgress()
  }

  fireClose(err?: Error): void {
    if (this.didFireClose) return
    this.didFireClose = true
    for (const callback of this.closeCallbacks) this.invokeCloseCallback(callback, err, true)
    this.closeCallbacks.length = 0
    this.notifyProgress()
  }

  private async runUntilEnded(): Promise<ChannelCloseResult> {
    while (!this.didEnd) {
      // Fire onClose only once the close roundtrip is settled and no inbound work is in flight,
      // so listeners see "closed" only after the channel can no longer receive frames.
      if (this.inflightAcks === 0 && (!this.awaitingCloseAck || this.didReceiveCloseAck)) this.fireClose()
      if (this.didFireClose && this.pendingCloseCallbacks === 0) {
        this.owner._end()
        break
      }
      const remaining = this.deadline - performance.now()
      if (remaining <= 0) {
        this.owner._end(new ChannelClosedError('Channel close timed out'))
        break
      }
      this.armDeadline(remaining)
      await new Promise<void>((resolve) => (this.wake = resolve))
    }
    return this.result
  }

  private armDeadline(remaining: number): void {
    if (this.deadlineTimer !== null) {
      if (this.timerDeadline === this.deadline) return
      clearTimeout(this.deadlineTimer)
    }
    this.timerDeadline = this.deadline
    this.deadlineTimer = setTimeout(() => {
      this.deadlineTimer = null
      this.notifyProgress()
    }, remaining)
  }

  private notifyProgress(): void {
    const wake = this.wake
    if (wake === null) return
    this.wake = null
    wake()
  }

  private invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined, track: boolean): void {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return
      if (track) {
        this.pendingCloseCallbacks++
        void pending
          .catch((e: unknown) => this.owner._reportCallbackError(e))
          .finally(() => {
            this.pendingCloseCallbacks--
            this.notifyProgress()
          })
      } else {
        void pending.catch((e: unknown) => this.owner._reportCallbackError(e))
      }
    } catch (callbackErr) {
      this.owner._reportCallbackError(callbackErr)
    }
  }
}
