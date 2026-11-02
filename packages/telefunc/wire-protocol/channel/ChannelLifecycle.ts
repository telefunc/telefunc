export { ChannelLifecycle }
export type { ChannelLifecycleOwner }

import type { ChannelCloseCallback, ChannelCloseResult } from './channel.js'
import { ChannelClosedError } from './channelErrors.js'
import { isAbort, type AbortError } from '../../shared/Abort.js'
import { isPromise } from '../../utils/isPromise.js'

/** The channel end, which ends itself and answers what its callbacks throw. */
interface ChannelLifecycleOwner {
  /** Ends the channel; `err` where it didn't close gracefully. */
  _end(err?: Error): void
  /** A callback threw `Abort`. */
  _abortFromCallback(err: AbortError): void
  _reportCallbackError(err: unknown): void
}

type ChannelLifecycleOptions = {
  owner: ChannelLifecycleOwner
}

/** A channel end's open, disconnect, reconnect and close callbacks, and its close handshake: the onClose callbacks run
 *  once its CLOSE is acknowledged and no ack is in flight, and it ends once they settled, or at the deadline. */
class ChannelLifecycle {
  private readonly owner: ChannelLifecycleOwner
  private openCallbacks: Array<() => void> = []
  private didFireOpen = false
  private disconnectCallbacks: Array<() => void> = []
  private reconnectCallbacks: Array<() => void> = []
  private isConnected = false
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

  /** It opened, and its peer hasn't gone away since, nor has it closed. */
  get connected(): boolean {
    return this.isConnected
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

  onOpen(callback: () => void): void {
    if (this.didFireOpen) {
      const result = callback()
      if (isPromise(result)) result.catch((err: unknown) => this.handleCallbackError(err))
      return
    }
    this.openCallbacks.push(callback)
  }

  onDisconnect(callback: () => void): void {
    if (!this.didFireClose) this.disconnectCallbacks.push(callback)
  }

  onReconnect(callback: () => void): void {
    if (!this.didFireClose) this.reconnectCallbacks.push(callback)
  }

  /** Its peer attached: the onOpen callbacks run the first time, the onReconnect ones after a disconnect. */
  fireOpen(): void {
    if (this.didFireOpen) {
      this.fireReconnect()
      return
    }
    this.didFireOpen = true
    this.isConnected = true
    this.invokeAll(this.openCallbacks)
    this.openCallbacks.length = 0
  }

  /** Its peer attached again, also while closing: the onReconnect callbacks run if it was disconnected. */
  fireReconnect(): void {
    if (!this.didFireOpen || this.isConnected || this.didFireClose) return
    this.isConnected = true
    this.invokeAll(this.reconnectCallbacks)
  }

  /** Its peer went away, and may come back. */
  fireDisconnect(): void {
    if (!this.isConnected) return
    this.isConnected = false
    this.invokeAll(this.disconnectCallbacks)
  }

  /** A callback's `Abort` aborts the channel: true where `err` is one. */
  handleCallbackError(err: unknown): boolean {
    if (isAbort(err)) {
      this.owner._abortFromCallback(err)
      return true
    }
    this.owner._reportCallbackError(err)
    return false
  }

  onClose(callback: ChannelCloseCallback): void {
    if (this.didFireClose) {
      void this.invokeCloseCallback(callback, this.endError)
      return
    }
    this.closeCallbacks.push(callback)
  }

  /** This end closes, within `timeoutMs` or by its peer's earlier deadline; `awaitingCloseAck` where its peer
   *  acknowledges the CLOSE. */
  startClose(timeoutMs: number, awaitingCloseAck: boolean): void {
    this.awaitingCloseAck = awaitingCloseAck
    this.shortenDeadline(timeoutMs)
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

  /** This end or its peer closes within `timeoutMs`, which only shortens the deadline. */
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
    this.isConnected = false
    for (const callback of this.closeCallbacks) {
      const pending = this.invokeCloseCallback(callback, err)
      if (pending === undefined) continue
      this.pendingCloseCallbacks++
      void pending.finally(() => {
        this.pendingCloseCallbacks--
        this.notifyProgress()
      })
    }
    this.closeCallbacks.length = 0
    this.openCallbacks.length = 0
    this.disconnectCallbacks.length = 0
    this.reconnectCallbacks.length = 0
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

  /** Calls each of `callbacks`, until one throws `Abort`. */
  private invokeAll(callbacks: Array<() => void>): void {
    for (const cb of callbacks) {
      try {
        const result = cb()
        if (isPromise(result)) result.catch((err: unknown) => this.handleCallbackError(err))
      } catch (err) {
        if (this.handleCallbackError(err)) return
      }
    }
  }

  /** Reports what `callback` throws or rejects with; where it returned a Promise, returns one that settles after it. */
  private invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined): Promise<void> | undefined {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return undefined
      return pending.catch((e: unknown) => this.owner._reportCallbackError(e))
    } catch (callbackErr) {
      this.owner._reportCallbackError(callbackErr)
      return undefined
    }
  }
}
