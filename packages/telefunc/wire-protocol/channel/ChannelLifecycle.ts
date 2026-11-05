export { ChannelLifecycle }
export type { ChannelLifecycleOwner }

import type { ChannelCloseCallback, ChannelCloseResult } from './channel.js'
import { ChannelClosedError } from './channelErrors.js'
import { isAbort, type AbortError } from '../../shared/Abort.js'
import { Listeners } from '../../utils/Listeners.js'
import { isPromise } from '../../utils/isPromise.js'

/** The channel end, which ends itself and answers what its callbacks throw. */
interface ChannelLifecycleOwner {
  /** Ends the channel; `err` where it didn't close gracefully. */
  _end(err?: Error): void
  /** A callback threw `Abort`. */
  _abortFromCallback(err: AbortError): void
}

type ChannelLifecycleOptions = {
  owner: ChannelLifecycleOwner
  /** Reports what a callback throws or rejects with, other than `Abort`. */
  reportError: (err: unknown) => void
}

/** A channel end's open, disconnect, reconnect and close callbacks, and its close handshake: the onClose callbacks run
 *  once its CLOSE is acknowledged and no ack is in flight, and it ends once they settled, or at the deadline. */
class ChannelLifecycle {
  private readonly owner: ChannelLifecycleOwner
  private readonly reportError: (err: unknown) => void
  private openListeners = new Listeners<() => void>()
  private didFireOpen = false
  private disconnectListeners = new Listeners<() => void>()
  private reconnectListeners = new Listeners<() => void>()
  private isConnected = false
  private closeListeners = new Listeners<ChannelCloseCallback>()
  private didFireClose = false
  private runningCloseListeners = 0
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

  private openedPromise?: Promise<void>
  private resolveOpened?: () => void
  private rejectOpened?: (err: Error) => void
  private closedPromise?: Promise<void>
  private resolveClosed?: () => void
  private rejectClosed?: (err: Error) => void

  get opened(): Promise<void> {
    if (!this.openedPromise) {
      this.openedPromise = new Promise((resolve, reject) => {
        this.resolveOpened = resolve
        this.rejectOpened = reject
        if (this.didFireOpen) resolve()
        else if (this.didEnd) reject(this.endError ?? new ChannelClosedError())
      })
      this.openedPromise.catch(() => {})
    }
    return this.openedPromise
  }

  get closed(): Promise<void> {
    if (!this.closedPromise) {
      this.closedPromise = new Promise((resolve, reject) => {
        this.resolveClosed = resolve
        this.rejectClosed = reject
        if (this.didEnd) {
          if (this.endError) reject(this.endError)
          else resolve()
        }
      })
      this.closedPromise.catch(() => {})
    }
    return this.closedPromise
  }

  constructor({ owner, reportError }: ChannelLifecycleOptions) {
    this.owner = owner
    this.reportError = reportError
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

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): void {
    if (options?.signal?.aborted) return
    if (this.didFireOpen) {
      this.invokeCallback(callback)
      return
    }
    this.openListeners.add(callback, options)
  }

  onDisconnect(callback: () => void, options?: { signal?: AbortSignal }): void {
    if (!this.didFireClose) this.disconnectListeners.add(callback, options)
  }

  onReconnect(callback: () => void, options?: { signal?: AbortSignal }): void {
    if (!this.didFireClose) this.reconnectListeners.add(callback, options)
  }

  /** Its peer attached: the onOpen callbacks run the first time, the onReconnect ones after a disconnect. */
  fireOpen(): void {
    if (this.didFireOpen) {
      this.fireReconnect()
      return
    }
    this.didFireOpen = true
    this.isConnected = true
    this.resolveOpened?.()
    this.invokeAll(this.openListeners)
    this.openListeners.clear()
  }

  /** Its peer attached again, also while closing: the onReconnect callbacks run if it was disconnected. */
  fireReconnect(): void {
    if (!this.didFireOpen || this.isConnected || this.didFireClose) return
    this.isConnected = true
    this.invokeAll(this.reconnectListeners)
  }

  /** Its peer went away, and may come back. */
  fireDisconnect(): void {
    if (!this.isConnected) return
    this.isConnected = false
    this.invokeAll(this.disconnectListeners)
  }

  /** Calls each of `listeners`, until one throws `Abort`. */
  private invokeAll(listeners: Listeners<() => void>): void {
    for (const cb of listeners.list()) {
      if (this.invokeCallback(cb)) return
    }
  }

  private invokeCallback(callback: () => void): boolean {
    try {
      const result = callback()
      if (isPromise(result)) result.catch((err: unknown) => this.handleCallbackError(err))
    } catch (err) {
      return this.handleCallbackError(err)
    }
    return false
  }

  /** A callback's `Abort` aborts the channel: true where `err` is one. */
  handleCallbackError(err: unknown): boolean {
    if (isAbort(err)) {
      this.owner._abortFromCallback(err)
      return true
    }
    this.reportError(err)
    return false
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): void {
    if (options?.signal?.aborted) return
    if (this.didFireClose) {
      void this.invokeCloseCallback(callback, this.endError)
      return
    }
    this.closeListeners.add(callback, options)
  }

  localCloseRequested({
    timeout,
    peer,
    send,
  }: {
    timeout: number
    peer: 'expected' | 'absent'
    send: () => void
  }): Promise<ChannelCloseResult> {
    if (this.closePromise) return this.closePromise
    if (this.didEnd) return Promise.resolve(this.result)
    this.awaitingCloseAck = peer === 'expected'
    this.shortenDeadline(timeout)
    send()
    this.closePromise = this.run ??= this.runUntilEnded()
    return this.closePromise
  }

  peerCloseReceived(timeout: number, acknowledge: () => void): void {
    if (this.didEnd) return
    this.shortenDeadline(timeout)
    acknowledge()
    this.run ??= this.runUntilEnded()
  }

  /** This end or its peer closes within `timeoutMs`, which only shortens the deadline. */
  private shortenDeadline(timeoutMs: number): void {
    const deadline = performance.now() + timeoutMs
    if (this.deadline && deadline >= this.deadline) return
    this.deadline = deadline
    this.notifyProgress()
  }

  /** How long the close has left. */
  timeLeft(): number {
    return Math.max(0, this.deadline - performance.now())
  }

  closeAckReceived(): void {
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
  forcedTermination(err: Error | undefined, finalize: () => void): void {
    if (this.didEnd) return
    this.didEnd = true
    this.endError = err
    this.awaitingCloseAck = false
    if (this.deadlineTimer !== null) clearTimeout(this.deadlineTimer)
    this.deadlineTimer = null
    finalize()
    if (!this.didFireOpen) this.rejectOpened?.(err ?? new ChannelClosedError())
    if (err) this.rejectClosed?.(err)
    else this.resolveClosed?.()
    this.fireClose(err)
    this.notifyProgress()
  }

  private fireClose(err?: Error): void {
    if (this.didFireClose) return
    this.didFireClose = true
    this.isConnected = false
    for (const callback of this.closeListeners.list()) {
      const pending = this.invokeCloseCallback(callback, err)
      if (pending !== undefined) void this.trackCloseListener(pending)
    }
    this.closeListeners.clear()
    this.openListeners.clear()
    this.disconnectListeners.clear()
    this.reconnectListeners.clear()
    this.notifyProgress()
  }

  private async trackCloseListener(pending: Promise<void>): Promise<void> {
    this.runningCloseListeners++
    try {
      await pending
    } finally {
      this.runningCloseListeners--
      this.notifyProgress()
    }
  }

  private async runUntilEnded(): Promise<ChannelCloseResult> {
    while (!this.didEnd) {
      // Fire onClose only once the close roundtrip is settled and no inbound work is in flight,
      // so listeners see "closed" only after the channel can no longer receive frames.
      if (this.inflightAcks === 0 && (!this.awaitingCloseAck || this.didReceiveCloseAck)) this.fireClose()
      if (this.didFireClose && this.runningCloseListeners === 0) {
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

  /** Reports what `callback` throws or rejects with; where it returned a Promise, returns one that settles after it. */
  private invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined): Promise<void> | undefined {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return undefined
      return pending.catch((e: unknown) => this.reportError(e))
    } catch (callbackErr) {
      this.reportError(callbackErr)
      return undefined
    }
  }
}
