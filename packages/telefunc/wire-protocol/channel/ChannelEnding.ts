export { ChannelEnding, closeTimedOutError }
export type { EndingFrame, EndingPort, EndingSide }

import { ChannelClosedError, ChannelCloseUnknownError } from './channelErrors.js'
import { assertProtocol } from '../wire/protocolViolation.js'

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

/** What one end does on the decisions of its `ChannelEnding`. */
interface EndingPort {
  send(frame: EndingFrame): void
  /** It takes no more sends, as it begins or follows a close: called once, before its first frame of the close. */
  stop(): void
  /** Runs the `onClose()` cleanup with `err`, `undefined` for a graceful close; the end calls `cleanupSettled()` once
   *  the promises it returned settled. At the end it isn't awaited. */
  startCleanup(err: Error | undefined): void
  /** The channel ended: with `err`, or gracefully. Called once. */
  end(err: Error | undefined): void
  /** Calls `timerFired()` at `at`, on the clock `now()` reads, in place of the timer it armed before. */
  armTimer(at: number): void
  disarmTimer(): void
  now(): number
  /** How long past the close's own timeout a follower waits for the verdict: how long the decider's frames may take to
   *  reach it across reconnects. */
  reconnectWindowMs(): number
}

type Role =
  /** Sent its CLOSE and hasn't heard the other end yet. */
  | 'initiator'
  /** Owns the deadline and alone sends a graceful verdict: the end whose CLOSE went first, or the server on a cross. */
  | 'decider'
  /** Waits for the decider's verdict, at most the close's timeout and the reconnect window past the CLOSE. */
  | 'follower'

/** How one end of a channel ends: the first terminal word, sent or received, ends it, and a graceful close has one
 *  decider. Holds no transport and no callback: its port does. */
class ChannelEnding {
  private readonly side: EndingSide
  private readonly port: EndingPort
  private role: Role | null = null
  private peerStopped = false
  private peerDone = false
  private sentDone = false
  private owed = 0
  private cleanup: 'idle' | 'running' | 'settled' = 'idle'
  /** The decider's deadline, or the follower's bound. */
  private timerAt = Number.POSITIVE_INFINITY
  private didEnd = false
  /** `endWith()` sends its own end, as an abort does. */
  private sendsOwnEnd = false
  private endError: Error | undefined

  constructor({ side, port }: { side: EndingSide; port: EndingPort }) {
    this.side = side
    this.port = port
  }

  /** It sends nothing more: it began or follows a close, it sends its own end, or it ended. */
  get stopped(): boolean {
    return this.role !== null || this.sendsOwnEnd || this.didEnd
  }

  get ended(): boolean {
    return this.didEnd
  }

  /** What it ended with, where it didn't close gracefully. */
  get error(): Error | undefined {
    return this.endError
  }

  /** Its `onClose()` cleanup has begun. */
  get cleanupStarted(): boolean {
    return this.cleanup !== 'idle'
  }

  /** The app closes this end within `timeoutMs`. `peerAbsent`: no other end is there to wait for, so it decides alone;
   *  its frames still go, for one that attaches later. */
  localClose(timeoutMs: number, { peerAbsent = false } = {}): void {
    if (this.didEnd) return
    const at = this.port.now() + timeoutMs
    switch (this.role) {
      case null:
        this.role = peerAbsent ? 'decider' : 'initiator'
        this.peerStopped = this.peerDone = peerAbsent
        this.port.stop()
        this.port.send({ kind: 'close', timeoutMs })
        this.setTimer(at)
        break
      case 'follower':
        // Only the decider's deadline ends the close: this end asks it for a shorter one.
        if (at + this.port.reconnectWindowMs() >= this.timerAt) return
        this.port.send({ kind: 'close', timeoutMs })
        this.setTimer(at + this.port.reconnectWindowMs())
        return
      default:
        if (at < this.timerAt) this.setTimer(at)
    }
    this.progress()
  }

  /** The other end's CLOSE arrived. */
  peerClose(timeoutMs: number): void {
    if (this.didEnd) return
    const at = this.port.now() + timeoutMs
    switch (this.role) {
      case null:
        this.role = 'follower'
        this.peerStopped = true
        this.port.stop()
        this.port.send({ kind: 'closeAck' })
        this.setTimer(at + this.port.reconnectWindowMs())
        break
      case 'initiator':
        // A cross: each end sent its CLOSE before it had the other's, and each CLOSE is its end's stop boundary.
        this.peerStopped = true
        if (this.side === 'server') {
          this.role = 'decider'
          if (at < this.timerAt) this.setTimer(at)
        } else {
          this.role = 'follower'
          this.setTimer(Math.min(this.timerAt, at) + this.port.reconnectWindowMs())
        }
        break
      case 'decider':
        // The follower asks for a shorter deadline: its CLOSE_ACK, or its crossing CLOSE, came first.
        if (at < this.timerAt) this.setTimer(at)
        break
      case 'follower':
        assertProtocol(false, 'a second CLOSE from the deciding end')
    }
    this.progress()
  }

  peerCloseAck(): void {
    if (this.didEnd) return
    assertProtocol(this.role === 'initiator', 'CLOSE_ACK without a CLOSE')
    this.role = 'decider'
    this.peerStopped = true
    this.progress()
  }

  peerCloseDone(): void {
    if (this.didEnd) return
    assertProtocol(this.role === 'decider' || this.role === 'follower', 'CLOSE_DONE before its boundary')
    if (this.role === 'follower') return this.finish(undefined)
    this.peerDone = true
    this.progress()
  }

  /** Ends it with `err` once `send` sent its terminal word, unless it ended already: the first terminal word, its own or
   *  one that arrived (no `send`), wins. It ends also where `send` throws. True where it ended it. */
  endWith(err: Error | undefined, send?: () => void): boolean {
    if (this.didEnd) return false
    this.sendsOwnEnd = send !== undefined
    try {
      send?.()
    } finally {
      this.finish(err)
    }
    return true
  }

  /** The connection ended for good with `err`. During a close, the verdict can no longer cross, on either end: it ends
   *  with that unknown, `err` its cause. A decider that has the other end's CLOSE_DONE still chooses the verdict, once
   *  its own side is done. */
  connectionLost(err: Error): void {
    if (this.role === 'decider' && this.peerDone) return
    this.endWith(this.role === null ? err : new ChannelCloseUnknownError({ cause: err }))
  }

  /** An acknowledged send, or the answer to one, the close waits for. */
  owedStarted(): void {
    this.owed++
  }

  owedSettled(): void {
    this.owed--
    this.progress()
  }

  cleanupSettled(): void {
    if (this.cleanup === 'running') this.cleanup = 'settled'
    this.progress()
  }

  timerFired(): void {
    if (this.didEnd) return
    if (this.role === 'follower') return this.finish(new ChannelCloseUnknownError())
    this.progress()
  }

  private setTimer(at: number): void {
    this.timerAt = at
    if (at !== Number.POSITIVE_INFINITY) this.port.armTimer(at)
  }

  private progress(): void {
    if (this.didEnd || this.role === null) return
    if (this.cleanup === 'idle' && this.peerStopped && this.owed === 0) {
      this.cleanup = 'running'
      this.port.startCleanup(undefined)
      if (this.didEnd) return
    }
    if (this.role === 'follower') {
      if (this.cleanup === 'settled' && !this.sentDone) {
        this.sentDone = true
        this.port.send({ kind: 'closeDone' })
      }
      return
    }
    // The deadline is checked where the verdict is chosen: a timer that fires late never turns into a graceful close.
    if (this.port.now() >= this.timerAt) {
      this.port.send({ kind: 'timedOut' })
      // Its port may end it with a terminal word of its own in the timeout's place.
      if (this.didEnd) return
      return this.finish(closeTimedOutError())
    }
    if (this.role === 'decider' && this.peerDone && this.cleanup === 'settled') {
      this.port.send({ kind: 'closeDone' })
      this.finish(undefined)
    }
  }

  private finish(err: Error | undefined): void {
    this.didEnd = true
    this.endError = err
    this.port.disarmTimer()
    const runCleanup = this.cleanup === 'idle'
    if (runCleanup) this.cleanup = 'settled'
    this.port.end(err)
    if (runCleanup) this.port.startCleanup(err)
  }
}

function closeTimedOutError(): ChannelClosedError {
  return new ChannelClosedError('Channel close timed out')
}
