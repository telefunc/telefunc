export { ChannelEnding }
export type { EndingFrame, EndingPort, EndingSide }

import { ChannelCloseUnknownError } from './channelErrors.js'
import { END_CAUSE } from './endCause.js'
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
  /** The other end's verdict ended the close, and that end with it: it needs nothing more of what this end sent.
   *  Called once, before `end`. */
  otherEnded(): void
  /** Calls `timerFired()` at `at`, on the clock `now()` reads, in place of the timer it armed before. */
  armTimer(at: number): void
  disarmTimer(): void
  now(): number
  /** How long past the close's own timeout a follower waits for the verdict: how long the decider's frames may take to
   *  reach it across reconnects. */
  reconnectWindowMs(): number
}

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

/** How one end of a channel ends: the first terminal word, sent or received, ends it, and a graceful close has one
 *  decider. Holds no transport and no callback: its port does. */
class ChannelEnding {
  private readonly side: EndingSide
  private readonly port: EndingPort
  private closing: Closing | null = null
  private terminal: Terminal | null = null
  private owed = 0
  private cleanup: 'idle' | 'running' | 'settled' = 'idle'

  constructor({ side, port }: { side: EndingSide; port: EndingPort }) {
    this.side = side
    this.port = port
  }

  /** It sends nothing more: it began or follows a close, it sends its own end, or it ended. */
  get stopped(): boolean {
    return this.closing !== null || this.terminal !== null
  }

  get ended(): boolean {
    return this.terminal?.ended === true
  }

  /** What it ended with, where it didn't close gracefully. */
  get error(): Error | undefined {
    return this.terminal?.ended ? this.terminal.error : undefined
  }

  /** Its `onClose()` cleanup has begun. */
  get cleanupStarted(): boolean {
    return this.cleanup !== 'idle'
  }

  /** The app closes this end within `timeoutMs`. `peerAbsent`: no other end is there to wait for, so it decides alone;
   *  its frames still go, for one that attaches later. */
  localClose(timeoutMs: number, { peerAbsent = false } = {}): void {
    if (this.ended) return
    const at = this.port.now() + timeoutMs
    const { closing } = this
    if (closing === null) {
      const begun: Closing = peerAbsent
        ? { role: 'decider', timerAt: Number.POSITIVE_INFINITY, peerDone: true }
        : { role: 'initiator', timerAt: Number.POSITIVE_INFINITY }
      this.closing = begun
      this.port.stop()
      this.port.send({ kind: 'close', timeoutMs })
      this.setTimer(begun, at)
    } else if (closing.role === 'follower') {
      // Only the decider's deadline ends the close: this end asks it for a shorter one.
      if (at + this.port.reconnectWindowMs() >= closing.timerAt) return
      this.port.send({ kind: 'close', timeoutMs })
      this.setTimer(closing, at + this.port.reconnectWindowMs())
      return
    } else if (at < closing.timerAt) this.setTimer(closing, at)
    this.progress()
  }

  /** The other end's CLOSE arrived. This and the two after it come only before it ends: an ended channel hands its end
   *  no frame of the other end's. */
  peerClose(timeoutMs: number): void {
    const at = this.port.now() + timeoutMs
    const { closing } = this
    if (closing === null) {
      const follower: Closing = { role: 'follower', timerAt: Number.POSITIVE_INFINITY, sentDone: false }
      this.closing = follower
      this.port.stop()
      this.port.send({ kind: 'closeAck' })
      this.setTimer(follower, at + this.port.reconnectWindowMs())
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
          this.setTimer(follower, Math.min(follower.timerAt, at) + this.port.reconnectWindowMs())
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
      this.port.otherEnded()
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
    const { closing } = this
    if (closing?.role === 'decider' && closing.peerDone) return
    this.endWith(closing === null ? err : new ChannelCloseUnknownError({ cause: err }))
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
    if (this.ended) return
    if (this.closing?.role === 'follower') return this.finish(new ChannelCloseUnknownError())
    this.progress()
  }

  private setTimer(closing: Closing, at: number): void {
    closing.timerAt = at
    if (at !== Number.POSITIVE_INFINITY) this.port.armTimer(at)
  }

  private progress(): void {
    const { closing } = this
    if (this.ended || closing === null) return
    // Past the initiator, the other end's CLOSE or CLOSE_ACK arrived: it sends nothing more.
    if (this.cleanup === 'idle' && closing.role !== 'initiator' && this.owed === 0) {
      this.cleanup = 'running'
      this.port.startCleanup(undefined)
      if (this.ended) return
    }
    if (closing.role === 'follower') {
      if (this.cleanup === 'settled' && !closing.sentDone) {
        closing.sentDone = true
        this.port.send({ kind: 'closeDone' })
      }
      return
    }
    // The deadline is checked where the verdict is chosen: a timer that fires late never turns into a graceful close.
    if (this.port.now() >= closing.timerAt) {
      this.port.send({ kind: 'timedOut' })
      // Its port may end it with a terminal word of its own in the timeout's place.
      if (this.ended) return
      return this.finish(END_CAUSE.closeTimedOut.error())
    }
    if (closing.role === 'decider' && closing.peerDone && this.cleanup === 'settled') {
      this.port.send({ kind: 'closeDone' })
      this.finish(undefined)
    }
  }

  private finish(err: Error | undefined): void {
    this.terminal = { ended: true, error: err }
    this.port.disarmTimer()
    const runCleanup = this.cleanup === 'idle'
    if (runCleanup) this.cleanup = 'settled'
    this.port.end(err)
    if (runCleanup) this.port.startCleanup(err)
  }
}
