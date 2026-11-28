export { DeadlineTimer }

import { TIMER_DELAY_MAX_MS } from '../constants.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

/** Calls a function once `performance.now()` reaches a deadline, however far off. A timer waits at most
 *  `TIMER_DELAY_MAX_MS` (a runtime fires a longer one at once), and may fire a fraction of a millisecond before the
 *  `performance` clock says it is due: the wait is taken in as many timers as it needs. */
class DeadlineTimer {
  private timer: ReturnType<typeof setTimeout> | null = null
  /** The timer doesn't keep the process alive. */
  private readonly unref: boolean

  constructor({ unref = false }: { unref?: boolean } = {}) {
    this.unref = unref
  }

  /** Calls `fire` at `at`, in place of what it was armed with before. */
  arm(at: number, fire: () => void): void {
    this.disarm()
    const timer = setTimeout(
      () => {
        this.timer = null
        if (performance.now() < at) this.arm(at, fire)
        else fire()
      },
      Math.min(Math.max(0, at - performance.now()), TIMER_DELAY_MAX_MS),
    )
    this.timer = this.unref ? unrefTimer(timer) : timer
  }

  disarm(): void {
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
  }
}
