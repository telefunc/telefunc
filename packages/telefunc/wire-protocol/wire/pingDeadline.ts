export { pingDeadline, reconnectWindowOf }

import { TIMER_DELAY_MAX_MS } from '../constants.js'

/** How long a wire may deliver nothing before it is taken for dead: two ping intervals. */
function pingDeadline(intervalMs: number): number {
  return intervalMs * 2
}

/** How long one end holds the other that went away: until the ping deadline notices, then `reconnectTimeoutMs` more. */
function reconnectWindowOf(pingIntervalMs: number, reconnectTimeoutMs: number): number {
  return Math.min(TIMER_DELAY_MAX_MS, pingDeadline(pingIntervalMs) + reconnectTimeoutMs)
}
