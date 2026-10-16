export { normalizeCloseTimeout }

import { CHANNEL_CLOSE_TIMEOUT_MS, TIMER_DELAY_MAX_MS } from './constants.js'
import { assertUsage } from '../utils/assert.js'

function normalizeCloseTimeout(timeout: number | undefined): number {
  if (timeout === undefined) return CHANNEL_CLOSE_TIMEOUT_MS
  assertUsage(
    Number.isFinite(timeout) && timeout >= 0 && timeout <= TIMER_DELAY_MAX_MS,
    `Channel close timeout must be a non-negative number of milliseconds, at most ${TIMER_DELAY_MAX_MS}`,
  )
  return timeout
}
