export { assertDuration }

import { assertUsage } from './assert.js'

/** Milliseconds a timer waits, at most `max`, which `bound` explains. */
function assertDuration(value: unknown, configPath: string, max: number, bound: string): asserts value is number {
  assertUsage(
    typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 && value <= max,
    `\`${configPath}\` should be a non-negative safe integer of milliseconds, at most ${max}, ${bound}`,
  )
}
