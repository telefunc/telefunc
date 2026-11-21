export { assertDriverOrdering, isOrdering }
export type { OrderingInfo }

import { assert } from '../../utils/assert.js'

type OrderingInfo = { seq: number; timestamp: number }

/** A positive safe-integer `seq` and a non-negative safe-integer `timestamp`. */
function isOrdering({ seq, timestamp }: OrderingInfo): boolean {
  return Number.isSafeInteger(seq) && seq > 0 && Number.isSafeInteger(timestamp) && timestamp >= 0
}

/** Checks an ordering a driver returned or delivered against the SPI promise. */
function assertDriverOrdering(info: OrderingInfo): void {
  assert(isOrdering(info), 'A backend driver returned an invalid ordering')
}
