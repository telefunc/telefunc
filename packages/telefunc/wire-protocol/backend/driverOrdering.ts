export { assertDriverOrdering, isOrdering }
export type { OrderingInfo }

import { assert } from '../../utils/assert.js'

/** `rewound`: a failover replaced what the driver delivered on the lane, and it follows the new history from this
 *  message, whose `seq` is at or below one it delivered before. */
type OrderingInfo = { seq: number; timestamp: number; rewound?: true }

/** A positive safe-integer `seq` and a non-negative safe-integer `timestamp`. */
function isOrdering({ seq, timestamp }: OrderingInfo): boolean {
  return Number.isSafeInteger(seq) && seq > 0 && Number.isSafeInteger(timestamp) && timestamp >= 0
}

/** Checks an ordering a driver returned or delivered against the SPI promise. */
function assertDriverOrdering(info: OrderingInfo): void {
  assert(isOrdering(info), 'A backend driver returned an invalid ordering')
}
