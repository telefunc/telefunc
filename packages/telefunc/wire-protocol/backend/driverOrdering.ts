export { assertDriverOrdering }

import { assert } from '../../utils/assert.js'
import { isOrdering, type OrderingInfo } from '../wire/orderingFrame.js'

/** Checks an ordering a driver returned or delivered against the SPI promise. */
function assertDriverOrdering(info: OrderingInfo): void {
  assert(isOrdering(info), 'A backend driver returned an invalid ordering')
}
