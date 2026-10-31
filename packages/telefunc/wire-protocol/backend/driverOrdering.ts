export { assertDriverOrdering }

import { assert } from '../../utils/assert.js'
import { isOrderingPosition, type OrderingInfo } from '../wire/orderingFrame.js'

/** Checks an ordering a driver returned or delivered against the SPI promise. */
function assertDriverOrdering(info: OrderingInfo): void {
  assert(isOrderingPosition(info), 'A backend driver returned an invalid ordering position')
}
