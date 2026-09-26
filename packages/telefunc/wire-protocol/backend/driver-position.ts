export { assertDriverPosition }

import { assert } from '../../utils/assert.js'
import { isOrderingPosition, type OrderingInfo } from '../ordering-frame.js'

/** Driver output enters core here, once: every ordering mark a driver returns or delivers is checked against the SPI promise. */
function assertDriverPosition(info: OrderingInfo): void {
  assert(isOrderingPosition(info), 'A backend driver returned an invalid ordering position')
}
