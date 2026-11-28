export { responseTypes, unconfirmedWants }
export { ClientBroadcast } from '../../channel/client/ClientBroadcast.js'

import type { FeatureResponseTypes } from './response/registry.js'
import { broadcastReviver } from './response/broadcast.js'
import { ClientConnection } from '../../channel/client/ClientConnection.js'
import { SERIALIZER_PREFIX_BROADCAST } from '../../constants.js'

const responseTypes: FeatureResponseTypes<'broadcasts'> = { [SERIALIZER_PREFIX_BROADCAST]: broadcastReviver }
const unconfirmedWants = ClientConnection.unconfirmedWants
