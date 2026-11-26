export { responseTypes, unconfirmedWants }
export { ClientBroadcast } from '../../channel/client/ClientBroadcast.js'

import type { ClientReviverContext, ReviverType, TypeContract } from '../types.js'
import { broadcastReviver } from './response/broadcast.js'
import { ClientConnection } from '../../channel/client/ClientConnection.js'

const responseTypes: ReviverType<TypeContract, ClientReviverContext>[] = [broadcastReviver]

/** The wants of the page's broadcasts on `telefuncUrl` the server hasn't confirmed: a call carries them. */
function unconfirmedWants(telefuncUrl: string) {
  return ClientConnection.unconfirmedWants(telefuncUrl)
}
