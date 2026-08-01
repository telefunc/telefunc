export { clientDialect }

import { clientTypes } from './response/registry.js'
import { roomReviver, roomParticipantReviver, roomRemoteReviver } from '../room/response-client.js'

const clientDialect = Object.freeze([
  ...clientTypes.slice(0, 7),
  roomReviver,
  roomParticipantReviver,
  roomRemoteReviver,
  ...clientTypes.slice(7),
])
