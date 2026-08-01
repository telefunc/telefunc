export { serverDialect }

import { serverRequestTypes } from './request/registry.js'
import { serverTypes } from './response/registry.js'
import { roomReplacer, roomParticipantReplacer, roomRemoteReplacer } from '../room/response-server.js'

const serverDialect = Object.freeze({
  request: Object.freeze([...serverRequestTypes]),
  response: Object.freeze([
    ...serverTypes.slice(0, 7),
    roomReplacer,
    roomParticipantReplacer,
    roomRemoteReplacer,
    ...serverTypes.slice(7),
  ]),
})
