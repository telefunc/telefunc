// Room's server entry: Telefunc imports it in every .telefunc file and when a Cloudflare Worker starts.
import { config, type ReplacerType, type ServerReplacerContext, type TypeContract } from 'telefunc'
import { loadedRoomServer, type RoomServer } from './server/load.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from './constants.js'
import './cloudflare/extension.js'

/** Room's own replacer once Room's server code is loaded; until then no value is a room's. */
function roomServerReplacer(
  prefix: string,
  own: (server: RoomServer) => ReplacerType<TypeContract, ServerReplacerContext>,
): ReplacerType<TypeContract, ServerReplacerContext> {
  return {
    prefix,
    detect(value): value is unknown {
      const server = loadedRoomServer()
      return server !== undefined && own(server).detect(value)
    },
    replace(value, context) {
      return own(loadedRoomServer()!).replace(value, context)
    },
  }
}

config.extensions.push({
  name: 'room',
  responseTypes: [
    roomServerReplacer(SERIALIZER_PREFIX_ROOM, (server) => server.roomReplacer),
    roomServerReplacer(SERIALIZER_PREFIX_ROOM_PARTICIPANT, (server) => server.roomParticipantReplacer),
    roomServerReplacer(SERIALIZER_PREFIX_ROOM_REMOTE, (server) => server.roomRemoteReplacer),
  ],
})
