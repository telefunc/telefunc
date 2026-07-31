export { roomReviver, roomParticipantReviver, roomRemoteReviver }

import type {
  ClientReviverContext,
  ReviverType,
  RoomContract,
  RoomParticipantContract,
  RoomRemoteContract,
} from '../types.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../constants.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import { roomCtrlKey } from './keys.js'
import { assert } from '../../utils/assert.js'

const roomReviver: ReviverType<RoomContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  revive(metadata, context) {
    const stub = context.createBroadcast({
      channelId: metadata.channelId,
      key: roomCtrlKey(metadata.roomId),
    })
    return {
      value: new ClientRoom(stub, metadata),
      async close() {
        await stub.close()
      },
      abort(abortError) {
        stub.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}
const roomParticipantReviver: ReviverType<RoomParticipantContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  revive(metadata, context) {
    const channel = context.createChannel({ channelId: metadata.channelId })
    // selfDelivery is enforced at the source (the server never relays a suppressed echo to this client's room stub — see server _onTextData), so the client reconstructs a plain standalone.
    return {
      value: new ClientStandaloneParticipant(channel, metadata),
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}
/** The metadata's `room` was revived first by the recursive parser — bind the view to that live `ClientRoom` so `room.getParticipant(m.id) === m`, then adopt its lifecycle. */
const roomRemoteReviver: ReviverType<RoomRemoteContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  revive(metadata, context) {
    // A remote is only ever serialized alongside its room, so the recursive parser has already revived `metadata.room` into its `ClientRoom` — assert that invariant instead of blind-casting.
    assert(metadata.room instanceof ClientRoom)
    const room = metadata.room
    const remote = room._reviveRemote(metadata)
    context.adoptSubordinate(remote, room)
    return {
      value: remote,
      close() {},
      abort() {},
    }
  },
}
