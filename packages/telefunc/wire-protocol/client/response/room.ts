export { roomReviver, roomParticipantReviver, roomRemoteReviver }

import type {
  ClientReviverContext,
  ReviverType,
  RoomContract,
  RoomParticipantContract,
  RoomRemoteContract,
} from '../../types.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../../constants.js'
import { ClientRoom, ClientStandaloneParticipant } from '../../room/client.js'
import { roomCtrlKey } from '../../room/shared.js'

const roomReviver: ReviverType<RoomContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  revive(metadata, context) {
    const stub = context.createBroadcast({ channelId: metadata.channelId, key: roomCtrlKey(metadata.roomId) })
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

/** The metadata's `room` was revived first by the recursive parser — bind the view to that live
 *  `ClientRoom` so `room.getParticipant(m.id) === m`. Subordinate lifetime: the view lives and
 *  dies with its room (`gcTrack: false` — no GC proxy, which would break that `===`). */
const roomRemoteReviver: ReviverType<RoomRemoteContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  revive(metadata) {
    const room = metadata.room as ClientRoom
    return {
      value: room._reviveRemote(metadata),
      close() {},
      abort() {},
      gcTrack: false,
    }
  },
}
