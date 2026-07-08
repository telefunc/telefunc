export { roomReviver, roomParticipantReviver }

import type { ClientReviverContext, ReviverType, RoomContract, RoomParticipantContract } from '../../types.js'
import { SERIALIZER_PREFIX_ROOM, SERIALIZER_PREFIX_ROOM_PARTICIPANT } from '../../constants.js'
import { ClientRoom, ClientStandaloneParticipant } from '../../room/client.js'
import { roomMainKey } from '../../room/shared.js'

const roomReviver: ReviverType<RoomContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  revive(metadata, context) {
    const stub = context.createBroadcast({ channelId: metadata.channelId, key: roomMainKey(metadata.roomId) })
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
