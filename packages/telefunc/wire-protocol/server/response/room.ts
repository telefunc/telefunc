export { roomReplacer, roomParticipantReplacer }

import type { ReplacerType, RoomContract, RoomParticipantContract, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_ROOM, SERIALIZER_PREFIX_ROOM_PARTICIPANT } from '../../constants.js'
import { ServerLocalParticipant, ServerRoom } from '../../room/server.js'
import { bindParticipantStubChannel, RoomStubChannel } from '../../room/stubs.js'
import { sizeToWire } from '../../room/shared.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const roomReplacer: ReplacerType<RoomContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  detect(value): value is RoomContract['value'] {
    return ServerRoom.isServerRoom(value)
  },
  replace(serverRoom, context) {
    const stub = new RoomStubChannel(serverRoom)
    context.registerChannel(stub)
    // Attach before snapshotting: events from this point on are relayed to the client,
    // earlier state is in the snapshot — overlaps are absorbed by idempotent application.
    serverRoom._attachStub(stub)
    return {
      metadata: {
        channelId: stub.id,
        roomId: serverRoom.id,
        meta: serverRoom.meta,
        size: sizeToWire(serverRoom.size),
        isolated: serverRoom._isolated,
        closed: serverRoom.isClosed,
        // Scalars only — the roster streams over the stub once its peer attaches, so
        // serialization is O(1) in member count.
        count: serverRoom.count,
      },
      async close() {
        await stub.close()
      },
      abort(abortError) {
        stub.abort(abortError.abortValue)
      },
    }
  },
}

const roomParticipantReplacer: ReplacerType<RoomParticipantContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  detect(value): value is RoomParticipantContract['value'] {
    return ServerLocalParticipant.isServerLocalParticipant(value)
  },
  replace(participant, context) {
    const channel = context.createChannel()
    bindParticipantStubChannel(channel, participant)
    return {
      metadata: {
        channelId: channel.id,
        roomId: participant._room.id,
        id: participant.id,
        meta: participant.meta,
        joinedAt: participant._joinedAt,
        selfDelivery: participant.selfDelivery,
      },
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
