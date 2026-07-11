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
import { roomCtrlKey } from '../../room/protocol.js'
import { assert } from '../../../utils/assert.js'

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
    // `metadata.room` rides along only when selfDelivery is off (see the server replacer); the
    // recursive parser has revived it into its ClientRoom, deduped against any co-returned room.
    let siblingRoom: ClientRoom | null = null
    if (metadata.room !== undefined) {
      assert(metadata.room instanceof ClientRoom)
      siblingRoom = metadata.room
    }
    return {
      value: new ClientStandaloneParticipant(channel, metadata, siblingRoom),
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
    // A remote is only ever serialized alongside its room, so the recursive parser has already
    // revived `metadata.room` into its `ClientRoom` — assert that invariant instead of blind-casting.
    assert(metadata.room instanceof ClientRoom)
    const room = metadata.room
    return {
      value: room._reviveRemote(metadata),
      close() {},
      abort() {},
      gcTrack: false,
    }
  },
}
