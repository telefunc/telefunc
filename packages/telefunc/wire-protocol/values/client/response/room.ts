export { roomRemoteReviver, roomTypes }

import type { InternalClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { ClientRoom, ClientStandaloneParticipant } from '../../../room/client/room.js'
import type { LocalParticipant, RemoteParticipant } from '../../../room/types.js'
import type {
  ParticipantStubMetadata,
  RemoteParticipantMetadata,
  RoomSnapshotMetadata,
} from '../../../room/protocol.js'
import { assert } from '../../../../utils/assert.js'
import { untether } from '../../../../utils/wrapProxy.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../../../constants.js'

type RoomReviverContract = TypeContract<never, ClientRoom, RoomSnapshotMetadata>
type RoomParticipantReviverContract = TypeContract<never, LocalParticipant, ParticipantStubMetadata>
type RoomRemoteReviverContract = TypeContract<never, RemoteParticipant, RemoteParticipantMetadata>

const roomReviver: ReviverType<RoomReviverContract, InternalClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  revive(metadata, context) {
    const stub = context.createBroadcast({
      channelId: metadata.channelId,
      key: metadata.roomId,
      maxFrameBytes: metadata.maxFrameBytes,
    })
    return {
      value: new ClientRoom(stub, metadata),
      // Not methods: on Bun (JavaScriptCore) a method keeps this object alive, and the proxy is put on it (parse.ts).
      close: async () => {
        await stub.close()
      },
      abort: (abortError) => {
        stub.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}

const roomParticipantReviver: ReviverType<RoomParticipantReviverContract, InternalClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  revive(metadata, context) {
    const channel = context.createChannel({ channelId: metadata.channelId, maxFrameBytes: metadata.maxFrameBytes })
    return {
      value: new ClientStandaloneParticipant(channel, metadata),
      // Not methods: on Bun (JavaScriptCore) a method keeps this object alive, and the proxy is put on it (parse.ts).
      close: async () => {
        await channel.close()
      },
      abort: (abortError) => {
        channel.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}

/** Bound to the live `ClientRoom` the parser revived first, so `room.getParticipant(m.id) === m`, and sharing its
 *  lifecycle. */
const roomRemoteReviver: ReviverType<RoomRemoteReviverContract, InternalClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  revive(metadata, context) {
    assert(metadata.room instanceof ClientRoom)
    const room = metadata.room
    const remote = room._reviveRemote(metadata)
    // A call through the room's wrapper tethers the result to it, but a participant never owns its room's lifetime.
    untether(remote)
    context.shareLifecycle(remote, room)
    return {
      value: remote,
      close() {},
      abort() {},
    }
  },
}

const roomTypes = [roomReviver, roomParticipantReviver, roomRemoteReviver]
