export { roomReviver, roomParticipantReviver, roomRemoteReviver }

import type { ClientReviverContext, ReviverType, TypeContract } from '../types.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import type { LocalParticipant, RemoteParticipant } from './types.js'
import type { ParticipantStubMetadata, RemoteParticipantMetadata, RoomSnapshotMetadata } from './protocol.js'
import { assert } from '../../utils/assert.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../constants.js'

type RoomReviverContract = TypeContract<never, ClientRoom, RoomSnapshotMetadata>
type RoomParticipantReviverContract = TypeContract<never, LocalParticipant, ParticipantStubMetadata>
type RoomRemoteReviverContract = TypeContract<never, RemoteParticipant, RemoteParticipantMetadata>

const roomReviver: ReviverType<RoomReviverContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  revive(metadata, context) {
    const stub = context.createBroadcast({
      channelId: metadata.channelId,
      key: metadata.roomId,
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
const roomParticipantReviver: ReviverType<RoomParticipantReviverContract, ClientReviverContext> = {
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
/** Bound to the live `ClientRoom` the parser revived first, so `room.getParticipant(m.id) === m`, and sharing its lifecycle. */
const roomRemoteReviver: ReviverType<RoomRemoteReviverContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  revive(metadata, context) {
    assert(metadata.room instanceof ClientRoom)
    const room = metadata.room
    const remote = room._reviveRemote(metadata)
    context.shareLifecycle(remote, room)
    return {
      value: remote,
      close() {},
      abort() {},
    }
  },
}
