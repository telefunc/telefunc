export { roomReviver, roomParticipantReviver, roomRemoteReviver }

import type { ClientReviverContext, ReviverType, TypeContract } from '../types.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import type { LocalParticipant, RemoteParticipant } from './types.js'
import type { ParticipantStubMetadata, RoomSnapshotMetadata } from './protocol.js'
import { roomCtrlKey } from './keys.js'
import { assert } from '../../utils/assert.js'

const ROOM_PREFIX = '!TelefuncRoom:'
const ROOM_PARTICIPANT_PREFIX = '!TelefuncRoomParticipant:'
const ROOM_REMOTE_PREFIX = '!TelefuncRoomRemoteParticipant:'
type RoomReviverContract = TypeContract<never, ClientRoom, RoomSnapshotMetadata>
type RoomParticipantReviverContract = TypeContract<never, LocalParticipant, ParticipantStubMetadata>
type RoomRemoteReviverContract = TypeContract<
  never,
  RemoteParticipant,
  {
    room: unknown
    id: string
    meta: Record<string, unknown>
    joinedAt: number
    metaSeq: number
    identity: string | null
    hidden?: boolean
  }
>

const roomReviver: ReviverType<RoomReviverContract, ClientReviverContext> = {
  prefix: ROOM_PREFIX,
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
const roomParticipantReviver: ReviverType<RoomParticipantReviverContract, ClientReviverContext> = {
  prefix: ROOM_PARTICIPANT_PREFIX,
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
const roomRemoteReviver: ReviverType<RoomRemoteReviverContract, ClientReviverContext> = {
  prefix: ROOM_REMOTE_PREFIX,
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
