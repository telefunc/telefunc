export { roomReplacer, roomParticipantReplacer, roomRemoteReplacer }

import type { InternalServerReplacerContext, ReplacerType, TypeContract } from '../../types.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../../../constants.js'
import { ServerLocalParticipant, ServerRoom } from '../../../room/server/room.js'
import type { RemoteParticipant } from '../../../room/types.js'
import type {
  ParticipantStubMetadata,
  RemoteParticipantMetadata,
  RoomSnapshotMetadata,
} from '../../../room/protocol.js'
import { RoomParticipantStubChannel, ResponseRoomGrants } from '../../../room/server/stub.js'
import { remoteBacking } from '../../../room/state.js'
import { getAdapterMaxFrameBytes, getServerConfig } from '../../../../node/server/serverConfig.js'
import { assertIsNotBrowser } from '../../../../utils/assertIsNotBrowser.js'
import { getOrCreate } from '../../../../utils/getOrCreate.js'
assertIsNotBrowser()

type RoomReplacerContract = TypeContract<ServerRoom, never, RoomSnapshotMetadata>
type RoomParticipantReplacerContract = TypeContract<ServerLocalParticipant, never, ParticipantStubMetadata>
type RoomRemoteReplacerContract = TypeContract<RemoteParticipant, never, RemoteParticipantMetadata>

const ROOM_GRANTS = Symbol.for('telefunc.RoomResponseGrants')

/** Keyed by room id: `Room.join(id)` and `Room.get(id)` build separate instances of one room. */
function responseRoomGrants(context: InternalServerReplacerContext, roomId: string): ResponseRoomGrants {
  const byRoom = context.responseState(ROOM_GRANTS, () => new Map<string, ResponseRoomGrants>())
  return getOrCreate(byRoom, roomId, () => new ResponseRoomGrants())
}

const roomReplacer: ReplacerType<RoomReplacerContract, InternalServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  detect(value): value is RoomReplacerContract['value'] {
    return ServerRoom.isServerRoom(value)
  },
  replace(serverRoom, context) {
    const { stub, metadata } = serverRoom._openStub({
      publishShield: context.validators.get('data'),
      grants: responseRoomGrants(context, serverRoom.id),
    })
    context.registerChannel(stub)
    const maxFrameBytes = getAdapterMaxFrameBytes()
    if (maxFrameBytes) metadata.maxFrameBytes = maxFrameBytes
    return {
      metadata,
      async close() {
        await stub.close()
      },
      abort(abortError) {
        stub.abort(abortError.abortValue)
      },
    }
  },
}

/** Rides its room's stub, so it has no lifecycle of its own; the recursive serializer replaces or dedupes the room it
 *  names. */
const roomRemoteReplacer: ReplacerType<RoomRemoteReplacerContract, InternalServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  detect(value): value is RoomRemoteReplacerContract['value'] {
    // A brand, not instanceof: a dev server can load two module graphs.
    return ServerRoom.isServerRoom(remoteBacking(value)?.state._owner)
  },
  replace(remote, context) {
    const { state, entry } = remoteBacking(remote)!
    // Returning a hidden member hands it to this client: its room stub relays that member's events.
    if (entry.hidden) responseRoomGrants(context, state.roomId).hidden.add(entry.id)
    // The entry outlives a racing leave, so the snapshot stays coherent and the client's roster heals it.
    const metadata: RemoteParticipantMetadata = {
      room: state._owner,
      id: entry.id,
      meta: entry.meta.object,
      joinedAt: entry.joinedAt,
      metaSeq: entry.meta.seq,
      identity: entry.identity,
    }
    if (entry.hidden) metadata.hidden = true
    return {
      metadata,
      close() {},
      abort() {},
    }
  },
}

const roomParticipantReplacer: ReplacerType<RoomParticipantReplacerContract, InternalServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  detect(value): value is RoomParticipantReplacerContract['value'] {
    return ServerLocalParticipant.isServerLocalParticipant(value)
  },
  replace(participant, context) {
    const channel = new RoomParticipantStubChannel(participant, context.validators.get('data'))
    context.registerChannel(channel)
    // Its room's stub, if co-returned, drops this member's echo at the source; otherwise the grant goes unused.
    if (!participant.selfDelivery) responseRoomGrants(context, participant._room.id).suppress(participant.id)
    const maxFrameBytes = getAdapterMaxFrameBytes()
    const metadata: ParticipantStubMetadata = {
      channelId: channel.id,
      id: participant.id,
      meta: participant.meta,
      selfDelivery: participant.selfDelivery,
      identity: participant.identity,
      bufferLimit: getServerConfig().channel.bufferLimit,
    }
    if (maxFrameBytes) metadata.maxFrameBytes = maxFrameBytes
    return {
      metadata,
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
