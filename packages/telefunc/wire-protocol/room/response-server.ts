export { roomReplacer, roomParticipantReplacer, roomRemoteReplacer }

import type { InternalServerReplacerContext, ReplacerType, TypeContract } from '../types.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../constants.js'
import { ServerLocalParticipant, ServerRoom } from './server/room.js'
import type { RemoteParticipant } from './types.js'
import type { ParticipantStubMetadata, RemoteParticipantMetadata, RoomSnapshotMetadata } from './protocol.js'
import { RoomParticipantStubChannel, type ResponseRoomGrants } from './server/stub.js'
import { remoteBacking } from './state.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

type RoomReplacerContract = TypeContract<ServerRoom, never, RoomSnapshotMetadata>
type RoomParticipantReplacerContract = TypeContract<ServerLocalParticipant, never, ParticipantStubMetadata>
type RoomRemoteReplacerContract = TypeContract<RemoteParticipant, never, RemoteParticipantMetadata>

const ROOM_GRANTS = Symbol('telefunc.RoomResponseGrants')
/** Keyed by room id: `Room.join(id)` and `Room.get(id)` build separate instances of one room. */
function responseRoomGrants(context: InternalServerReplacerContext, roomId: string): ResponseRoomGrants {
  const byRoom = context.responseState(ROOM_GRANTS, () => new Map<string, ResponseRoomGrants>())
  let grants = byRoom.get(roomId)
  if (!grants) byRoom.set(roomId, (grants = { selfSuppressed: new Set(), hidden: new Set() }))
  return grants
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
/** Rides its room's stub, so it has no lifecycle of its own; the recursive serializer replaces or dedupes the room it names. */
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
    return {
      // The entry outlives a racing leave, so the snapshot stays coherent and the client's roster heals it.
      metadata: {
        room: state._owner,
        id: entry.id,
        meta: entry.meta,
        joinedAt: entry.joinedAt,
        metaSeq: entry.metaSeq,
        identity: entry.identity,
        ...(entry.hidden ? { hidden: true } : {}),
      },
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
    if (!participant.selfDelivery) responseRoomGrants(context, participant._room.id).selfSuppressed.add(participant.id)
    return {
      metadata: {
        channelId: channel.id,
        id: participant.id,
        meta: participant.meta,
        selfDelivery: participant.selfDelivery,
        identity: participant.identity,
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
