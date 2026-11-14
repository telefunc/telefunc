export { roomReplacer, roomParticipantReplacer, roomRemoteReplacer }

import type { ReplacerType, ServerReplacerContext, TypeContract } from 'telefunc'
import { getOrCreate } from 'telefunc/__internal'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../shared/constants.js'
import { ServerLocalParticipant, ServerRoom, handleOf } from './room.js'
import type { RemoteParticipant } from '../types.js'
import type { ParticipantStubMetadata, RemoteParticipantMetadata, RoomSnapshotMetadata } from '../shared/protocol.js'
import { remoteBacking } from '../shared/state.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

type RoomReplacerContract = TypeContract<ServerRoom, never, RoomSnapshotMetadata>
type RoomParticipantReplacerContract = TypeContract<ServerLocalParticipant, never, ParticipantStubMetadata>
type RoomRemoteReplacerContract = TypeContract<RemoteParticipant, never, RemoteParticipantMetadata>

const RESPONSE_HIDDEN = Symbol.for('telefunc.RoomResponseHidden')

/** The hidden members a response hands its client, by room id: `Room.join(id)` and `Room.get(id)` build separate
 *  instances of one room. */
function responseHidden(context: ServerReplacerContext, roomId: string): Set<string> {
  const byRoom = context.responseState(RESPONSE_HIDDEN, () => new Map<string, Set<string>>())
  return getOrCreate(byRoom, roomId, () => new Set())
}

const roomReplacer: ReplacerType<RoomReplacerContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  detect(value): value is RoomReplacerContract['value'] {
    return ServerRoom.isServerRoom(value)
  },
  replace(serverRoom, context) {
    const end = context.createBroadcastChannel()
    const metadata = handleOf(serverRoom).openStub({
      end,
      publishShield: context.validators.get('data'),
      grantedHidden: responseHidden(context, serverRoom.id),
      page: context.page,
    })
    if (context.maxFrameBytes) metadata.maxFrameBytes = context.maxFrameBytes
    return {
      metadata,
      async close() {
        await end.close()
      },
      abort(abortError) {
        end.abort(abortError.abortValue)
      },
    }
  },
}

/** Rides its room's stub, so it has no lifecycle of its own; the recursive serializer replaces or dedupes the room it
 *  names. */
const roomRemoteReplacer: ReplacerType<RoomRemoteReplacerContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  detect(value): value is RoomRemoteReplacerContract['value'] {
    // A brand, not instanceof: a dev server can load two module graphs.
    return ServerRoom.isServerRoom(remoteBacking(value)?.state.view)
  },
  replace(remote, context) {
    const { state, entry } = remoteBacking(remote)!
    // Returning a hidden member hands it to this client: its room stub relays that member's events. One that left is
    // not handed: as any member its room's roster doesn't list, it revives on the page as one that left.
    const handedHidden = entry.hidden && !entry.left
    if (handedHidden) responseHidden(context, state.model.roomId).add(entry.id)
    const metadata: RemoteParticipantMetadata = {
      room: state.view,
      id: entry.id,
      meta: entry.meta.object,
      joinedAt: entry.joinedAt,
      metaSeq: entry.meta.seq,
      identity: entry.identity,
    }
    if (handedHidden) metadata.hidden = true
    if (!entry.meta.connected) metadata.disconnected = true
    return {
      metadata,
      close() {},
      abort() {},
    }
  },
}

const roomParticipantReplacer: ReplacerType<RoomParticipantReplacerContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  detect(value): value is RoomParticipantReplacerContract['value'] {
    return ServerLocalParticipant.isServerLocalParticipant(value)
  },
  replace(participant, context) {
    const end = context.createBroadcastChannel()
    const metadata = participant._openStub({
      end,
      publishShield: context.validators.get('data'),
      page: context.page,
    })
    if (context.maxFrameBytes) metadata.maxFrameBytes = context.maxFrameBytes
    return {
      metadata,
      async close() {
        await end.close()
      },
      abort(abortError) {
        end.abort(abortError.abortValue)
      },
    }
  },
}
