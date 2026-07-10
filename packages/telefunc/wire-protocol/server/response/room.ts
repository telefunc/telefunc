export { roomReplacer, roomParticipantReplacer, roomRemoteReplacer }

import type {
  ReplacerType,
  RoomContract,
  RoomParticipantContract,
  RoomRemoteContract,
  ServerReplacerContext,
} from '../../types.js'
import {
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
} from '../../constants.js'
import { ServerLocalParticipant, ServerRoom } from '../../room/server.js'
import { bindParticipantStubChannel, RoomStubChannel } from '../../room/stubs.js'
import { sizeToWire } from '../../room/protocol.js'
import { remoteBacking } from '../../room/state.js'
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
        stamp: serverRoom._state.updateStamp,
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

/** A `RemoteParticipant` view: serialized as (backing room, member snapshot). The room is a
 *  regular value inside the metadata — the recursive serializer replaces it (or dedupes it
 *  against a co-returned occurrence), so the client revives the view bound to the same live
 *  `ClientRoom`. The view has no lifecycle of its own — it rides the room's stub. */
const roomRemoteReplacer: ReplacerType<RoomRemoteContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM_REMOTE,
  detect(value): value is RoomRemoteContract['value'] {
    // Brand check, not instanceof — dev servers load two SSR module graphs, and a class from
    // one graph never instanceof-matches the other's. Brands (Symbol.for) span graphs.
    return ServerRoom.isServerRoom(remoteBacking(value)?.state._owner)
  },
  replace(remote, _context) {
    const { state, entry } = remoteBacking(remote)!
    return {
      // The entry survives the member's departure (the handle closes over it), so a serialize
      // racing a leave still ships a coherent snapshot — the client's roster then heals it.
      metadata: {
        room: state._owner,
        id: entry.id,
        meta: entry.meta,
        joinedAt: entry.joinedAt,
        metaSeq: entry.metaSeq,
      },
      close() {},
      abort() {},
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
