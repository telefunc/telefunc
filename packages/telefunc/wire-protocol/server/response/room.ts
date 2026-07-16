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
import { remoteBacking } from '../../room/state.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

/** Per-response echo-suppression rendezvous (`selfDelivery: false`). Both a room and a co-returned
 *  self-suppressing participant are serialized into the same response, in arbitrary key order — so
 *  the drop-set of member ids lives in `context.passScope`, one shared `Set` per room instance. The
 *  participant adds its id; the room's stub adopts the very same set by reference. Order-independent
 *  by object identity, scoped to this one response, discarded when the pass ends. */
const ROOM_SELF_SUPPRESS = Symbol('telefunc:roomSelfSuppress')
function roomSelfSuppressSet(context: ServerReplacerContext, room: ServerRoom): Set<string> {
  let byRoom = context.passScope.get(ROOM_SELF_SUPPRESS) as Map<ServerRoom, Set<string>> | undefined
  if (!byRoom) context.passScope.set(ROOM_SELF_SUPPRESS, (byRoom = new Map()))
  let set = byRoom.get(room)
  if (!set) byRoom.set(room, (set = new Set()))
  return set
}

const roomReplacer: ReplacerType<RoomContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_ROOM,
  detect(value): value is RoomContract['value'] {
    return ServerRoom.isServerRoom(value)
  },
  replace(serverRoom, context) {
    const stub = new RoomStubChannel(serverRoom)
    context.registerChannel(stub)
    // Adopt this response's echo drop-set for the room: any co-returned self-suppressing member
    // (either serialization order) lands in the same set, and the relay gate reads it at source.
    stub._adoptSelfSuppressed(roomSelfSuppressSet(context, serverRoom))
    // Attach before snapshotting: events from this point on are relayed to the client,
    // earlier state is in the snapshot — overlaps are absorbed by idempotent application.
    serverRoom._attachStub(stub)
    return {
      metadata: {
        channelId: stub.id,
        roomId: serverRoom.id,
        meta: serverRoom.meta,
        isolated: serverRoom._isolated,
        closed: serverRoom.isClosed,
        stamp: serverRoom._state.updateStamp,
        // Scalars only — the roster streams over the stub once its peer attaches, so
        // serialization is O(1) in member count.
        count: serverRoom.count,
        // Tail mode: the client holds relayed text until its first subscribe() (see `_tailHold`).
        ...(serverRoom._tail ? { tail: true } : {}),
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
        // App identity rides the snapshot: a directly-returned RemoteParticipant reports the trusted
        // `identity` immediately, not `null`-until-roster (a reconcile won't re-stamp an already-
        // revived entry). The local-participant replacer does the same.
        identity: entry.identity,
        // The hidden flag rides the snapshot so a directly-returned hidden participant revives
        // off-presence (unlike the roster, a reconcile won't re-flag an already-revived entry).
        ...(entry.hidden ? { hidden: true } : {}),
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
    // selfDelivery off: bind this member's id onto its room's stub drop-set for this response, so the
    // server drops its echo at the source. If the room isn't co-returned there's no stub to adopt the
    // set and it's discarded with the pass — a clean no-op, never leaking to another client's stub.
    if (!participant.selfDelivery) roomSelfSuppressSet(context, participant._room).add(participant.id)
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
