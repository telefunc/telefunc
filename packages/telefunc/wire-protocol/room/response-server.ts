export { roomReplacer, roomParticipantReplacer, roomRemoteReplacer }

import type { ReplacerType, ServerReplacerContext, TypeContract } from '../types.js'
import { ServerLocalParticipant, ServerRoom } from './server.js'
import type { RemoteParticipant } from './types.js'
import type { ParticipantStubMetadata, RoomSnapshotMetadata } from './protocol.js'
import {
  bindParticipantStubChannel,
  RoomParticipantStubChannel,
  RoomStubChannel,
  type ResponseRoomGrants,
} from './stubs.js'
import { remoteBacking } from './state.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()
const ROOM_PREFIX = '!TelefuncRoom:'
const ROOM_PARTICIPANT_PREFIX = '!TelefuncRoomParticipant:'
const ROOM_REMOTE_PREFIX = '!TelefuncRoomRemoteParticipant:'
type RoomReplacerContract = TypeContract<ServerRoom, never, RoomSnapshotMetadata>
type RoomParticipantReplacerContract = TypeContract<ServerLocalParticipant, never, ParticipantStubMetadata>
type RoomRemoteReplacerContract = TypeContract<
  RemoteParticipant,
  never,
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
/** Per-response grants shared by the Room replacers in one serializer pass. */
const ROOM_GRANTS = Symbol()
type RoomReplacerContext = ServerReplacerContext & {
  [ROOM_GRANTS]?: Map<string, ResponseRoomGrants>
}
/** Keyed by room id: `Room.join(id)` and `Room.get(id)` build separate instances of one room. */
function responseRoomGrants(context: ServerReplacerContext, room: ServerRoom): ResponseRoomGrants {
  const byRoom = ((context as RoomReplacerContext)[ROOM_GRANTS] ??= new Map())
  let grants = byRoom.get(room.id)
  if (!grants) byRoom.set(room.id, (grants = { selfSuppressed: new Set(), hidden: new Set() }))
  return grants
}
const roomReplacer: ReplacerType<RoomReplacerContract, ServerReplacerContext> = {
  prefix: ROOM_PREFIX,
  detect(value): value is RoomReplacerContract['value'] {
    return ServerRoom.isServerRoom(value)
  },
  replace(serverRoom, context) {
    const stub = new RoomStubChannel(serverRoom)
    context.registerChannel(stub)
    // The publish shield, auto-generated from the room's declared message type (`Pub`, see `RoomShield`), lives in `context.validators` under the `data` slot. Install it on the stub's dedicated
    // `_publishShield` — never its `_validators` map, which the base channel runs against every request envelope (join/leave/dm); the payload is shielded at the publish ingress (`_publishFromStub`).
    stub._publishShield = context.validators.get('data')
    // Adopt this response's grants for the room: co-returned self-suppressing members and returned hidden members (either serialization order) land in the same sets, read by the relay gates at source.
    stub._adoptResponseGrants(responseRoomGrants(context, serverRoom))
    // Attach before snapshotting: events from this point on are relayed to the client, earlier state is in the snapshot — overlaps are absorbed by idempotent application. In tail mode (`Room.get({
    // tail })`), attaching hands the pre-attach hold to the stub, which keeps it server-side until the client's first subscribe (see `ServerRoom._attachStub`).
    serverRoom._attachStub(stub)
    return {
      metadata: {
        channelId: stub.id,
        roomId: serverRoom.id,
        meta: serverRoom.meta,
        closed: serverRoom.isClosed,
        stamp: serverRoom._state.updateStamp,
        // Scalars only — the roster streams over the stub once its peer attaches, so serialization is O(1) in member count.
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
/** A `RemoteParticipant` view: serialized as (backing room, member snapshot). The room is a regular value inside the metadata — the recursive serializer replaces it (or dedupes it against a
 * co-returned occurrence), so the client revives the view bound to the same live `ClientRoom`. The view has no lifecycle of its own — it rides the room's stub.
 */
const roomRemoteReplacer: ReplacerType<RoomRemoteReplacerContract, ServerReplacerContext> = {
  prefix: ROOM_REMOTE_PREFIX,
  detect(value): value is RoomRemoteReplacerContract['value'] {
    // Brand check, not instanceof — dev servers load two SSR module graphs, and a class from one graph never instanceof-matches the other's. Brands (Symbol.for) span graphs.
    return ServerRoom.isServerRoom(remoteBacking(value)?.state._owner)
  },
  replace(remote, context) {
    const { state, entry } = remoteBacking(remote)!
    // Returning a hidden member hands it to this client: its room stub relays that member's events.
    if (entry.hidden) responseRoomGrants(context, state._owner as ServerRoom).hidden.add(entry.id)
    return {
      // The entry survives the member's departure (the handle closes over it), so a serialize racing a leave still ships a coherent snapshot — the client's roster then heals it.
      metadata: {
        room: state._owner,
        id: entry.id,
        meta: entry.meta,
        joinedAt: entry.joinedAt,
        metaSeq: entry.metaSeq,
        // App identity rides the snapshot: a directly-returned RemoteParticipant reports the trusted `identity` immediately, not `null`-until-roster (a reconcile won't re-stamp an already- revived
        // entry). The local-participant replacer does the same.
        identity: entry.identity,
        // The hidden flag rides the snapshot so a directly-returned hidden participant revives off-presence (unlike the roster, a reconcile won't re-flag an already-revived entry).
        ...(entry.hidden ? { hidden: true } : {}),
      },
      close() {},
      abort() {},
    }
  },
}
const roomParticipantReplacer: ReplacerType<RoomParticipantReplacerContract, ServerReplacerContext> = {
  prefix: ROOM_PARTICIPANT_PREFIX,
  detect(value): value is RoomParticipantReplacerContract['value'] {
    return ServerLocalParticipant.isServerLocalParticipant(value)
  },
  replace(participant, context) {
    const channel = new RoomParticipantStubChannel()
    context.registerChannel(channel)
    // Same publish shield as the room stub, for a standalone participant that publishes through its own channel (`req-publish`) rather than the room stub. The `data` verifier auto-generated from the
    // participant value's declared message type (see `RoomShield`) is handed straight to the binding, which runs it at the publish ingress — kept off `channel._validators` for the same reason as the
    // stub.
    bindParticipantStubChannel(channel, participant, context.validators.get('data'))
    // selfDelivery off: bind this member's id onto its room's stub drop-set for this response, so the server drops its echo at the source. If the room isn't co-returned there's no stub to adopt the
    // set and it's discarded with the pass — a clean no-op, never leaking to another client's stub.
    if (!participant.selfDelivery) responseRoomGrants(context, participant._room).selfSuppressed.add(participant.id)
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
