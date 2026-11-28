export { Room }

import type {
  JoinOptions,
  ParticipantMeta,
  ParticipantRef,
  Room as RoomInstance,
  RoomGetOptions,
  RoomMeta,
  RoomOptions,
  RoomSendReceipt,
} from '../types.js'
import type { RoomStatic } from './roomStatics.js'
import { loadRoomServer, loadedRoomServer, type RoomServer } from './load.js'
import { assertUsage } from '../utils/assert.js'
import { ROOM_GUARD_USAGE } from './constants.js'

type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = RoomInstance<
  M,
  P,
  Pub
>

/** Calls Room's server code: at once where it is loaded, else once it is. */
function withServer<T>(call: (server: RoomServer) => Promise<T>): Promise<T> {
  const server = loadedRoomServer()
  return server === undefined ? loadRoomServer().then(call) : call(server)
}

/** Calls one of Room's server statics, as `withServer()`. */
function withStatics<T>(call: (statics: RoomStatic) => Promise<T>): Promise<T> {
  return withServer(({ roomStatics }) => call(roomStatics))
}

function send(id: string, target: { id: string }, data: unknown): Promise<RoomSendReceipt>
function send(id: string, target: { identity: string }, data: unknown): Promise<RoomSendReceipt[]>
function send(id: string, target: ParticipantRef, data: unknown): Promise<RoomSendReceipt | RoomSendReceipt[]> {
  return withServer((server) => server.sendToParticipantRef(id, target, data))
}

/** Room's statics, each loading Room's server code on its first call; `guard`, which only a room that code made can
 *  reach, finds it loaded. */
const Room: RoomStatic = {
  create<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomOptions<M>,
  ) {
    return withStatics((statics) => statics.create<M, P, Pub>(id, options))
  },
  get<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomGetOptions,
  ) {
    return withStatics((statics) => statics.get<M, P, Pub>(id, options))
  },
  getOrCreate<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomOptions<M> & RoomGetOptions,
  ) {
    return withStatics((statics) => statics.getOrCreate<M, P, Pub>(id, options))
  },
  join<P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(id: string, options?: JoinOptions<P>) {
    return withStatics((statics) => statics.join<P, Pub>(id, options))
  },
  list<M extends RoomMeta = RoomMeta>(options?: { prefix?: string }) {
    return withStatics((statics) => statics.list<M>(options))
  },
  setMeta: (id, meta) => withStatics((statics) => statics.setMeta(id, meta)),
  updateMeta: (id, meta) => withStatics((statics) => statics.updateMeta(id, meta)),
  close: (id) => withStatics((statics) => statics.close(id)),
  removeParticipant: (id, target: ParticipantRef & { reason?: unknown }) =>
    withStatics((statics) => statics.removeParticipant(id, target)),
  publish: (id, data) => withStatics((statics) => statics.publish(id, data)),
  send,
  getParticipants<P extends ParticipantMeta = ParticipantMeta>(id: string, target?: { identity: string }) {
    return withStatics((statics) => statics.getParticipants<P>(id, target))
  },
  guard(room, guards) {
    const server = loadedRoomServer()
    // Only Room's server code makes a room.
    assertUsage(server !== undefined, ROOM_GUARD_USAGE)
    server.roomStatics.guard(room, guards)
  },
}
