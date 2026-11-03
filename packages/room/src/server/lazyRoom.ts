export { Room }

import type { ParticipantMeta, Room as RoomInstance, RoomMeta } from '../types.js'
import type { RoomStatic } from './roomStatics.js'
import { loadRoomServer, loadedRoomServer, type RoomServer } from './load.js'
import { assertUsage } from '../utils/assert.js'
import { ROOM_GUARD_USAGE } from './constants.js'

type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = RoomInstance<
  M,
  P,
  Pub
>

/** Room's statics that load its server code on their first call: every one but `guard`, which only a room it made
 *  can reach. */
const ROOM_STATICS = [
  'create',
  'get',
  'getOrCreate',
  'join',
  'list',
  'setMeta',
  'setAttributes',
  'close',
  'removeParticipant',
  'announce',
  'send',
  'getParticipants',
] as const satisfies readonly Exclude<keyof RoomStatic, 'guard'>[]
type LoadingStatic = (typeof ROOM_STATICS)[number]

/** `name` of Room's statics, loading Room's server code on the first call. */
function onLoad<K extends LoadingStatic>(name: K): RoomStatic[K] {
  const call = (server: RoomServer, args: unknown[]) =>
    (server.roomStatics[name] as (...args: unknown[]) => unknown)(...args)
  return ((...args: unknown[]) => {
    const server = loadedRoomServer()
    return server === undefined ? loadRoomServer().then((loaded) => call(loaded, args)) : call(server, args)
  }) as RoomStatic[K]
}

const loading = Object.fromEntries(ROOM_STATICS.map((name) => [name, onLoad(name)])) as {
  [K in LoadingStatic]: RoomStatic[K]
}

const Room: RoomStatic = {
  ...loading,
  guard(room, guards) {
    const server = loadedRoomServer()
    // Only Room's server code makes a room.
    assertUsage(server !== undefined, ROOM_GUARD_USAGE)
    server.roomStatics.guard(room, guards)
  },
}
