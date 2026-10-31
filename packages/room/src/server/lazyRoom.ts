export { Room, ROOM_GUARD_USAGE }

import type { ParticipantMeta, Room as RoomInstance, RoomMeta } from '../types.js'
import type { RoomStatic } from './statics.js'
import { loadRoomServer, loadedRoomServer, type RoomServer } from './load.js'
import { assertUsage } from '../utils/assert.js'

type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = RoomInstance<
  M,
  P,
  Pub
>

const ROOM_GUARD_USAGE = 'Room.guard() expects a room obtained from Room.get()/Room.create()'

/** `name` of Room's statics, loading Room's server code on the first call. */
function onLoad<K extends Exclude<keyof RoomStatic, 'guard'>>(name: K): RoomStatic[K] {
  const call = (server: RoomServer, args: unknown[]) => (server.Room[name] as (...args: unknown[]) => unknown)(...args)
  return ((...args: unknown[]) => {
    const server = loadedRoomServer()
    return server === undefined ? loadRoomServer().then((loaded) => call(loaded, args)) : call(server, args)
  }) as RoomStatic[K]
}

const Room: RoomStatic = {
  create: onLoad('create'),
  get: onLoad('get'),
  getOrCreate: onLoad('getOrCreate'),
  guard(room, guards) {
    const server = loadedRoomServer()
    // Only Room's server code makes a room.
    assertUsage(server !== undefined, ROOM_GUARD_USAGE)
    server.Room.guard(room, guards)
  },
  join: onLoad('join'),
  list: onLoad('list'),
  setMeta: onLoad('setMeta'),
  setAttributes: onLoad('setAttributes'),
  close: onLoad('close'),
  removeParticipant: onLoad('removeParticipant'),
  announce: onLoad('announce'),
  send: onLoad('send'),
  getParticipants: onLoad('getParticipants'),
}
