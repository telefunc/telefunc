export { holdMember, letGoOfMember, heldConnection, heldConnectionChanged, addRoom, roomsOf, holdRoomOpen, releaseRoom }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import type { LocalParticipant } from '../types.js'
import type { ClientRoomHandle } from './ClientRoomHandle.js'

/** What the page as a whole holds of Room, once across its module graphs. */
/** This file's state on `globalThis`. */
function createGlobalObject() {
  return {
    /** The members the page holds, joined through any of its rooms or held through their own channel, by id. */
    heldMembers: new Map<string, LocalParticipant>(),
    /** The page's rooms, each showing a member the page holds as the page's own connection to it. */
    rooms: new Set<WeakRef<ClientRoomHandle>>(),
    /** Each member handle's `join()` promise, which keeps a room a telefunction returned open while reachable. */
    roomHolds: new WeakMap<LocalParticipant, Promise<LocalParticipant>>(),
  }
}
declare module '../utils/getGlobalObject.js' {
  interface GlobalObjects {
    'client/page.ts': ReturnType<typeof createGlobalObject>
  }
}
const { heldMembers, rooms, roomHolds } = getGlobalObject('client/page.ts', createGlobalObject)

/** The page holds `member` from now on, until it leaves, and its rooms show the page's own connection to it. */
function holdMember(member: LocalParticipant): void {
  heldMembers.set(member.id, member)
  heldConnectionChanged(member.id, null)
}

/** `member` left: the page holds it no longer, unless it already holds another of that id. */
function letGoOfMember(member: LocalParticipant): void {
  if (heldMembers.get(member.id) === member) heldMembers.delete(member.id)
}

function heldConnection(id: string): boolean | null {
  return heldMembers.get(id)?.isConnected ?? null
}

function heldConnectionChanged(id: string, wasConnected: boolean | null): void {
  for (const room of liveRooms()) room.heldConnectionChanged(id, wasConnected)
}

/** A room of the page, until the returned function removes it. */
function addRoom(room: ClientRoomHandle): () => void {
  const ref = new WeakRef(room)
  rooms.add(ref)
  return () => void rooms.delete(ref)
}

function* roomsOf(roomId: string): Generator<ClientRoomHandle> {
  for (const room of liveRooms()) if (room.id === roomId) yield room
}

/** The page's rooms still reachable; it forgets the others. */
function* liveRooms(): Generator<ClientRoomHandle> {
  for (const ref of rooms) {
    const room = ref.deref()
    if (room) yield room
    else rooms.delete(ref)
  }
}

function holdRoomOpen(handle: LocalParticipant, joining: Promise<LocalParticipant>): void {
  roomHolds.set(handle, joining)
}

function releaseRoom(handle: LocalParticipant): void {
  roomHolds.delete(handle)
}
