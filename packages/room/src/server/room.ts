export { ServerRoom, handleOf, roomOf }

import { assert } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { assertKnownOptions } from '../shared/arguments.js'
import { RoomView, type RoomRemotes } from '../shared/RoomView.js'
import type { RoomState } from '../shared/RoomState.js'
import type { JoinOptions, LocalParticipant, RemoteParticipant, Room } from '../types.js'
import { RoomHandle, type RoomHandleOptions } from './RoomHandle.js'
assertIsNotBrowser()

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/** The user's `Room` for one `Room.get()`: the public methods, over the handle that does the work, which Room's own
 *  code is given instead. */
class ServerRoom extends RoomView implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  private readonly handle: RoomHandle

  constructor(options: RoomHandleOptions) {
    const handle = new RoomHandle(options)
    super(handle.remotes)
    this.handle = handle
    roomHandles.set(this, handle)
    rooms.set(handle.state, this)
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  protected override get _read(): RoomRemotes {
    this.handle.replica.read()
    return this._remotes
  }

  join(options?: JoinOptions): Promise<LocalParticipant> {
    return this.handle.join(options)
  }

  getParticipants(options?: { hidden?: boolean }): RemoteParticipant[] {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    const remotes = this._read
    return options?.hidden ? remotes.hidden() : remotes.visible()
  }
}

// One map each for every module graph of the server, as a Room from one graph is a Room in another.
/** This file's state on `globalThis`. */
function createGlobalObject() {
  return {
    roomHandles: new WeakMap<ServerRoom, RoomHandle>(),
    /** The `Room` over each state, which the serializer finds a remote's room by. */
    rooms: new WeakMap<RoomState, ServerRoom>(),
  }
}
declare module '../utils/getGlobalObject.js' {
  interface GlobalObjects {
    'server/room.ts': ReturnType<typeof createGlobalObject>
  }
}
const { roomHandles, rooms } = getGlobalObject('server/room.ts', createGlobalObject)

/** The handle a user's `Room` is over, for Room's own code. */
function handleOf(room: ServerRoom): RoomHandle {
  const handle = roomHandles.get(room)
  assert(handle)
  return handle
}

/** The user's `Room` over `state`, if it is a server's. */
function roomOf(state: RoomState): ServerRoom | null {
  return rooms.get(state) ?? null
}
