export { ServerRoom, handleOf }

import { assert } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { assertKnownOptions } from '../shared/arguments.js'
import { RoomState, RoomStateView } from '../shared/state.js'
import type { JoinOptions, LocalParticipant, RemoteParticipant, Room } from '../types.js'
import { RoomHandle, type RoomHandleOptions } from './RoomHandle.js'
assertIsNotBrowser()

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/** The user's `Room` for one `Room.get()`: the public methods, over the handle that does the work, which Room's own
 *  code is given instead. */
class ServerRoom extends RoomStateView implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  protected readonly _state: RoomState
  private readonly handle: RoomHandle

  constructor(options: Omit<RoomHandleOptions, 'view'>) {
    super()
    this.handle = new RoomHandle({ ...options, view: this })
    this._state = this.handle.state
    roomHandles.set(this, this.handle)
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  protected override get _read(): RoomState {
    this.handle.replica.read()
    return this._state
  }

  join(options?: JoinOptions): Promise<LocalParticipant> {
    return this.handle.join(options)
  }

  getParticipants(options?: { hidden?: boolean }): RemoteParticipant[] {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    const state = this._read
    return options?.hidden ? state.listHidden() : state.listVisible()
  }
}

// One map for every module graph of the server, as a Room from one graph is a Room in another.
const { roomHandles } = getGlobalObject('server/room.ts', () => ({
  roomHandles: new WeakMap<ServerRoom, RoomHandle>(),
}))

/** The handle a user's `Room` is over, for Room's own code. */
function handleOf(room: ServerRoom): RoomHandle {
  const handle = roomHandles.get(room)
  assert(handle)
  return handle
}
