export { ClientRoom }

import { assert, assertUsage } from '../utils/assert.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { assertKnownOptions } from '../shared/arguments.js'
import { RoomStateView, type RoomState } from '../shared/state.js'
import { ClientRoomHandle, type ClientRoomHandleOptions } from './ClientRoomHandle.js'
import type { JoinOptions, LocalParticipant, RemoteParticipant, Room } from '../types.js'

/** The user's `Room` for one room a telefunction returned: the public methods, over the handle that does the work,
 *  which Room's own code is given instead. */
class ClientRoom extends RoomStateView implements Room {
  /** @internal */
  readonly _state: RoomState

  constructor(options: Omit<ClientRoomHandleOptions, 'view'>) {
    super()
    const handle = new ClientRoomHandle({ ...options, view: this })
    this._state = handle.state
    roomHandles.set(this, handle)
  }

  join(options?: JoinOptions): Promise<LocalParticipant> {
    const joining: Promise<LocalParticipant> = handleOf(this)
      .join(options)
      .then((participant) => participant._handOut(joining))
    return joining
  }

  getParticipants(options?: { hidden?: boolean }): RemoteParticipant[] {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    assertUsage(!options?.hidden, 'Hidden participants can only be enumerated on the server')
    return this._read.listVisible()
  }
}

// One map for every module graph of the page, as a Room from one graph is a Room in another.
const { roomHandles } = getGlobalObject('client/room.ts', () => ({
  roomHandles: new WeakMap<ClientRoom, ClientRoomHandle>(),
}))

/** The handle a user's `Room` is over, for Room's own code. */
function handleOf(room: ClientRoom): ClientRoomHandle {
  const handle = roomHandles.get(room)
  assert(handle)
  return handle
}
