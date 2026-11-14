export { heldMembers, roomViews, roomHolds, heldConnection, heldConnectionChanged }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import type { LocalParticipant } from '../types.js'
import type { ClientRoom } from './room.js'
import type { ClientParticipantBase } from './ClientParticipant.js'

const { heldMembers, roomViews, roomHolds } = getGlobalObject('client/room.ts', () => ({
  /** The members the page holds, joined through any of its room views or held through their own channel, by id. */
  heldMembers: new Map<string, ClientParticipantBase>(),
  /** The page's room views, each showing a member the page holds as the page's own connection to it. */
  roomViews: new Set<WeakRef<ClientRoom>>(),
  /** Each member handle's `join()` promise, which keeps a room a telefunction returned open while reachable. */
  roomHolds: new WeakMap<LocalParticipant, Promise<LocalParticipant>>(),
}))

function heldConnection(id: string): boolean | null {
  return heldMembers.get(id)?.isConnected ?? null
}

function heldConnectionChanged(id: string, wasConnected: boolean | null): void {
  for (const view of roomViews) {
    const room = view.deref()
    if (room) room._heldConnectionChanged(id, wasConnected)
    else roomViews.delete(view)
  }
}
