export { guardMatchRoom }

import { Room } from '@telefunc/room'
import { MATCH_SIZE, type MatchRoom } from '../shared/types'

// Server-side policy for a match room. Attached to every instance that hands out memberships.
//
// ── FINDING (guards are per-instance — carried over from the Discord clone, finding 3) ────────
// `Room.guard()` covers only memberships granted through *this* instance, and throws if called
// twice on one. So every telefunction that returns the room must funnel through this one helper,
// and any stray `Room.get()` that joins would silently bypass capacity. A room-scoped guard
// (`Room.guard(roomId, …)`), or a guard set at create/get time, would make the policy impossible
// to detach from the room. ────────────────────────────────────────────────────────────────────
//
// The round-1 anti-spoof check (rejecting a client-forged `meta.authority`) is gone: with the
// hidden authority (`join({ hidden: true })`, findings 1+2 adopted in 51b4613, generalized in
// da1b9f7) there is no marker flag to forge — it's a non-presence member, not a roster row a
// client could impersonate. A hidden join bypasses `onBeforeJoin` entirely, so this never sees it.
function guardMatchRoom(room: MatchRoom): void {
  Room.guard(room, {
    onBeforeJoin: () => {
      // Capacity: a room has no size limit of its own, so the app rejects the over-capacity join
      // itself. `count` excludes the hidden authority, so this is the true player count.
      if (room.count >= MATCH_SIZE) throw new Error('This match is full')
    },
  })
}
