export { guardMatchRoom }

import { Room } from 'telefunc'
import type { MatchRoom } from '../shared/types'
import { isAuthorityIdentity } from './matches'

// Server-side policy for a match room. Attached to every instance that hands out memberships.
//
// ── FINDING (guards are per-instance — carried over from the Discord clone, finding 3) ────────
// `Room.guard()` covers only memberships granted through *this* instance, and throws if called
// twice on one. So every telefunction that returns the room must funnel through this one helper,
// and any stray `Room.get()` that joins would silently bypass capacity + the anti-spoof check.
// A room-scoped guard (`Room.guard(roomId, …)`), or a guard set at create/get time, would make
// the policy impossible to detach from the room. ────────────────────────────────────────────────
function guardMatchRoom(room: MatchRoom): void {
  Room.guard(room, {
    onBeforeJoin: (member) => {
      // Capacity: `size` is only a hint (Telefunc tracks it but never enforces it), so the app
      // rejects the over-capacity join itself.
      if (room.count >= room.size) throw new Error('This match is full')
      // Anti-spoof: only the server's own authority join (server-stamped `authority:` identity)
      // may carry `meta.authority`. A client cannot set `identity`, so it can never forge it —
      // this stops a client from hiding itself from every roster by faking the flag.
      if (member.meta.authority && !isAuthorityIdentity(member.identity)) {
        throw new Error('Reserved metadata')
      }
    },
  })
}
