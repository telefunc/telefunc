// Types that cross the wire: room/participant metadata, the typed `Room` aliases, and the
// client→authority command protocol.

import type { RemoteParticipant, Room } from 'telefunc'
import type { Team } from './constants'

export type MatchPhase = 'lobby' | 'playing' | 'ended'

/** Room metadata for a match room. One room serves a match's whole life — lobby, play, result —
 *  so the phase lives here and every observer (browser list, joined players) reads it live. */
export type MatchMeta = {
  name: string
  phase: MatchPhase
  hostId: string // identity of the player who created the match (may start it)
  mapSeed: number
  winner: Team // 0 while playing; set when phase === 'ended'
}

/** Participant metadata. The server's simulation seat is a *hidden* participant (it joins with
 *  `{ hidden: true }`, excluded from presence), so no marker flag is needed here — the roster is
 *  players only. The seat reuses this shape (a display name/color) purely for convenience. */
export type PlayerMeta = {
  name: string
  color: string
  team: Team
  ready: boolean
}

/** Room-wide chat, delivered over `publish()`/`subscribe()` and typed via the room's third
 *  generic. All-chat only: a team-scoped variant would leak on a room-wide publish (see README
 *  finding "No audience-scoped publish"). */
export type ChatMsg = {
  k: 'chat'
  from: string
  color: string
  team: Team
  text: string
  at: number
}

/** Room-authored announcements (`Room.announce()`), for transient banners. */
export type Announce =
  | { k: 'match-start' }
  | { k: 'match-end'; winner: Team }
  | { k: 'eliminated'; team: Team; name: string }

export type MatchRoom = Room<MatchMeta, PlayerMeta, ChatMsg>

/** The match **authority**: the room's hidden `{ hidden: true }` participant that runs the sim,
 *  handed to the client by `onEnterMatch`. The client subscribes to its binary tracks and DMs it
 *  orders — no `room.server`, no roster scan. */
export type MatchAuthority = RemoteParticipant<PlayerMeta, ChatMsg>

// ── Commands: client → authority ─────────────────────────────────────────────────────────────
// Issued by selecting units and right-clicking / pressing build hotkeys. Delivered with
// `me.send(authority.id, command)` to the room's hidden authority (`join({ hidden: true })`), which
// `listen()`s for them server-side — see app/net.ts and server/sim/match.ts. (Round-2 adoption of
// the seat closed the "no first-class client→server lane" finding; `send()` now returns a receipt.)
export type Command =
  | { t: 'move'; ids: number[]; x: number; y: number }
  | { t: 'attackMove'; ids: number[]; x: number; y: number }
  | { t: 'attack'; ids: number[]; target: number }
  | { t: 'gather'; ids: number[]; target: number }
  | { t: 'build'; worker: number; building: number; x: number; y: number }
  | { t: 'produce'; building: number; unit: number }
  | { t: 'cancel'; building: number } // cancel the front of a building's production queue
  | { t: 'rally'; building: number; x: number; y: number }
  | { t: 'stop'; ids: number[] }
  | { t: 'resign' }

/** What the match-browser row needs (from `Room.list`). */
export type MatchListing = {
  id: string
  name: string
  phase: MatchPhase
  players: number
  size: number
}
