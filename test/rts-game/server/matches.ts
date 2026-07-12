export { createMatch, getMatchRoom, listMatches, startMatch, getPlayerTeam, MATCH_PREFIX }

import { randomUUID } from 'node:crypto'
import { Room } from 'telefunc'
import { BLUE, RED, type Team } from '../shared/constants'
import type { ChatMsg, MatchListing, MatchMeta, MatchRoom, PlayerMeta } from '../shared/types'
import { Match } from './sim/match'

const MATCH_PREFIX = 'rts:match:'
const MATCH_SIZE = 20 // 10v10 — the server seat is not counted (excluded from presence)

// ── FINDING #7 (per-room sim loop lifecycle) — resolved as DOCS in 51b4613 ─────────────────────
// The Room API is request/event-driven, with no "room active/idle" hook to hang a 10 Hz
// `setInterval` + authoritative object on. That's by design: a per-room loop in a multi-node
// deployment needs leader election (which node runs it?), which a lifecycle hook can't provide —
// so the app owns it. Single-node, the idiomatic shape is this `globalThis` singleton registry
// (pinned so Vite's dual SSR graphs / dev reloads don't spawn a second loop), and the **hidden
// authority** now gives a real `onEmpty` to drive teardown (see wireAbandonment) instead of the
// hand-rolled real-player count round 1 needed. 51b4613 documents both.
function registry(): Map<string, Match> {
  const g = globalThis as { __rtsMatches?: Map<string, Match> }
  g.__rtsMatches ??= new Map()
  return g.__rtsMatches
}

function matchRoomId(matchId: string): string {
  return MATCH_PREFIX + matchId
}

/** The trusted team of a player in a running match (or null): lets a reconnecting client be
 *  rejoined onto its original team so it resubscribes to the correct fog track. */
function getPlayerTeam(roomId: string, identity: string): Team | null {
  return registry().get(roomId)?.teamOf(identity) ?? null
}

async function createMatch(name: string, hostId: string): Promise<MatchRoom> {
  const roomId = matchRoomId(randomUUID().slice(0, 8))
  const meta: MatchMeta = {
    name: name.slice(0, 28) || 'Skirmish',
    phase: 'lobby',
    hostId,
    mapSeed: (Math.random() * 1e9) | 0,
    winner: 0,
  }
  const room = await Room.create<MatchMeta, PlayerMeta, ChatMsg>(roomId, { meta, size: MATCH_SIZE })

  // Seat the hidden authority *now*, at room creation, so it's present before any client enters:
  // `onEnterMatch` hands each client the authority directly (`getParticipants({ hidden: true })`).
  // The simulation doesn't run until the host launches (`startMatch` → `match.begin`).
  const match = new Match(roomId, room)
  match.onDispose = () => {
    registry().delete(roomId)
    Room.close(roomId).catch(() => {})
  }
  registry().set(roomId, match)
  await match.takeSeat()
  wireAbandonment(room, match)
  return room
}

async function getMatchRoom(roomId: string): Promise<MatchRoom> {
  return await Room.get<MatchMeta, PlayerMeta, ChatMsg>(roomId)
}

/** The match browser. `Room.list` is a point-in-time directory — there's no live "a match
 *  opened/closed" event, so the browser polls this (finding 10b, working-as-designed: the lobby-
 *  room + announce pattern is the live shape). `Room.list<MatchMeta>()` types `meta` end-to-end
 *  now (finding 10a adopted in 51b4613 — the cast is gone). `count` excludes the server seat. */
async function listMatches(): Promise<MatchListing[]> {
  const rooms = await Room.list<MatchMeta>({ prefix: MATCH_PREFIX })
  return rooms
    .map((r) => ({ id: r.id, name: r.meta.name, phase: r.meta.phase, players: r.count, size: r.size }))
    .filter((m) => m.phase === 'lobby')
    .sort((a, b) => a.name.localeCompare(b.name))
}

async function startMatch(roomId: string, byIdentity: string): Promise<void> {
  const match = registry().get(roomId)
  if (!match) throw new Error('That match no longer exists')
  const room = await getMatchRoom(roomId)
  if (room.meta.hostId !== byIdentity) throw new Error('Only the host can start the match')
  if (room.meta.phase !== 'lobby' || match.isRunning) return // already launched (double-start guard)

  // Snapshot the lobby's team choices server-side (trusted). `meta.team` is client-authored
  // display state, so it is read *here, once*, to build the authoritative identity→team map — it
  // is never trusted again mid-match (see commands.ts). The roster is players only — the authority
  // is a hidden member, so `getParticipants()` (without `{ hidden: true }`) never includes it.
  const participants = await room.getParticipants()
  const teamPlayers = new Map<Team, string[]>([
    [RED, []],
    [BLUE, []],
  ])
  const seen = new Set<string>()
  for (const p of participants) {
    if (!p.identity || seen.has(p.identity)) continue
    seen.add(p.identity)
    const team: Team = p.meta.team === BLUE ? BLUE : RED
    teamPlayers.get(team)?.push(p.identity)
  }
  if ((teamPlayers.get(RED)?.length ?? 0) === 0 || (teamPlayers.get(BLUE)?.length ?? 0) === 0) {
    throw new Error('Both teams need at least one player')
  }

  await match.begin(teamPlayers)

  wireAbandonment(room, match)
}

/** Tear a match down once every player has left for good. The **hidden authority** is excluded from
 *  presence, so `onEmpty` fires exactly when the last *real* player leaves (round 1 had to count
 *  non-authority participants by hand). The grace also starts at creation, so a match created but
 *  never joined (host bailed) doesn't linger. A reload keeps the player's seat for `reconnectTimeout`,
 *  so a normal reconnect fires `onJoin` and cancels the grace before it abandons. */
function wireAbandonment(room: MatchRoom, match: Match): void {
  let timer: ReturnType<typeof setTimeout> | null = setTimeout(() => match.abandon(), 45_000)
  const arm = (): void => {
    if (!timer) timer = setTimeout(() => match.abandon(), 30_000)
  }
  const cancel = (): void => {
    if (timer) {
      clearTimeout(timer)
      timer = null
    }
  }
  room.onEmpty(arm)
  room.onJoin(cancel)
}
