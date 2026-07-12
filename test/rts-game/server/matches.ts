export { createMatch, getMatchRoom, listMatches, startMatch, getPlayerTeam, MATCH_PREFIX, isAuthorityIdentity }

import { randomUUID } from 'node:crypto'
import { Room } from 'telefunc'
import { BLUE, RED, type Team } from '../shared/constants'
import type { ChatMsg, MatchListing, MatchMeta, MatchRoom, PlayerMeta } from '../shared/types'
import { Match } from './sim/match'

const MATCH_PREFIX = 'rts:match:'

// ── FINDING (server-owned per-room state + loop have no lifecycle home) ────────────────────────
// The Room API is request-driven (telefunctions) and event-driven (guards/hooks). There is no
// "room created / first player / room idle / room gone" lifecycle a match's authoritative object
// and 10 Hz `setInterval` can hang on. So the app keeps a global registry of live `Match`
// instances, starts/stops the loop by hand, and — because Vite runs *two* SSR module graphs and
// reloads them in dev — pins the registry on `globalThis` so a reload doesn't spawn a second tick
// loop for the same match. This is the Discord clone's finding 13 ("long-lived server state needs
// globalThis latches") escalated from a bot + a DB handle to a real-time simulation loop.
// ──────────────────────────────────────────────────────────────────────────────────────────────
function registry(): Map<string, Match> {
  const g = globalThis as { __rtsMatches?: Map<string, Match> }
  g.__rtsMatches ??= new Map()
  return g.__rtsMatches
}

function matchRoomId(matchId: string): string {
  return MATCH_PREFIX + matchId
}

function isAuthorityIdentity(identity: string | null): boolean {
  return identity !== null && identity.startsWith('authority:')
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
  return await Room.create<MatchMeta, PlayerMeta, ChatMsg>(roomId, { meta, size: 21 }) // 10v10 + authority
}

async function getMatchRoom(roomId: string): Promise<MatchRoom> {
  return await Room.get<MatchMeta, PlayerMeta, ChatMsg>(roomId)
}

/** The match browser. `Room.list` gives a point-in-time directory; there is no live "a match
 *  opened/closed" event, so the browser polls this (see README finding "No live room directory").
 *  `RoomInfo.meta` is untyped (`Room.list` predates the metadata generics), hence the cast. */
async function listMatches(): Promise<MatchListing[]> {
  const rooms = await Room.list({ prefix: MATCH_PREFIX })
  return rooms
    .map((r) => {
      const meta = r.meta as MatchMeta
      return { id: r.id, name: meta.name, phase: meta.phase, players: r.count, size: r.size - 1 }
    })
    .filter((m) => m.phase === 'lobby')
    .sort((a, b) => a.name.localeCompare(b.name))
}

async function startMatch(roomId: string, byIdentity: string): Promise<void> {
  if (registry().has(roomId)) return // already running (guards a double-start race)
  const room = await getMatchRoom(roomId)
  if (room.meta.hostId !== byIdentity) throw new Error('Only the host can start the match')
  if (room.meta.phase !== 'lobby') return

  // Snapshot the lobby's team choices server-side (trusted). `meta.team` is client-authored
  // display state, so it is read *here, once*, to build the authoritative identity→team map — it
  // is never trusted again mid-match (see commands.ts).
  const participants = await room.getParticipants()
  const teamPlayers = new Map<Team, string[]>([
    [RED, []],
    [BLUE, []],
  ])
  const seen = new Set<string>()
  for (const p of participants) {
    if (p.meta.authority || !p.identity || seen.has(p.identity)) continue
    seen.add(p.identity)
    const team: Team = p.meta.team === BLUE ? BLUE : RED
    teamPlayers.get(team)?.push(p.identity)
  }
  if ((teamPlayers.get(RED)?.length ?? 0) === 0 || (teamPlayers.get(BLUE)?.length ?? 0) === 0) {
    throw new Error('Both teams need at least one player')
  }

  const match = new Match(roomId, room)
  match.onDispose = () => {
    registry().delete(roomId)
    Room.close(roomId).catch(() => {})
  }
  registry().set(roomId, match)
  await match.start(teamPlayers)

  wireAbandonment(room, match, roomId)
}

/** End a match as a no-contest if every real player has left for good (a reload keeps the seat
 *  for `reconnectTimeout`, so a normal reconnect never trips this). */
function wireAbandonment(room: MatchRoom, match: Match, roomId: string): void {
  let timer: ReturnType<typeof setTimeout> | null = null
  const authorityIdentity = 'authority:' + roomId
  const realPlayers = (): number => {
    const ids = new Set<string>()
    for (const p of room.snapshot().participants)
      if (p.identity && p.identity !== authorityIdentity) ids.add(p.identity)
    return ids.size
  }
  const check = (): void => {
    if (realPlayers() === 0) {
      if (!timer) timer = setTimeout(() => match.abandon(), 30_000)
    } else if (timer) {
      clearTimeout(timer)
      timer = null
    }
  }
  room.onLeave(check)
  room.onJoin(check)
}
