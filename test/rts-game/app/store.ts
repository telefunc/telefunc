// The client store (zustand): all Room wiring on one side, plain state for React on the other.
//
// Live handles (the room, my participant) live in module variables, never in the
// store — and every subscription is set up from user events (sign-in, join, start), never from
// render, so nothing double-subscribes. The member list is `snapshot({ by: 'identity' })`, so two
// tabs of one player collapse to one lobby row; the match's high-frequency world does NOT flow
// through here — it rides the binary lane into `net`/`engine` (see app/net.ts).

export type { LobbyPlayer, AppPhase }
export {
  useApp,
  getState,
  bootOnce,
  signIn,
  signOut,
  refreshMatches,
  createMatch,
  joinMatch,
  leaveMatch,
  setTeam,
  toggleReady,
  startMatch,
  sendChat,
  setHud,
  setSelection,
  getMyTeam,
  showToast,
}

import type { LocalParticipant, RoomIdentitySnapshotView, RoomSnapshotView } from 'telefunc'
import { create } from 'zustand'
import { BLUE, NEUTRAL, RED, type Team } from '../shared/constants'
import type { ChatMsg, MatchAuthority, MatchListing, MatchMeta, MatchRoom, PlayerMeta } from '../shared/types'
import type { HudSnapshot } from './engine/game'
import type { SelectionInfo } from './engine/input'
import { startStream, stopStream } from './net'
import { onCreateMatch, onListMatches, onWhoAmI } from '../telefunc/lobby.telefunc'
import { onEnterMatch, onStartMatch } from '../telefunc/match.telefunc'
import type { SessionUser } from '../server/identity'

type AppPhase = 'boot' | 'auth' | 'home' | 'lobby' | 'match'
type LobbyPlayer = { identity: string; name: string; color: string; team: Team; ready: boolean }

type AppState = {
  phase: AppPhase
  user: SessionUser | null
  authPending: boolean
  authError: string | null
  matches: MatchListing[]
  matchMeta: MatchMeta | null
  roster: LobbyPlayer[]
  myTeam: Team
  ready: boolean
  isHost: boolean
  chat: ChatMsg[]
  banner: string | null
  result: { winner: Team } | null
  hud: HudSnapshot | null
  selection: SelectionInfo | null
  toast: string | null
}

const initial: AppState = {
  phase: 'boot',
  user: null,
  authPending: false,
  authError: null,
  matches: [],
  matchMeta: null,
  roster: [],
  myTeam: NEUTRAL,
  ready: false,
  isHost: false,
  chat: [],
  banner: null,
  result: null,
  hud: null,
  selection: null,
  toast: null,
}

const useApp = create<AppState>(() => initial)
const setState = useApp.setState
const getState = useApp.getState

// ── live handles (kept out of the store) ────────────────────────────────────────────────────
let room: MatchRoom | null = null
let me: LocalParticipant<PlayerMeta> | null = null
let authority: MatchAuthority | null = null // the room's hidden sim participant, from onEnterMatch
let unsubs: (() => void)[] = []
let streaming = false
let pollTimer: ReturnType<typeof setInterval> | null = null
let toastTimer: ReturnType<typeof setTimeout> | null = null

function getMyTeam(): Team {
  return getState().myTeam
}

async function bootOnce(): Promise<void> {
  const user = await onWhoAmI().catch(() => null)
  if (user) {
    setState({ user, phase: 'home' })
    startPolling()
  } else {
    setState({ phase: 'auth' })
  }
}

async function signIn(name: string): Promise<void> {
  setState({ authPending: true, authError: null })
  try {
    const res = await fetch('/api/session', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    })
    const body = (await res.json()) as { user?: SessionUser; error?: string }
    if (!res.ok || !body.user) throw new Error(body.error || 'Could not sign in')
    setState({ user: body.user, phase: 'home', authPending: false })
    startPolling()
  } catch (err) {
    setState({ authPending: false, authError: err instanceof Error ? err.message : 'Could not sign in' })
  }
}

async function signOut(): Promise<void> {
  await leaveMatch()
  await fetch('/api/logout', { method: 'POST' }).catch(() => {})
  stopPolling()
  setState({ ...initial, phase: 'auth' })
}

// ── home: the match browser (polled — `Room.list` has no live directory event) ────────────────
function startPolling(): void {
  refreshMatches()
  stopPolling()
  pollTimer = setInterval(() => {
    if (getState().phase === 'home') refreshMatches()
  }, 1600)
}
function stopPolling(): void {
  if (pollTimer) clearInterval(pollTimer)
  pollTimer = null
}
async function refreshMatches(): Promise<void> {
  const matches = await onListMatches().catch(() => [])
  setState({ matches })
}

async function createMatch(name: string): Promise<void> {
  try {
    const { roomId } = await onCreateMatch(name)
    await joinMatch(roomId)
  } catch (err) {
    showToast(abortMessage(err) ?? 'Could not create match')
  }
}

async function joinMatch(roomId: string): Promise<void> {
  if (room) await leaveMatch()
  let res: { room: MatchRoom; me: LocalParticipant<PlayerMeta>; authority: MatchAuthority }
  try {
    res = await onEnterMatch(roomId)
  } catch (err) {
    showToast(abortMessage(err) ?? 'Could not join match')
    return
  }
  room = res.room
  me = res.me
  authority = res.authority
  streaming = false
  const meta = room.meta
  setState({
    matchMeta: meta,
    myTeam: me.meta.team,
    ready: me.meta.ready,
    isHost: meta.hostId === getState().user?.id,
    chat: [],
    banner: null,
    result: null,
    hud: null,
    selection: null,
    phase: meta.phase === 'playing' ? 'match' : 'lobby',
  })
  wireRoom(room, me)
  syncRoom()
}

function wireRoom(r: MatchRoom, self: LocalParticipant<PlayerMeta>): void {
  unsubs.push(
    r.subscribe((data) => {
      if (data && data.k === 'chat') setState((s) => ({ chat: [...s.chat, data].slice(-120) }))
    }),
  )
  unsubs.push(r.onChange(() => syncRoom()))
  unsubs.push(
    r.onAnnounce((data) => {
      const a = data as { k: string; winner?: Team; name?: string; team?: Team }
      if (a.k === 'match-end') setState({ result: { winner: (a.winner ?? NEUTRAL) as Team } })
    }),
  )
  unsubs.push(r.onClose(() => backToHome('The match has closed')))
  unsubs.push(
    self.onLeave((cause) => {
      if (cause.type === 'removed') backToHome('You were removed from the match')
      else if (cause.type === 'disconnected') showToast('Connection lost — reconnecting…')
    }),
  )
}

/** Reconcile store state from the room on any change: roster, phase, and — when the match starts —
 *  discovering the server seat and opening the binary stream. */
function syncRoom(): void {
  if (!room || !me) return
  const snap = room.snapshot() as RoomSnapshotView<MatchMeta, PlayerMeta>
  const meta = snap.meta
  const roster = rosterFromSnapshot()
  setState({ matchMeta: meta, roster })

  if (meta.phase === 'ended' && meta.winner) {
    setState((s) => ({ result: s.result ?? { winner: meta.winner } }))
  }

  if (meta.phase === 'playing' && !streaming) beginStream()
}

/** Collapse tabs to one row per identity. `RoomIdentitySnapshotView` / `IdentityGroupView` are now
 *  exported from the public barrel (finding 9 adopted in 51b4613), so the view is named directly —
 *  no structural derivation. The server seat isn't in the roster, so there's nothing to filter. */
function rosterFromSnapshot(): LobbyPlayer[] {
  if (!room) return []
  const view: RoomIdentitySnapshotView<MatchMeta, PlayerMeta> = room.snapshot({ by: 'identity' })
  const out: LobbyPlayer[] = []
  for (const g of view.identities) {
    const p = g.participants[0]
    if (!p || g.identity === null) continue
    out.push({ identity: g.identity, name: p.meta.name, color: p.meta.color, team: p.meta.team, ready: p.meta.ready })
  }
  return out.sort((a, b) => a.team - b.team || a.name.localeCompare(b.name))
}

/** Open the binary stream once the match is live. The server's simulation is the room's **hidden
 *  authority**, handed to us by `onEnterMatch` (finding 1+2 adopted) — no roster scan, no
 *  `room.server`, no `getParticipant()` round-trip, and no "wait for the seat to appear" retry: the
 *  authority is seated before anyone joins, so we hold it from the moment we entered. */
function beginStream(): void {
  if (!room || !me || !authority || streaming) return
  streaming = true
  setState({ myTeam: me.meta.team, phase: 'match' })
  startStream(authority, me, me.meta.team)
}

async function setTeam(team: Team): Promise<void> {
  if (!me || getState().matchMeta?.phase !== 'lobby') return
  setState({ myTeam: team })
  await me.setAttributes({ team }).catch(() => {})
}

async function toggleReady(): Promise<void> {
  if (!me) return
  const ready = !getState().ready
  setState({ ready })
  await me.setAttributes({ ready }).catch(() => {})
}

async function startMatch(): Promise<void> {
  if (!room) return
  try {
    await onStartMatch(room.id)
  } catch (err) {
    showToast(abortMessage(err) ?? 'Could not start')
  }
}

async function sendChat(text: string): Promise<void> {
  const t = text.trim()
  const user = getState().user
  if (!me || !t || !user) return
  const msg: ChatMsg = {
    k: 'chat',
    from: user.name,
    color: user.color,
    team: getState().myTeam,
    text: t.slice(0, 160),
    at: Date.now(),
  }
  await me.publish(msg).catch(() => {})
}

async function leaveMatch(): Promise<void> {
  stopStream()
  streaming = false
  for (const off of unsubs.splice(0)) off()
  const leaving = me
  room = null
  me = null
  authority = null
  if (leaving) await leaving.leave().catch(() => {})
  setState({
    phase: getState().user ? 'home' : 'auth',
    matchMeta: null,
    roster: [],
    chat: [],
    result: null,
    hud: null,
    selection: null,
    banner: null,
  })
  startPolling()
}

function backToHome(msg: string): void {
  showToast(msg)
  void leaveMatch()
}

function setHud(hud: HudSnapshot): void {
  setState({ hud, selection: hud.selection })
  if (hud.winner && !getState().result) setState({ result: { winner: hud.winner } })
}

function setSelection(selection: SelectionInfo): void {
  setState({ selection })
}

function showToast(msg: string): void {
  setState({ toast: msg })
  if (toastTimer) clearTimeout(toastTimer)
  toastTimer = setTimeout(() => setState({ toast: null }), 3200)
}

function abortMessage(err: unknown): string | null {
  const v = (err as { abortValue?: { error?: string } })?.abortValue
  return v?.error ?? null
}

export { RED, BLUE, NEUTRAL }
