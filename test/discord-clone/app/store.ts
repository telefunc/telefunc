// The client store (zustand): all Room wiring on one side, immutable snapshots for React on
// the other. Rooms and participants are long-lived *mutable* event emitters (`member.meta` is
// replaced in place), while React wants immutable state — this file is the adapter every
// React-on-Room app ends up writing (see README finding on the React impedance).
//
// Live handles (rooms, participants) live in module variables, never in the store.
// All room wiring runs from user events (login, clicks) — none from render — which also keeps
// `<React.StrictMode>`'s double-invoked effects from double-joining anything.

export type { AppState, CallState, ChannelSnapshot, DmThread, Member, View }
export {
  useApp,
  getState,
  bootOnce,
  registerUser,
  loginUser,
  logout,
  openChannel,
  loadOlderMessages,
  openCall,
  openGuild,
  sendMessage,
  sendTyping,
  createChannel,
  setTopic,
  deleteChannel,
  openDmHome,
  openDm,
  sendDm,
  setStatus,
  kickUser,
  announce,
  showToast,
  getChannelRoom,
  getIdentity,
  patchCall,
}

import type { LocalParticipant, RemoteParticipant, Room } from 'telefunc'
import { create } from 'zustand'
import {
  asChannelMeta,
  asMemberMeta,
  type ChannelPublish,
  type ChatMessage,
  type DmMessage,
  type GuildAnnouncement,
  type SystemNotice,
} from '../shared/types'
import { onAnnounce, onKickUser } from '../telefunc/admin.telefunc'
import {
  onChannelHistory,
  onCreateChannel,
  onDeleteChannel,
  onGetChannel,
  onSetTopic,
} from '../telefunc/channels.telefunc'
import { onDmThread, onListDmThreads, onSendDm } from '../telefunc/dms.telefunc'
import { onEnterGuild } from '../telefunc/session.telefunc'

// ---------------------------------------------------------------------------
// State (immutable snapshots — what React renders)
// ---------------------------------------------------------------------------

/** A room member, snapshotted for rendering. `userId` is the durable identity; `participantId`
 *  identifies one connection in one room (a user with two tabs has two participants). */
type Member = {
  userId: string
  participantId: string
  name: string
  color: string
  status: 'online' | 'idle' | 'dnd'
  bot: boolean
  admin: boolean
  // Call state (voice-room memberships only):
  muted: boolean
  camera: boolean
  screen: boolean
  joinedAt: number
}

type ChannelSnapshot = {
  id: string
  kind: 'text' | 'voice'
  name: string
  topic: string
  /** Text: connections with the channel open (they can publish). Includes the bot. */
  memberCount: number
  /** Voice: the connected participants, with live mute/camera state. */
  occupants: Member[]
  size: number
  isFull: boolean
}

type DmThread = {
  userId: string
  name: string
  messages: DmMessage[]
  lastText: string
  lastAt: number
  hasMore: boolean
  loaded: boolean
}

/** What the main pane shows. `dm.withUserId === null` is the conversation list home. */
type View = { kind: 'channel' } | { kind: 'call' } | { kind: 'dm'; withUserId: string | null }

type CallState = {
  channelId: string
  myParticipantId: string
  muted: boolean
  micAvailable: boolean
  cameraOn: boolean
  screenOn: boolean
}

type AppState = {
  phase: 'boot' | 'auth' | 'ready' | 'kicked' | 'disconnected'
  kickedBy: string | null
  authError: string | null
  authPending: boolean
  me: { userId: string; name: string; color: string; admin: boolean; status: Member['status'] } | null
  members: Member[] // guild roster, deduped by user
  channels: ChannelSnapshot[]
  activeChannelId: string | null
  view: View
  messages: Record<string, ChatMessage[]> // channel room ID → history ∪ live, deduped by ID
  hasOlder: Record<string, boolean>
  unread: Record<string, number>
  typing: Record<string, string[]> // channel room ID → names typing right now
  dmThreads: Record<string, DmThread> // other user's ID → conversation
  dmUnread: Record<string, number>
  banner: { text: string; by: string } | null
  toast: string | null
  call: CallState | null
}

const initialState: AppState = {
  phase: 'boot',
  kickedBy: null,
  authError: null,
  authPending: false,
  me: null,
  members: [],
  channels: [],
  activeChannelId: null,
  view: { kind: 'channel' },
  messages: {},
  hasOlder: {},
  unread: {},
  typing: {},
  dmThreads: {},
  dmUnread: {},
  banner: null,
  toast: null,
  call: null,
}

const useApp = create<AppState>(() => initialState)
const setState = useApp.setState
const getState = useApp.getState

// ---------------------------------------------------------------------------
// Live handles (mutable Room objects — kept out of the store on purpose)
// ---------------------------------------------------------------------------

let guild: Room | null = null
let identity = { userId: '', name: '', color: '' }
const channelRooms = new Map<string, Room>()
const voiceOccupants = new Map<string, Member[]>() // voice channel room ID → live occupants
const seenMessageIds = new Map<string, Set<string>>() // history/live overlap dedup, per channel
const seenDmIds = new Set<string>()

// My membership in the open text channel. Switching channels is a leave + join round-trip, and
// a message sent during that window has no membership to publish through — so senders await the
// in-flight join instead of a resolved handle (our first version dropped fast-typed messages;
// see README finding on the channel-switch window).
let viewingJoin: Promise<LocalParticipant | null> = Promise.resolve(null)
let viewingNow: LocalParticipant | null = null // resolved membership — for fire-and-forget typing
let activeSwitch: symbol | null = null // identifies the latest channel switch (stale joins abandon)

function getChannelRoom(channelId: string): Room | undefined {
  return channelRooms.get(channelId)
}

function getIdentity(): { userId: string; name: string; color: string } {
  return identity
}

// ---------------------------------------------------------------------------
// Auth & boot
// ---------------------------------------------------------------------------

let bootStarted = false
/** Idempotent app boot — safe under StrictMode's double-invoked effects. */
function bootOnce(): void {
  if (bootStarted) return
  bootStarted = true
  void enter()
}

/** Try to enter with the session cookie; fall back to the auth screen. */
async function enter(): Promise<void> {
  try {
    const entered = await onEnterGuild()
    if (!entered.ok) {
      setState({ phase: 'auth' })
      return
    }
    identity = { userId: entered.user.id, name: entered.user.name, color: entered.user.color }
    guild = entered.guild
    wireGuild(entered.guild, entered.me)
    for (const room of entered.channels) wireChannel(room)
    publishChannels()

    setState({
      phase: 'ready',
      me: {
        userId: entered.user.id,
        name: entered.user.name,
        color: entered.user.color,
        admin: entered.user.isAdmin,
        status: 'online',
      },
    })
    keepMyStatus(entered.me)

    // The DM conversation list (durable, from the database).
    for (const thread of await onListDmThreads()) {
      upsertThread(thread.otherId, thread.otherName, (t) => ({
        ...t,
        lastText: thread.lastText,
        lastAt: thread.lastAt,
      }))
    }

    const general = getState().channels.find((c) => c.kind === 'text' && c.name === 'general')
    await openChannel(general?.id ?? getState().channels.find((c) => c.kind === 'text')?.id ?? '')
  } catch (err) {
    setState({ phase: 'auth', authError: errorMessage(err) })
  }
}

async function registerUser(name: string, password: string, color: string): Promise<void> {
  await authCall('/api/auth/register', { name, password, color })
}

async function loginUser(name: string, password: string): Promise<void> {
  await authCall('/api/auth/login', { name, password })
}

async function authCall(url: string, body: unknown): Promise<void> {
  setState({ authPending: true, authError: null })
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    })
    if (!response.ok) {
      const { error } = (await response.json().catch(() => ({}))) as { error?: string }
      setState({ authPending: false, authError: error ?? `Login failed (${response.status})` })
      return
    }
    setState({ authPending: false })
    await enter()
  } catch (err) {
    setState({ authPending: false, authError: errorMessage(err) })
  }
}

async function logout(): Promise<void> {
  await fetch('/api/auth/logout', { method: 'POST' }).catch(() => {})
  location.reload() // drop every live room/participant with the page
}

// ---------------------------------------------------------------------------
// Guild wiring
// ---------------------------------------------------------------------------

function wireGuild(guildRoom: Room, myself: LocalParticipant): void {
  // The member sidebar: presence + status changes, one row per *user* (a user with two tabs
  // has two participants — dedupe on the durable identity).
  watchRoster(guildRoom, (participants) => {
    const byUser = new Map<string, Member>()
    for (const member of participants) if (!byUser.has(member.userId)) byUser.set(member.userId, member)
    setState({ members: [...byUser.values()].sort(byName) })
  })

  // My private inbox: server-delivered DMs and kick notices (room-authored, `from === null`).
  myself.listen((data, from) => {
    if (from !== null) return // the member-to-member lane is closed by the guard
    const notice = data as SystemNotice
    if (notice.kind === 'kicked') setState({ phase: 'kicked', kickedBy: notice.by })
    if (notice.kind === 'dm') receiveDm(notice.message)
  })

  // Kicked, or the connection died. The kick notice (private lane) and the removal (control
  // lane) are different streams with no cross-lane ordering — give the notice a beat before
  // concluding it was a plain disconnect (README finding on cross-lane ordering).
  myself.onLeave(() => {
    setTimeout(() => {
      if (getState().phase === 'ready') setState({ phase: 'disconnected' })
    }, 500)
  })
  guildRoom.onClose(() => {
    if (getState().phase === 'ready') setState({ phase: 'disconnected' })
  })

  // The guild announce lane: server banners, kick notices — and the channel directory feed.
  guildRoom.onAnnounce((data) => {
    const event = data as GuildAnnouncement
    if (event.kind === 'announcement') showBanner(event.text, event.by)
    if (event.kind === 'channel-created') void adoptChannel(event.channelId)
    if (event.kind === 'member-kicked') showToast(`${event.name} was kicked by ${event.by}`)
  })
}

/** My own status changes (setMeta is a full replace — spread the rest; README finding). */
let myGuildParticipant: LocalParticipant | null = null
function keepMyStatus(me: LocalParticipant): void {
  myGuildParticipant = me
}

async function setStatus(status: Member['status']): Promise<void> {
  const { me } = getState()
  if (myGuildParticipant === null || me === null) return
  await myGuildParticipant.setMeta({ ...myGuildParticipant.meta, status })
  setState({ me: { ...me, status } })
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

/** A channel appeared (everyone learns it from the announce lane, including its creator). */
async function adoptChannel(channelId: string): Promise<void> {
  if (channelRooms.has(channelId)) return
  wireChannel(await onGetChannel(channelId))
  publishChannels()
}

function wireChannel(room: Room): void {
  if (channelRooms.has(room.id)) return
  channelRooms.set(room.id, room)

  if (asChannelMeta(room.meta).kind === 'text') {
    // One subscription per channel for the whole session: it feeds both the open conversation
    // and the unread badges. Discord-style unread means consuming every channel's text lane —
    // that's the honest cost (README finding on unread counts).
    room.subscribe((data, info, from) => {
      const published = data as ChannelPublish
      const sender = asMemberMeta(from.meta)
      if (published.kind === 'typing') {
        markTyping(room.id, sender.name)
        return
      }
      stopTyping(room.id, sender.name)
      recordMessage(room.id, {
        id: published.id,
        authorId: sender.userId,
        author: { name: sender.name, color: sender.color, bot: sender.bot },
        text: published.text,
        at: info.timestamp,
      })
    })
  } else {
    // Voice occupancy is plain presence — visible to everyone, costs no media: binary flows
    // only to clients that subscribed to a member's stream (see call.ts).
    watchRoster(room, (occupants) => {
      voiceOccupants.set(room.id, occupants)
      publishChannels()
    })
  }

  // Live counts, topic edits, deletion:
  room.onJoin(publishChannels)
  room.onLeave(publishChannels)
  room.onUpdate(publishChannels)
  room.onClose(() => dropChannel(room.id))
}

function publishChannels(): void {
  const snapshots = [...channelRooms.values()].map(toChannelSnapshot)
  snapshots.sort((a, b) => (a.kind === b.kind ? a.name.localeCompare(b.name) : a.kind === 'text' ? -1 : 1))
  setState({ channels: snapshots })
}

/** `room.onClose()` *is* the "channel deleted" signal — no announcement needed. */
function dropChannel(channelId: string): void {
  channelRooms.delete(channelId)
  voiceOccupants.delete(channelId)
  seenMessageIds.delete(channelId)
  publishChannels()
  if (getState().activeChannelId === channelId) {
    const general = getState().channels.find((c) => c.kind === 'text')
    if (general) void openChannel(general.id)
  }
}

async function openChannel(channelId: string): Promise<void> {
  const room = channelRooms.get(channelId)
  if (room === undefined || asChannelMeta(room.meta).kind !== 'text') return
  setState({
    activeChannelId: channelId,
    view: { kind: 'channel' },
    unread: { ...getState().unread, [channelId]: 0 },
  })

  // Swap my "viewing" membership — publishing (messages, typing) needs membership, and you
  // publish into the channel you have open. Switches are chained through `viewingJoin` so that
  // leave/join pairs can't interleave and senders always have a join to await.
  const previousJoin = viewingJoin
  viewingNow = null
  const switchToken = Symbol(channelId)
  activeSwitch = switchToken
  const thisJoin: Promise<LocalParticipant | null> = (async () => {
    const previous = await previousJoin.catch(() => null)
    if (previous !== null) void previous.leave().catch(() => {})
    const membership = await room.join({ ...identity, status: 'online' })
    if (activeSwitch !== switchToken) {
      void membership.leave().catch(() => {}) // user moved on while the join was in flight
      return null
    }
    viewingNow = membership
    return membership
  })()
  viewingJoin = thisJoin

  // The lossless-history recipe (/room docs § Load history, then go live): the subscription
  // has been live since boot (step 1), the join ack fences it server-side (step 2), and only
  // then is history read (step 3). Overlap is deduped by message ID; a gap can't happen.
  if ((await thisJoin) === null) return
  const history = await onChannelHistory(channelId)
  for (const message of history.messages) recordMessage(channelId, message)
  setState({
    unread: { ...getState().unread, [channelId]: 0 },
    hasOlder: { ...getState().hasOlder, [channelId]: history.hasMore },
  })
}

/** "Load older messages" — pages the database backwards from the oldest loaded message. */
async function loadOlderMessages(channelId: string): Promise<void> {
  const oldest = getState().messages[channelId]?.[0]
  if (oldest === undefined) return
  const page = await onChannelHistory(channelId, oldest.at)
  for (const message of page.messages) recordMessage(channelId, message, { silent: true })
  setState({ hasOlder: { ...getState().hasOlder, [channelId]: page.hasMore } })
}

async function sendMessage(text: string): Promise<void> {
  text = text.trim()
  if (text === '') return
  const membership = await viewingJoin.catch(() => null) // mid-switch: wait for the join, don't drop the message
  if (membership === null) return
  try {
    // No optimistic append: with selfDelivery (the default) my message comes back on the same
    // subscription as everyone else's — one render path (README finding on optimistic UI).
    await membership.publish({ kind: 'chat', id: crypto.randomUUID(), text } satisfies ChannelPublish)
  } catch (err) {
    showToast(errorMessage(err)) // e.g. the banned-word guard, rejected through the wire ack
  }
}

let lastTypingSentAt = 0
function sendTyping(): void {
  if (viewingNow === null) return // mid-switch typing isn't worth signaling
  const now = Date.now()
  if (now - lastTypingSentAt < 2000) return
  lastTypingSentAt = now
  void viewingNow.publish({ kind: 'typing' } satisfies ChannelPublish).catch(() => {})
}

async function createChannel(kind: 'text' | 'voice', name: string): Promise<void> {
  const created = await onCreateChannel(kind, name).catch((err) => ({ ok: false as const, error: errorMessage(err) }))
  if (!created.ok) showToast(created.error)
  // The new channel reaches everyone — including us — via the guild's `channel-created` announcement.
}

async function setTopic(topic: string): Promise<void> {
  const { activeChannelId } = getState()
  if (activeChannelId === null) return
  await onSetTopic(activeChannelId, topic).catch((err) => showToast(errorMessage(err)))
}

async function deleteChannel(channelId: string): Promise<void> {
  await onDeleteChannel(channelId).catch((err) => showToast(errorMessage(err)))
}

// ---------------------------------------------------------------------------
// Messages, typing, unread
// ---------------------------------------------------------------------------

function recordMessage(channelId: string, message: ChatMessage, opts?: { silent?: boolean }): void {
  const seen = seenMessageIds.get(channelId) ?? new Set<string>()
  if (seen.has(message.id)) return // the history/live overlap — drop the duplicate
  seen.add(message.id)
  seenMessageIds.set(channelId, seen)

  const state = getState()
  const thread = [...(state.messages[channelId] ?? []), message].sort((a, b) => a.at - b.at)
  const isOpen = state.activeChannelId === channelId && state.view.kind === 'channel'
  const isMine = message.authorId === identity.userId
  setState({
    messages: { ...state.messages, [channelId]: thread },
    unread:
      isOpen || isMine || opts?.silent
        ? state.unread
        : { ...state.unread, [channelId]: (state.unread[channelId] ?? 0) + 1 },
  })
}

const typingUntil = new Map<string, Map<string, number>>() // channel room ID → name → expiry

function markTyping(channelId: string, name: string): void {
  if (name === identity.name) return
  const perChannel = typingUntil.get(channelId) ?? new Map<string, number>()
  perChannel.set(name, Date.now() + 3000)
  typingUntil.set(channelId, perChannel)
  publishTyping(channelId)
  setTimeout(() => publishTyping(channelId), 3100)
}

function stopTyping(channelId: string, name: string): void {
  if (typingUntil.get(channelId)?.delete(name)) publishTyping(channelId)
}

function publishTyping(channelId: string): void {
  const now = Date.now()
  const names = [...(typingUntil.get(channelId) ?? [])].filter(([, until]) => until > now).map(([name]) => name)
  setState({ typing: { ...getState().typing, [channelId]: names } })
}

// ---------------------------------------------------------------------------
// Direct messages
// ---------------------------------------------------------------------------

function openDmHome(): void {
  setState({ view: { kind: 'dm', withUserId: null } })
}

async function openDm(withUserId: string, name: string): Promise<void> {
  upsertThread(withUserId, name, (t) => t)
  setState({ view: { kind: 'dm', withUserId }, dmUnread: { ...getState().dmUnread, [withUserId]: 0 } })
  if (!getState().dmThreads[withUserId]?.loaded) {
    const page = await onDmThread(withUserId)
    upsertThread(withUserId, name, (t) => ({ ...t, loaded: true, hasMore: page.hasMore }))
    for (const message of page.messages) recordDm(withUserId, message, { silent: true })
  }
}

async function sendDm(text: string): Promise<void> {
  const { view } = getState()
  if (view.kind !== 'dm' || view.withUserId === null) return
  try {
    // Server-delivered: the row is persisted first, then pushed to every live participant of
    // both users (my own copy comes back too — `recordDm` dedupes by ID).
    const message = await onSendDm(view.withUserId, text)
    recordDm(view.withUserId, message)
  } catch (err) {
    showToast(errorMessage(err)) // e.g. Do Not Disturb
  }
}

/** A DM arrived over the guild's room-authored lane (mine from another tab, or theirs). */
function receiveDm(message: DmMessage): void {
  const otherId = message.fromId === identity.userId ? message.toId : message.fromId
  recordDm(otherId, message)
}

function recordDm(otherId: string, message: DmMessage, opts?: { silent?: boolean }): void {
  if (seenDmIds.has(message.id)) return
  seenDmIds.add(message.id)
  const otherName = message.fromId === identity.userId ? message.toName : message.fromName
  upsertThread(otherId, otherName, (thread) => ({
    ...thread,
    messages: [...thread.messages, message].sort((a, b) => a.at - b.at),
    lastText: message.text,
    lastAt: Math.max(thread.lastAt, message.at),
  }))
  const state = getState()
  const isIncoming = message.fromId !== identity.userId && opts?.silent !== true
  const isOpen = state.view.kind === 'dm' && state.view.withUserId === otherId
  if (isIncoming && !isOpen) {
    setState({ dmUnread: { ...state.dmUnread, [otherId]: (state.dmUnread[otherId] ?? 0) + 1 } })
  }
}

function upsertThread(userId: string, name: string, update: (thread: DmThread) => DmThread): void {
  const state = getState()
  const existing = state.dmThreads[userId] ?? {
    userId,
    name,
    messages: [],
    lastText: '',
    lastAt: 0,
    hasMore: false,
    loaded: false,
  }
  setState({ dmThreads: { ...state.dmThreads, [userId]: update(existing) } })
}

// ---------------------------------------------------------------------------
// Moderation, notices, call state
// ---------------------------------------------------------------------------

async function kickUser(userId: string): Promise<void> {
  await onKickUser(userId).catch((err) => showToast(errorMessage(err)))
}

async function announce(text: string): Promise<void> {
  await onAnnounce(text).catch((err) => showToast(errorMessage(err)))
}

let bannerTimer: ReturnType<typeof setTimeout> | undefined
function showBanner(text: string, by: string): void {
  clearTimeout(bannerTimer)
  setState({ banner: { text, by } })
  bannerTimer = setTimeout(() => setState({ banner: null }), 8000)
}

let toastTimer: ReturnType<typeof setTimeout> | undefined
function showToast(text: string): void {
  clearTimeout(toastTimer)
  setState({ toast: text })
  toastTimer = setTimeout(() => setState({ toast: null }), 4000)
}

/** Show the connected call in the main pane. */
function openCall(): void {
  if (getState().call !== null) setState({ view: { kind: 'call' } })
}

/** Back to the guild (the rail's server button). */
function openGuild(): void {
  setState({ view: { kind: 'channel' } })
}

/** Call state lives here so the whole app renders from one store; call.ts drives it. */
function patchCall(call: CallState | null): void {
  // If the call ends while its view is open (leave, kick, channel deleted), fall back home.
  if (call === null && getState().view.kind === 'call') setState({ call, view: { kind: 'channel' } })
  else setState({ call })
}

// ---------------------------------------------------------------------------
// Room → snapshot projection
// ---------------------------------------------------------------------------

function toMember(participant: { id: string; meta: Record<string, unknown> }, joinedAt: number): Member {
  const meta = asMemberMeta(participant.meta)
  return {
    userId: meta.userId,
    participantId: participant.id,
    name: meta.name,
    color: meta.color,
    status: meta.status ?? 'online',
    bot: meta.bot === true,
    admin: meta.admin === true,
    muted: meta.muted === true,
    camera: meta.camera === true,
    screen: meta.screen === true,
    joinedAt,
  }
}

function toChannelSnapshot(room: Room): ChannelSnapshot {
  const meta = asChannelMeta(room.meta)
  return {
    id: room.id,
    kind: meta.kind,
    name: meta.name,
    topic: meta.topic ?? '',
    memberCount: room.count, // live — the event stream keeps the getter fresh
    occupants: voiceOccupants.get(room.id) ?? [],
    size: room.size,
    isFull: room.isFull,
  }
}

/**
 * Project a room's live roster into snapshots — THE recurring Room→React glue. Metadata
 * changes are per-member events (`member.onUpdate`), so each member is watched as it appears;
 * a member's listeners are released when it leaves, so there's no unsubscribe bookkeeping.
 */
function watchRoster(room: Room, onChange: (members: Member[]) => void): void {
  const push = () => void room.getParticipants().then((all) => onChange(all.map((m) => toMember(m, m.joinedAt))))
  const watch = (member: RemoteParticipant) => member.onUpdate(push)
  void room.getParticipants().then((all) => {
    for (const member of all) watch(member)
    push()
  })
  room.onJoin((member) => {
    watch(member)
    push()
  })
  room.onLeave(push)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function byName(a: Member, b: Member): number {
  return a.name.localeCompare(b.name)
}

function errorMessage(err: unknown): string {
  // Expected failures arrive as telefunc aborts — the human-readable text is the abort value.
  if (typeof err === 'object' && err !== null && 'abortValue' in err) {
    const value = (err as { abortValue: unknown }).abortValue
    if (typeof value === 'string') return value
  }
  return err instanceof Error ? err.message : String(err)
}
