// The client store (zustand): all Room wiring on one side, immutable snapshots for React on
// the other.
//
// Round 2 of the stress test: the hand-rolled ~80-line roster adapter (`watchRoster` + per-
// member `onUpdate` bookkeeping + immutable copying) is gone — rooms now expose
// `snapshot()`/`onChange()`, the exact UI-store contract (README finding 9, fixed upstream).
// Unread badges cost one subscription: the channel's `onAfterPublish` hook pings the guild
// announce lane (which every client already listens to), so only the open channel's full text
// lane crosses the wire (finding 10). History and the live lane share one clock and one order —
// the message's central `seq` (finding 13) — and the first history read is fenced losslessly by
// `Room.get(…, { tail: true })` (finding 14).
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

import type { LeaveCause, LocalParticipant, ParticipantSnapshotView } from 'telefunc'
import { create } from 'zustand'
import type {
  ChannelPublish,
  ChannelRoom,
  ChatMessage,
  DmMessage,
  GuildAnnouncement,
  GuildRoom,
  MemberMeta,
  SystemNotice,
} from '../shared/types'
import { onAnnounce, onKickUser } from '../telefunc/admin.telefunc'
import {
  onChannelHistory,
  onCreateChannel,
  onDeleteChannel,
  onGetChannel,
  onOpenChannel,
  onSetTopic,
} from '../telefunc/channels.telefunc'
import { onDmThread, onListDmThreads, onSendDm } from '../telefunc/dms.telefunc'
import { onEnterGuild } from '../telefunc/session.telefunc'

// ---------------------------------------------------------------------------
// State (immutable snapshots — what React renders)
// ---------------------------------------------------------------------------

/** A room member, snapshotted for rendering. `userId` is the durable app identity (the join's
 *  server-stamped `identity`); `participantId` identifies one connection in one room (a user
 *  with two tabs has two participants). */
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
  /** Unread dot per channel — fed by the room's `onActivity` signal (throttled, body-free),
   *  so it can't be an exact count. Discord shows a dot for plain unread too. */
  unread: Record<string, boolean>
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

let guild: GuildRoom | null = null
let identity = { userId: '', name: '', color: '' }
const channelRooms = new Map<string, ChannelRoom>()
const seenMessageIds = new Map<string, Set<string>>() // history/live overlap dedup, per channel
const seenDmIds = new Set<string>()

// My membership in the open text channel. Switching channels is a leave + join round-trip, and
// a message sent during that window has no membership to publish through — so senders await the
// in-flight join instead of a resolved handle (our first version dropped fast-typed messages;
// see README finding on the channel-switch window).
let viewingJoin: Promise<LocalParticipant<MemberMeta> | null> = Promise.resolve(null)
let viewingNow: LocalParticipant<MemberMeta> | null = null // resolved membership — for fire-and-forget typing
let activeSwitch: symbol | null = null // identifies the latest channel switch (stale joins abandon)
let activeMessagesUnsubscribe: (() => void) | null = null // the open channel's text-lane subscription

function getChannelRoom(channelId: string): ChannelRoom | undefined {
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

function wireGuild(guildRoom: GuildRoom, myself: LocalParticipant<MemberMeta>): void {
  // The member sidebar. `snapshot({ by: 'identity' })` returns one group per *user* — the server
  // collapses a user's tabs/connections, so this store no longer hand-rolls the identity dedupe
  // with a Map (finding 19). Reference-stable + `onChange()` is still the whole adapter (finding 9).
  const pushMembers = () => {
    const members: Member[] = []
    for (const group of guildRoom.snapshot({ by: 'identity' }).identities) {
      const rep = group.participants[0] // a group always has ≥1 membership; the tabs share a user
      if (rep !== undefined) members.push(toMember(rep))
    }
    setState({ members: members.sort(byName) })
  }
  guildRoom.onChange(pushMembers)
  pushMembers()

  // My private inbox: server-delivered DMs (room-authored, `from === null`).
  myself.listen((data, from) => {
    if (from !== null) return // the member-to-member lane is closed by the guard
    const notice = data as SystemNotice
    if (notice.kind === 'dm') receiveDm(notice.message)
  })

  // Every leave carries its cause now — a kick arrives as `removed` with the kicker's name as
  // `reason`, on the leave itself. The old pre-kick notice and its 500ms cross-lane
  // disambiguation timer are gone (finding 12, fixed upstream).
  myself.onLeave((cause: LeaveCause) => {
    if (getState().phase !== 'ready') return
    if (cause.type === 'removed') {
      setState({ phase: 'kicked', kickedBy: typeof cause.reason === 'string' ? cause.reason : null })
    } else if (cause.type !== 'left') {
      setState({ phase: 'disconnected' })
    }
  })
  guildRoom.onClose(() => {
    if (getState().phase === 'ready') setState({ phase: 'disconnected' })
  })

  // The guild announce lane: server banners, kick announcements, the channel directory feed —
  // and now unread activity. One subscription carries every channel's "something was posted"
  // ping (the base removed the per-channel `onActivity` signal; the channel's `onAfterPublish`
  // hook re-derives it here, server-side — README finding 10).
  guildRoom.onAnnounce((data) => {
    const event = data as GuildAnnouncement
    if (event.kind === 'announcement') showBanner(event.text, event.by)
    if (event.kind === 'channel-created') void adoptChannel(event.channelId)
    if (event.kind === 'member-kicked') showToast(`${event.name} was kicked by ${event.by}`)
    if (event.kind === 'channel-activity') markChannelActivity(event.channelId)
  })
}

/** My own status changes. `setAttributes` merges one field — no read-modify-write of the whole
 *  metadata record, and a concurrent change to a different field can't clobber it (README
 *  finding 5). */
let myGuildParticipant: LocalParticipant<MemberMeta> | null = null
function keepMyStatus(me: LocalParticipant<MemberMeta>): void {
  myGuildParticipant = me
}

async function setStatus(status: Member['status']): Promise<void> {
  const { me } = getState()
  if (myGuildParticipant === null || me === null) return
  await myGuildParticipant.setAttributes({ status })
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

function wireChannel(room: ChannelRoom): void {
  if (channelRooms.has(room.id)) return
  channelRooms.set(room.id, room)

  // Unread dots no longer need a per-channel subscription: the guild announce lane carries every
  // channel's activity ping (see `markChannelActivity`, wired in wireGuild). The full text lane
  // flows only for the channel that's actually open (see openChannel).

  // Counts, occupant/meta changes, topic edits — one subscription covers them all:
  room.onChange(publishChannels)
  room.onClose(() => dropChannel(room.id))
}

/** A message landed somewhere (a guild-lane activity ping) — show a dot unless that channel is
 *  the one currently open. */
function markChannelActivity(channelId: string): void {
  const state = getState()
  const isOpen = state.activeChannelId === channelId && state.view.kind === 'channel'
  if (!isOpen) setState({ unread: { ...state.unread, [channelId]: true } })
}

function publishChannels(): void {
  const snapshots = [...channelRooms.values()].map(toChannelSnapshot)
  snapshots.sort((a, b) => (a.kind === b.kind ? a.name.localeCompare(b.name) : a.kind === 'text' ? -1 : 1))
  setState({ channels: snapshots })
}

/** `room.onClose()` *is* the "channel deleted" signal — no announcement needed. */
function dropChannel(channelId: string): void {
  channelRooms.delete(channelId)
  seenMessageIds.delete(channelId)
  publishChannels()
  if (getState().activeChannelId === channelId) {
    const general = getState().channels.find((c) => c.kind === 'text')
    if (general) void openChannel(general.id)
  }
}

async function openChannel(channelId: string): Promise<void> {
  const room = channelRooms.get(channelId)
  if (room === undefined || room.meta.kind !== 'text') return
  setState({
    activeChannelId: channelId,
    view: { kind: 'channel' },
    unread: { ...getState().unread, [channelId]: false },
  })

  // Stop the previously open channel's live lane now; the new channel subscribes only *after* its
  // history is rendered, so the tail buffered since the server returned the room flushes in order
  // behind the past (README finding 14).
  activeMessagesUnsubscribe?.()
  activeMessagesUnsubscribe = null

  // Swap my "viewing" membership — publishing (messages, typing) needs membership, and you publish
  // into the channel you have open. `onOpenChannel` is one telefunction that joins with my trusted
  // identity *and* returns fenced history (it fetched the room with `{ tail: true }`). Switches are
  // chained so leave/join pairs can't interleave and senders always have a join to await.
  type Opened = { membership: LocalParticipant<MemberMeta, ChannelPublish>; history: ChatMessage[]; hasMore: boolean }
  const previousJoin = viewingJoin
  viewingNow = null
  const switchToken = Symbol(channelId)
  activeSwitch = switchToken
  const thisOpen: Promise<Opened | null> = (async () => {
    const previous = await previousJoin.catch(() => null)
    if (previous !== null) void previous.leave().catch(() => {})
    let opened: Opened
    try {
      opened = await onOpenChannel(channelId)
    } catch (err) {
      // Surface the failure instead of rejecting: a bare throw here bounces boot to the auth
      // screen (enter()'s catch) and drops the sender's next message silently. Only toast if this
      // switch is still the one the user is waiting on.
      if (activeSwitch === switchToken) showToast(errorMessage(err))
      return null
    }
    if (activeSwitch !== switchToken) {
      void opened.membership.leave().catch(() => {}) // user moved on while the join was in flight
      return null
    }
    viewingNow = opened.membership
    return opened
  })()
  viewingJoin = thisOpen.then((opened) => opened?.membership ?? null)

  const opened = await thisOpen
  // Re-check the token: the user can switch away in the gap before this continuation runs, and the
  // history-record + trailing unread-clear below would otherwise land on an abandoned channel
  // (wrongly clearing a genuinely-unread badge).
  if (opened === null || activeSwitch !== switchToken) return

  // The past first…
  for (const message of opened.history) recordMessage(channelId, message)
  // …then go live: subscribing flushes the held tail behind the history, deduped by id
  // (`recordMessage` drops ids it has already shown). No message can fall in the gap.
  activeMessagesUnsubscribe = room.subscribe((published, info, from) => {
    // `published` is typed `ChannelPublish` — the room carries its publish type now, no cast
    // (README finding 20, adopted upstream).
    if (published.kind === 'typing') {
      markTyping(room.id, from.meta.name)
      return
    }
    stopTyping(room.id, from.meta.name)
    recordMessage(room.id, {
      id: published.id,
      authorId: from.identity ?? from.id,
      author: { name: from.meta.name, color: from.meta.color, bot: from.meta.bot },
      text: published.text,
      seq: info.seq,
      at: info.timestamp,
    })
  })
  setState({
    unread: { ...getState().unread, [channelId]: false },
    hasOlder: { ...getState().hasOlder, [channelId]: opened.hasMore },
  })
}

/** "Load older messages" — pages the database backwards from the oldest loaded message, by `seq`
 *  (the room's authoritative order). */
async function loadOlderMessages(channelId: string): Promise<void> {
  const oldest = getState().messages[channelId]?.[0]
  if (oldest === undefined) return
  const page = await onChannelHistory(channelId, oldest.seq)
  for (const message of page.messages) recordMessage(channelId, message)
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
// Messages & typing
// ---------------------------------------------------------------------------

function recordMessage(channelId: string, message: ChatMessage): void {
  const seen = seenMessageIds.get(channelId) ?? new Set<string>()
  if (seen.has(message.id)) return // the history/live overlap — drop the duplicate
  seen.add(message.id)
  seenMessageIds.set(channelId, seen)

  const state = getState()
  // Order by the room's authoritative `seq` — history and the live tail read as one timeline.
  const thread = [...(state.messages[channelId] ?? []), message].sort((a, b) => a.seq - b.seq)
  setState({ messages: { ...state.messages, [channelId]: thread } })
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

function toMember(p: ParticipantSnapshotView<MemberMeta>): Member {
  return {
    userId: p.identity ?? p.id, // all app joins are server-side, so identity is always set
    participantId: p.id,
    name: p.meta.name,
    color: p.meta.color,
    status: p.meta.status ?? 'online',
    bot: p.meta.bot === true,
    admin: p.meta.admin === true,
    muted: p.meta.muted === true,
    camera: p.meta.camera === true,
    screen: p.meta.screen === true,
    joinedAt: p.joinedAt,
  }
}

function toChannelSnapshot(room: ChannelRoom): ChannelSnapshot {
  return {
    id: room.id,
    kind: room.meta.kind,
    name: room.meta.name,
    topic: room.meta.topic ?? '',
    memberCount: room.count, // live — the event stream keeps the getter fresh
    occupants: room.meta.kind === 'voice' ? room.snapshot().participants.map(toMember) : [],
    size: room.size,
    isFull: room.isFull,
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function byName(a: Member, b: Member): number {
  return a.name.localeCompare(b.name)
}

function errorMessage(err: unknown): string {
  // A telefunction `Abort('…')` now surfaces its own message on `err.message` (finding 7, fixed
  // upstream), so expected failures read straight off the Error — no digging in `err.abortValue`.
  return err instanceof Error ? err.message : String(err)
}
