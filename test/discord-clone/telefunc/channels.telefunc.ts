export { onChannelHistory, onCreateChannel, onDeleteChannel, onGetChannel, onJoinVoice, onOpenChannel, onSetTopic }

import { randomUUID } from 'node:crypto'
import { Abort, getContext, Room, type LocalParticipant } from 'telefunc'
import * as q from '../database/queries'
import { getGuardedChannel } from '../server/guards'
import { channelRoomId, dbChannelIdOf, ensureLiveWorld, GUILD_ROOM_ID, VOICE_CHANNEL_SIZE } from '../server/rooms'
import type { ChannelRoom, ChatMessage, MemberMeta } from '../shared/types'

// Page size is env-tunable so the e2e suite can exercise "Load older" without 50 sends.
const PAGE_SIZE = Number(process.env.DISCORD_CLONE_PAGE_SIZE ?? 50)

/** A page of history, newest-first cursor (`beforeAt`), returned oldest-first for rendering. */
async function onChannelHistory(roomId: string, beforeAt?: number) {
  requireUser()
  const dbChannelId = requireDbChannelId(roomId)
  const rows = q.pageMessages(dbChannelId, beforeAt, PAGE_SIZE + 1)
  const hasMore = rows.length > PAGE_SIZE
  const page = rows.slice(0, PAGE_SIZE).reverse()
  return {
    hasMore,
    messages: page.map(
      (row): ChatMessage => ({
        id: row.id,
        authorId: row.author_id,
        author: { name: row.author_name, color: row.author_color, bot: row.author_is_bot === 1 },
        text: row.text,
        at: row.at,
      }),
    ),
  }
}

async function onCreateChannel(kind: 'text' | 'voice', rawName: string) {
  requireUser()
  await ensureLiveWorld()
  const name = rawName
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 32)
  if (name === '') return { ok: false as const, error: 'Pick a name with some letters in it' }
  if (q.getChannelByName(name) !== undefined) {
    return { ok: false as const, error: `#${name} already exists` }
  }

  const id = randomUUID()
  q.insertChannel({ id, kind, name, topic: '', created_at: Date.now() })
  const roomId = channelRoomId(id)
  await Room.create(
    roomId,
    kind === 'voice'
      ? { meta: { kind, name }, size: VOICE_CHANNEL_SIZE, isolated: true }
      : { meta: { kind, name, topic: '' } },
  )
  // Rooms have no directory events — nothing tells other clients a room appeared. The guild's
  // announce lane doubles as the app's directory feed (every client fetches the room on this).
  await Room.announce(GUILD_ROOM_ID, { kind: 'channel-created', channelId: roomId })
  return { ok: true as const, channelId: roomId }
}

/** Fetch one channel — used when the guild announces `channel-created`. */
async function onGetChannel(roomId: string): Promise<ChannelRoom> {
  requireUser()
  requireDbChannelId(roomId)
  return await getGuardedChannel(roomId)
}

/**
 * Open a text channel: the "viewing" membership (it grants publish). Joined server-side so the
 * membership carries the caller's trusted `identity` — client-side joins can't set one.
 */
async function onOpenChannel(roomId: string): Promise<LocalParticipant<MemberMeta>> {
  const user = requireUser()
  const channel = await getGuardedChannel(roomId)
  if (channel.meta.kind !== 'text') throw Abort('Not a text channel')
  return await channel.join({ name: user.name, color: user.color, status: 'online' }, { identity: user.id })
}

/**
 * Join a voice channel. Server-side for the same identity reason — and because the voice rooms'
 * `onJoin` guard enforces capacity here, where it can't be bypassed (README finding 11, fixed
 * upstream). `selfDelivery: false`: my own media must not come back to me.
 */
async function onJoinVoice(roomId: string): Promise<LocalParticipant<MemberMeta>> {
  const user = requireUser()
  const channel = await getGuardedChannel(roomId)
  if (channel.meta.kind !== 'voice') throw Abort('Not a voice channel')
  try {
    return await channel.join(
      { name: user.name, color: user.color, status: 'online', muted: false, camera: false, screen: false },
      { identity: user.id, selfDelivery: false },
    )
  } catch (err) {
    throw Abort(err instanceof Error ? err.message : String(err)) // e.g. the capacity guard
  }
}

async function onSetTopic(roomId: string, topic: string): Promise<void> {
  requireUser()
  const dbChannelId = requireDbChannelId(roomId)
  const channel = await Room.get<import('../shared/types').ChannelMeta>(roomId)
  if (channel.meta.kind !== 'text') throw Abort('Voice channels have no topic')
  topic = topic.trim().slice(0, 120)
  q.setChannelTopic(dbChannelId, topic)
  // `Room.update()` is per-field now — omitting `size` keeps the cap (README finding 5, fixed
  // upstream; this used to require a read-modify-write of everything).
  await Room.update(roomId, { meta: { ...channel.meta, topic } })
}

async function onDeleteChannel(roomId: string): Promise<void> {
  const user = requireUser()
  if (!user.isAdmin) throw Abort('Only the server owner can delete channels')
  const dbChannelId = requireDbChannelId(roomId)
  const row = q.getChannel(dbChannelId)
  if (row === undefined) throw Abort('No such channel')
  if (row.name === 'general') throw Abort('#general is forever')
  // Every holder's `room.onClose()` fires — that *is* the "channel deleted" signal.
  await Room.close(roomId)
  q.deleteChannel(dbChannelId) // messages included
}

// --- Helpers ---

function requireUser() {
  const { user } = getContext()
  if (user === null) throw Abort('Not logged in')
  return user
}

function requireDbChannelId(roomId: string): string {
  const dbChannelId = dbChannelIdOf(roomId)
  if (dbChannelId === null) throw Abort('Not a channel of this app')
  return dbChannelId
}
