export { onChannelHistory, onCreateChannel, onDeleteChannel, onGetChannel, onJoinVoice, onOpenChannel, onSetTopic }

import { randomUUID } from 'node:crypto'
import { Abort, getContext, Room, type LocalParticipant } from 'telefunc'
import type { MessageRow } from '../database/schema'
import * as q from '../database/queries'
import { getGuardedChannel } from '../server/guards'
import { channelRoomId, dbChannelIdOf, ensureLiveWorld, GUILD_ROOM_ID, VOICE_CHANNEL_SIZE } from '../server/rooms'
import type { ChannelPublish, ChannelRoom, ChatMessage, MemberMeta } from '../shared/types'

// Page size is env-tunable so the e2e suite can exercise "Load older" without 50 sends.
const PAGE_SIZE = Number(process.env.DISCORD_CLONE_PAGE_SIZE ?? 50)

/** One newest-first page of history, ordered by the room's authoritative `seq`, returned
 *  oldest-first for rendering. Reads the DB directly — see `onOpenChannel` for the fence that
 *  makes the *first* read lossless against the live lane. */
function readHistory(dbChannelId: string, beforeSeq?: number): { messages: ChatMessage[]; hasMore: boolean } {
  const rows = q.pageMessages(dbChannelId, beforeSeq, PAGE_SIZE + 1)
  const hasMore = rows.length > PAGE_SIZE
  return { hasMore, messages: rows.slice(0, PAGE_SIZE).reverse().map(toChatMessage) }
}

function toChatMessage(row: MessageRow): ChatMessage {
  return {
    id: row.id,
    authorId: row.author_id,
    author: { name: row.author_name, color: row.author_color, bot: row.author_is_bot === 1 },
    text: row.text,
    seq: row.seq,
    at: row.at,
  }
}

/** "Load older": pages backwards from a `seq` cursor. The first page comes from `onOpenChannel`. */
async function onChannelHistory(roomId: string, beforeSeq?: number) {
  requireUser()
  const dbChannelId = requireDbChannelId(roomId)
  return readHistory(dbChannelId, beforeSeq)
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
  try {
    q.insertChannel({ id, kind, name, topic: '', created_at: Date.now() })
  } catch {
    // Lost the check-then-insert race to a concurrent create with the same name (UNIQUE(name)).
    return { ok: false as const, error: `#${name} already exists` }
  }
  const roomId = channelRoomId(id)
  try {
    await Room.create(
      roomId,
      kind === 'voice'
        ? { meta: { kind, name }, size: VOICE_CHANNEL_SIZE, isolated: true }
        : { meta: { kind, name, topic: '' } },
    )
  } catch (err) {
    q.deleteChannel(id) // don't leave a ghost row with no live room behind
    throw err
  }
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
 * Open a text channel: join the "viewing" membership (it grants publish, and carries the caller's
 * trusted `identity` because the join is server-side) *and* load history — in one round-trip.
 *
 * `{ tail: true }` fetches the room already relaying its live messages; the client holds them until
 * its first `subscribe()`. Reading history *after* that fetch means nothing published in between is
 * lost — the client renders the page, then subscribes to flush the held tail behind it (deduped by
 * id). That closes the lossless-history fence this app used to only approximate (README finding 14).
 */
async function onOpenChannel(
  roomId: string,
): Promise<{ membership: LocalParticipant<MemberMeta, ChannelPublish>; history: ChatMessage[]; hasMore: boolean }> {
  const user = requireUser()
  const dbChannelId = requireDbChannelId(roomId)
  const channel = await getGuardedChannel(roomId, { tail: true })
  if (channel.meta.kind !== 'text') throw Abort('Not a text channel')
  const membership = await channel.join({ name: user.name, color: user.color, status: 'online' }, { identity: user.id })
  const { messages, hasMore } = readHistory(dbChannelId) // read after tailing began — see above
  return { membership, history: messages, hasMore }
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
  const user = requireUser()
  if (!user.isAdmin) throw Abort('Only the server owner can edit channel topics') // was unguarded
  const dbChannelId = requireDbChannelId(roomId)
  const channel = await Room.get<import('../shared/types').ChannelMeta>(roomId)
  if (channel.meta.kind !== 'text') throw Abort('Voice channels have no topic')
  topic = topic.trim().slice(0, 120)
  q.setChannelTopic(dbChannelId, topic)
  // Per-key room-meta merge — write just `topic`, no read-modify-write respread of `kind`/`name`
  // and no clobber of a concurrent edit to another field (README finding 21, adopted upstream).
  await Room.setAttributes(roomId, { topic })
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
