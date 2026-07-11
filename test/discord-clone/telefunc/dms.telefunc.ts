export { onDmThread, onListDmThreads, onSendDm }

// Direct messages are DB-first and delivered by the server:
//
//   1. the row is written (DMs work even when the recipient is offline),
//   2. then it's pushed live — `Room.send()` to each of the recipient's (and the sender's
//      other tabs') guild participants, arriving on `me.listen()` as a room-authored notice.
//
// This deliberately does NOT use the member-to-member `me.send()` lane: participants exist
// only while their connection lives, so that lane can't address offline users — a Discord-like
// app can't be built on it (see README finding). The guild guard closes it outright.

import { randomUUID } from 'node:crypto'
import { Abort, getContext, Room, type RemoteParticipant } from 'telefunc'
import { dmThreadKey } from '../database/db'
import * as q from '../database/queries'
import { ensureLiveWorld, GUILD_ROOM_ID } from '../server/rooms'
import type { DmMessage, GuildRoom, MemberMeta, SystemNotice } from '../shared/types'

const PAGE_SIZE = 50

async function onSendDm(toUserId: string, text: string): Promise<DmMessage> {
  const user = requireUser()
  text = text.trim()
  if (text === '' || text.length > 2000) throw Abort('Say something (under 2000 characters)')
  const target = q.getUserById(toUserId)
  if (target === undefined) throw Abort('No such user')
  if (target.id === user.id) throw Abort("That's you")

  await ensureLiveWorld()
  const guild: GuildRoom = await Room.get(GUILD_ROOM_ID)

  // Do-Not-Disturb: live presence state, read from the guild roster. `identity` is the
  // server-stamped app identity — the durable way to say "the same user" (finding 1).
  const participants = await guild.getParticipants()
  const targetParticipants = participants.filter((p) => p.identity === target.id)
  if (targetParticipants.some((p) => p.meta.status === 'dnd')) {
    throw Abort(`${target.name} has Do Not Disturb on`)
  }

  const message = persistDm({ fromId: user.id, fromName: user.name, toId: target.id, toName: target.name, text })
  await deliverLive(guild, participants, message)

  // The bot answers DMs — same lane, same persistence.
  if (target.is_bot === 1) {
    const reply = persistDm({
      fromId: target.id,
      fromName: target.name,
      toId: user.id,
      toName: user.name,
      text: `You said: "${text.slice(0, 200)}" — I'm a bot; try !help in a channel.`,
    })
    await deliverLive(guild, participants, reply)
  }

  return message
}

/** A page of one conversation, newest-first cursor, oldest-first result. */
async function onDmThread(otherUserId: string, beforeAt?: number) {
  const user = requireUser()
  const rows = q.pageDmThread(dmThreadKey(user.id, otherUserId), beforeAt, PAGE_SIZE + 1)
  return { hasMore: rows.length > PAGE_SIZE, messages: rows.slice(0, PAGE_SIZE).reverse().map(toDmMessage) }
}

/** The sidebar's conversation list: one entry per correspondent, newest first. */
async function onListDmThreads() {
  const user = requireUser()
  const threads = new Map<string, { otherId: string; otherName: string; lastText: string; lastAt: number }>()
  for (const row of q.listDmsInvolving(user.id, 500)) {
    const other =
      row.from_id === user.id ? { id: row.to_id, name: row.to_name } : { id: row.from_id, name: row.from_name }
    if (!threads.has(other.id)) {
      threads.set(other.id, { otherId: other.id, otherName: other.name, lastText: row.text, lastAt: row.at })
    }
  }
  return [...threads.values()]
}

// --- Helpers ---

function toDmMessage(row: import('../database/schema').DmRow): DmMessage {
  return {
    id: row.id,
    fromId: row.from_id,
    fromName: row.from_name,
    toId: row.to_id,
    toName: row.to_name,
    text: row.text,
    at: row.at,
  }
}

function persistDm(dm: { fromId: string; fromName: string; toId: string; toName: string; text: string }): DmMessage {
  const message: DmMessage = { id: randomUUID(), ...dm, at: Date.now() }
  q.insertDm({
    id: message.id,
    thread_key: dmThreadKey(dm.fromId, dm.toId),
    from_id: dm.fromId,
    from_name: dm.fromName,
    to_id: dm.toId,
    to_name: dm.toName,
    text: dm.text,
    at: message.at,
  })
  return message
}

/** Push to every participant of the two users involved (sender's other tabs included) —
 *  clients dedupe by message ID. A participant racing away mid-send is fine. */
async function deliverLive(
  _guild: GuildRoom,
  participants: RemoteParticipant<MemberMeta>[],
  message: DmMessage,
): Promise<void> {
  const notice: SystemNotice = { kind: 'dm', message }
  await Promise.all(
    participants
      .filter((p) => p.identity === message.toId || p.identity === message.fromId)
      .map((p) => Room.send(GUILD_ROOM_ID, p.id, notice).catch(() => {})),
  )
}

function requireUser() {
  const { user } = getContext()
  if (user === null) throw Abort('Not logged in')
  return user
}
