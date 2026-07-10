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
import { and, desc, eq, lt, or } from 'drizzle-orm'
import { Abort, getContext, Room } from 'telefunc'
import { db, dmThreadKey } from '../database/db'
import { dms, users } from '../database/schema'
import { ensureLiveWorld, GUILD_ROOM_ID } from '../server/rooms'
import { asMemberMeta, type DmMessage, type SystemNotice } from '../shared/types'

const PAGE_SIZE = 50

async function onSendDm(toUserId: string, text: string): Promise<DmMessage> {
  const user = requireUser()
  text = text.trim()
  if (text === '' || text.length > 2000) throw Abort('Say something (under 2000 characters)')
  const target = db.select().from(users).where(eq(users.id, toUserId)).limit(1).all()[0]
  if (target === undefined) throw Abort('No such user')
  if (target.id === user.id) throw Abort("That's you")

  await ensureLiveWorld()
  const guild = await Room.get(GUILD_ROOM_ID)

  // Do-Not-Disturb: live presence state, read from the guild roster.
  const participants = await guild.getParticipants()
  const targetParticipants = participants.filter((p) => asMemberMeta(p.meta).userId === target.id)
  if (targetParticipants.some((p) => asMemberMeta(p.meta).status === 'dnd')) {
    throw Abort(`${target.name} has Do Not Disturb on`)
  }

  const message = persistDm({ fromId: user.id, fromName: user.name, toId: target.id, toName: target.name, text })
  await deliverLive(guild, participants, message)

  // The bot answers DMs — same lane, same persistence.
  if (target.isBot) {
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
  const threadKey = dmThreadKey(user.id, otherUserId)
  const rows = db
    .select()
    .from(dms)
    .where(
      beforeAt === undefined ? eq(dms.threadKey, threadKey) : and(eq(dms.threadKey, threadKey), lt(dms.at, beforeAt)),
    )
    .orderBy(desc(dms.at))
    .limit(PAGE_SIZE + 1)
    .all()
  return { hasMore: rows.length > PAGE_SIZE, messages: rows.slice(0, PAGE_SIZE).reverse().map(toDmMessage) }
}

/** The sidebar's conversation list: one entry per correspondent, newest first. */
async function onListDmThreads() {
  const user = requireUser()
  const rows = db
    .select()
    .from(dms)
    .where(or(eq(dms.fromId, user.id), eq(dms.toId, user.id)))
    .orderBy(desc(dms.at))
    .limit(500)
    .all()
  const threads = new Map<string, { otherId: string; otherName: string; lastText: string; lastAt: number }>()
  for (const row of rows) {
    const other = row.fromId === user.id ? { id: row.toId, name: row.toName } : { id: row.fromId, name: row.fromName }
    if (!threads.has(other.id)) {
      threads.set(other.id, { otherId: other.id, otherName: other.name, lastText: row.text, lastAt: row.at })
    }
  }
  return [...threads.values()]
}

// --- Helpers ---

function toDmMessage(row: typeof dms.$inferSelect): DmMessage {
  return {
    id: row.id,
    fromId: row.fromId,
    fromName: row.fromName,
    toId: row.toId,
    toName: row.toName,
    text: row.text,
    at: row.at,
  }
}

function persistDm(dm: { fromId: string; fromName: string; toId: string; toName: string; text: string }): DmMessage {
  const message: DmMessage = { id: randomUUID(), ...dm, at: Date.now() }
  db.insert(dms)
    .values({ ...message, threadKey: dmThreadKey(dm.fromId, dm.toId) })
    .run()
  return message
}

/** Push to every participant of the two users involved (sender's other tabs included) —
 *  clients dedupe by message ID. A participant racing away mid-send is fine. */
async function deliverLive(
  guild: Room,
  participants: Awaited<ReturnType<Room['getParticipants']>>,
  message: DmMessage,
): Promise<void> {
  const notice: SystemNotice = { kind: 'dm', message }
  await Promise.all(
    participants
      .filter((p) => {
        const userId = asMemberMeta(p.meta).userId
        return userId === message.toId || userId === message.fromId
      })
      .map((p) => Room.send(GUILD_ROOM_ID, p.id, notice).catch(() => {})),
  )
}

function requireUser() {
  const { user } = getContext()
  if (user === null) throw Abort('Not logged in')
  return user
}
