// All SQL in one place — typed, prepared statements over node:sqlite.

export {
  countHumans,
  deleteChannel,
  deleteSession,
  getChannel,
  getChannelByName,
  getSessionUser,
  getUserById,
  getUserByName,
  insertChannel,
  insertDm,
  insertMessage,
  insertSession,
  insertUser,
  listChannels,
  listDmsInvolving,
  pageDmThread,
  pageMessages,
  setChannelTopic,
  theBotUser,
}

import { db } from './db'
import type { ChannelRow, DmRow, MessageRow, UserRow } from './schema'

// --- Users & sessions ---

function insertUser(user: UserRow): void {
  db.prepare('INSERT INTO users (id, name, color, password_hash, is_admin, is_bot, created_at) VALUES (?,?,?,?,?,?,?)') //
    .run(user.id, user.name, user.color, user.password_hash, user.is_admin, user.is_bot, user.created_at)
}

function getUserByName(name: string): UserRow | undefined {
  return db.prepare('SELECT * FROM users WHERE name = ?').get(name) as UserRow | undefined
}

function getUserById(id: string): UserRow | undefined {
  return db.prepare('SELECT * FROM users WHERE id = ?').get(id) as UserRow | undefined
}

function theBotUser(): UserRow | undefined {
  return db.prepare('SELECT * FROM users WHERE is_bot = 1 LIMIT 1').get() as UserRow | undefined
}

function countHumans(): number {
  const row = db.prepare('SELECT COUNT(*) AS n FROM users WHERE is_bot = 0').get() as { n: number }
  return row.n
}

function insertSession(token: string, userId: string, expiresAt: number): void {
  db.prepare('INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)').run(token, userId, expiresAt)
}

function deleteSession(token: string): void {
  db.prepare('DELETE FROM sessions WHERE token = ?').run(token)
}

function getSessionUser(token: string, now: number): UserRow | undefined {
  return db
    .prepare(
      `SELECT users.* FROM sessions JOIN users ON users.id = sessions.user_id
       WHERE sessions.token = ? AND sessions.expires_at > ?`,
    )
    .get(token, now) as UserRow | undefined
}

// --- Channels ---

function insertChannel(channel: ChannelRow): void {
  db.prepare('INSERT INTO channels (id, kind, name, topic, created_at) VALUES (?,?,?,?,?)') //
    .run(channel.id, channel.kind, channel.name, channel.topic, channel.created_at)
}

function listChannels(): ChannelRow[] {
  return db.prepare('SELECT * FROM channels ORDER BY created_at').all() as ChannelRow[]
}

function getChannel(id: string): ChannelRow | undefined {
  return db.prepare('SELECT * FROM channels WHERE id = ?').get(id) as ChannelRow | undefined
}

function getChannelByName(name: string): ChannelRow | undefined {
  return db.prepare('SELECT * FROM channels WHERE name = ?').get(name) as ChannelRow | undefined
}

function setChannelTopic(id: string, topic: string): void {
  db.prepare('UPDATE channels SET topic = ? WHERE id = ?').run(topic, id)
}

function deleteChannel(id: string): void {
  db.prepare('DELETE FROM messages WHERE channel_id = ?').run(id)
  db.prepare('DELETE FROM channels WHERE id = ?').run(id)
}

// --- Messages ---

function insertMessage(message: MessageRow): void {
  db.prepare(
    `INSERT INTO messages (id, channel_id, author_id, author_name, author_color, author_is_bot, text, seq, at)
     VALUES (?,?,?,?,?,?,?,?,?)`,
  ).run(
    message.id,
    message.channel_id,
    message.author_id,
    message.author_name,
    message.author_color,
    message.author_is_bot,
    message.text,
    message.seq,
    message.at,
  )
}

/** Newest-first page, ordered by the room's authoritative `seq` (`beforeSeq` cursor). `limit + 1`
 *  rows tells the caller whether more exist. */
function pageMessages(channelId: string, beforeSeq: number | undefined, limit: number): MessageRow[] {
  return (
    beforeSeq === undefined
      ? db.prepare('SELECT * FROM messages WHERE channel_id = ? ORDER BY seq DESC LIMIT ?').all(channelId, limit)
      : db
          .prepare('SELECT * FROM messages WHERE channel_id = ? AND seq < ? ORDER BY seq DESC LIMIT ?')
          .all(channelId, beforeSeq, limit)
  ) as MessageRow[]
}

// --- Direct messages ---

function insertDm(dm: DmRow): void {
  db.prepare(
    'INSERT INTO dms (id, thread_key, from_id, from_name, to_id, to_name, text, at) VALUES (?,?,?,?,?,?,?,?)',
  ).run(dm.id, dm.thread_key, dm.from_id, dm.from_name, dm.to_id, dm.to_name, dm.text, dm.at)
}

function pageDmThread(threadKey: string, beforeAt: number | undefined, limit: number): DmRow[] {
  return (
    beforeAt === undefined
      ? db.prepare('SELECT * FROM dms WHERE thread_key = ? ORDER BY at DESC LIMIT ?').all(threadKey, limit)
      : db
          .prepare('SELECT * FROM dms WHERE thread_key = ? AND at < ? ORDER BY at DESC LIMIT ?')
          .all(threadKey, beforeAt, limit)
  ) as DmRow[]
}

/** Recent DMs where the user is either end — the conversation list derives from these. */
function listDmsInvolving(userId: string, limit: number): DmRow[] {
  return db
    .prepare('SELECT * FROM dms WHERE from_id = ? OR to_id = ? ORDER BY at DESC LIMIT ?')
    .all(userId, userId, limit) as DmRow[]
}
