// The durable truth. Rooms are the *live* layer (presence, delivery, events) — everything
// that must survive a restart lives here.

export { users, sessions, channels, messages, dms }

import { index, integer, sqliteTable, text } from 'drizzle-orm/sqlite-core'

const users = sqliteTable('users', {
  id: text('id').primaryKey(),
  name: text('name').notNull().unique(),
  color: text('color').notNull(),
  /** `scrypt` as `salt:hash` (hex). Empty for the bot. */
  passwordHash: text('password_hash').notNull(),
  /** The first registered human is the server owner. */
  isAdmin: integer('is_admin', { mode: 'boolean' }).notNull().default(false),
  isBot: integer('is_bot', { mode: 'boolean' }).notNull().default(false),
  createdAt: integer('created_at').notNull(),
})

const sessions = sqliteTable('sessions', {
  token: text('token').primaryKey(),
  userId: text('user_id')
    .notNull()
    .references(() => users.id),
  expiresAt: integer('expires_at').notNull(),
})

const channels = sqliteTable('channels', {
  /** Also the suffix of the channel's room ID (see server/rooms.ts). */
  id: text('id').primaryKey(),
  kind: text('kind', { enum: ['text', 'voice'] }).notNull(),
  name: text('name').notNull().unique(),
  topic: text('topic').notNull().default(''),
  createdAt: integer('created_at').notNull(),
})

const messages = sqliteTable(
  'messages',
  {
    /** Minted by the sender (also the client's history/live dedup key). */
    id: text('id').primaryKey(),
    channelId: text('channel_id')
      .notNull()
      .references(() => channels.id, { onDelete: 'cascade' }),
    authorId: text('author_id').notNull(),
    // Denormalized author display fields: history renders without a join and survives renames.
    authorName: text('author_name').notNull(),
    authorColor: text('author_color').notNull(),
    authorIsBot: integer('author_is_bot', { mode: 'boolean' }).notNull().default(false),
    text: text('text').notNull(),
    at: integer('at').notNull(),
  },
  (table) => [index('messages_channel_at').on(table.channelId, table.at)],
)

const dms = sqliteTable(
  'dms',
  {
    id: text('id').primaryKey(),
    /** The two user IDs, sorted and joined with `:` — one key per conversation. */
    threadKey: text('thread_key').notNull(),
    fromId: text('from_id').notNull(),
    fromName: text('from_name').notNull(),
    toId: text('to_id').notNull(),
    toName: text('to_name').notNull(),
    text: text('text').notNull(),
    at: integer('at').notNull(),
  },
  (table) => [index('dms_thread_at').on(table.threadKey, table.at)],
)
