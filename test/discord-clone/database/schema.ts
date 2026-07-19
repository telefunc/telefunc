// Row types + DDL — the durable truth. Rooms are the *live* layer (presence, delivery,
// events); everything that must survive a restart lives here.
//
// This app uses Node's built-in SQLite (`node:sqlite`, Node ≥ 22.5) on purpose: a native
// driver (better-sqlite3) broke `pnpm install` for the whole monorepo on Windows CI runners,
// and no ORM release supported `node:sqlite` yet — so the queries are hand-written prepared
// statements in queries.ts.

export { DDL }
export type { ChannelRow, DmRow, MessageRow, SessionRow, UserRow }

type UserRow = {
  id: string
  name: string
  color: string
  /** `scrypt` as `salt:hash` (hex). Empty for the bot. */
  password_hash: string
  /** The first registered human owns the server. SQLite booleans are 0/1. */
  is_admin: 0 | 1
  is_bot: 0 | 1
  created_at: number
}

type SessionRow = {
  token: string
  user_id: string
  expires_at: number
}

type ChannelRow = {
  /** Also the suffix of the channel's room ID (see server/rooms.ts). */
  id: string
  kind: 'text' | 'voice'
  name: string
  topic: string
  created_at: number
}

type MessageRow = {
  /** Minted by the sender (also the client's history/live dedup key). */
  id: string
  channel_id: string
  author_id: string
  // Denormalized author display fields: history renders without a join and survives renames.
  author_name: string
  author_color: string
  author_is_bot: 0 | 1
  text: string
  /** The room's authoritative per-channel order, from the `onAfterPublish` receipt (`info.seq`).
   *  History is ordered and paginated by this, so it reads exactly as the live lane did. */
  seq: number
  /** Central server clock (`info.timestamp`) — one clock shared with the live render. */
  at: number
}

type DmRow = {
  id: string
  /** The two user IDs, sorted and joined with `:` — one key per conversation. */
  thread_key: string
  from_id: string
  from_name: string
  to_id: string
  to_name: string
  text: string
  at: number
}

const DDL = `
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    color TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    is_bot INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    expires_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    name TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    author_id TEXT NOT NULL,
    author_name TEXT NOT NULL,
    author_color TEXT NOT NULL,
    author_is_bot INTEGER NOT NULL DEFAULT 0,
    text TEXT NOT NULL,
    seq INTEGER NOT NULL,
    at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS messages_channel_seq ON messages(channel_id, seq);
  CREATE TABLE IF NOT EXISTS dms (
    id TEXT PRIMARY KEY,
    thread_key TEXT NOT NULL,
    from_id TEXT NOT NULL,
    from_name TEXT NOT NULL,
    to_id TEXT NOT NULL,
    to_name TEXT NOT NULL,
    text TEXT NOT NULL,
    at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS dms_thread_at ON dms(thread_key, at);
`
