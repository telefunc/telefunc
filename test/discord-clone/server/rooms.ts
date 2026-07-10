export { BOT_COLOR, BOT_NAME, channelRoomId, dbChannelIdOf, ensureLiveWorld, GUILD_ROOM_ID, VOICE_CHANNEL_SIZE }

// The database is the durable truth; rooms are the live layer on top. This module makes sure
// the live layer exists: one guild room, one room per `channels` row, and the bot.

import { randomUUID } from 'node:crypto'
import { eq } from 'drizzle-orm'
import { Room } from 'telefunc'
import { db } from '../database/db'
import { channels, users } from '../database/schema'

const GUILD_ROOM_ID = 'discord:guild'
const CHANNEL_ROOM_PREFIX = 'discord:channel:'
const VOICE_CHANNEL_SIZE = 8
const BOT_NAME = 'RoomBot'
const BOT_COLOR = '#5865f2'

function channelRoomId(dbChannelId: string): string {
  return CHANNEL_ROOM_PREFIX + dbChannelId
}

function dbChannelIdOf(roomId: string): string | null {
  return roomId.startsWith(CHANNEL_ROOM_PREFIX) ? roomId.slice(CHANNEL_ROOM_PREFIX.length) : null
}

/** Boot latch — on `globalThis` so dev-server module reloads don't re-run it. */
async function ensureLiveWorld(): Promise<void> {
  const g = globalThis as { __discordWorldReady?: Promise<void> }
  g.__discordWorldReady ??= boot()
  await g.__discordWorldReady
}

async function boot(): Promise<void> {
  seedDatabase()

  // `Room.create()` throws if the room exists and there is no upsert — seeding after a server
  // restart is a create-and-tolerate dance (see README finding).
  await createIfMissing(GUILD_ROOM_ID, { meta: { name: 'Telefunc HQ' } })
  for (const channel of db.select().from(channels).all()) {
    await createIfMissing(
      channelRoomId(channel.id),
      channel.kind === 'voice'
        ? // `size` exercises the capacity hint (`isFull`); `isolated` gives each member their own
          // upstream key (removes publish contention on Cloudflare; invisible on this server).
          { meta: { kind: 'voice', name: channel.name }, size: VOICE_CHANNEL_SIZE, isolated: true }
        : { meta: { kind: 'text', name: channel.name, topic: channel.topic } },
    )
  }

  // Lazy import: the bot builds on rooms + guards, which import this module.
  await (await import('./bot')).startBot()
}

function seedDatabase(): void {
  if (db.select().from(users).where(eq(users.isBot, true)).limit(1).all().length === 0) {
    db.insert(users)
      .values({
        id: randomUUID(),
        name: BOT_NAME,
        color: BOT_COLOR,
        passwordHash: '', // never logs in
        isAdmin: false,
        isBot: true,
        createdAt: Date.now(),
      })
      .run()
  }
  if (db.select().from(channels).limit(1).all().length === 0) {
    const now = Date.now()
    db.insert(channels)
      .values([
        { id: randomUUID(), kind: 'text', name: 'general', topic: 'Anything goes', createdAt: now },
        { id: randomUUID(), kind: 'text', name: 'help', topic: 'Ask RoomBot: !help', createdAt: now },
        { id: randomUUID(), kind: 'voice', name: 'lounge', topic: '', createdAt: now },
      ])
      .run()
  }
}

async function createIfMissing(roomId: string, options: Parameters<typeof Room.create>[1]): Promise<void> {
  try {
    await Room.create(roomId, options)
  } catch {
    // Already exists (previous boot, another instance) — fine either way.
  }
}
