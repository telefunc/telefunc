export { BOT_COLOR, BOT_NAME, channelRoomId, dbChannelIdOf, ensureLiveWorld, GUILD_ROOM_ID, VOICE_CHANNEL_SIZE }

// The database is the durable truth; rooms are the live layer on top. This module makes sure
// the live layer exists: one guild room, one room per `channels` row, and the bot.

import { randomUUID } from 'node:crypto'
import { Room } from 'telefunc'
import * as q from '../database/queries'
import type { ChannelMeta, MemberMeta } from '../shared/types'

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

/** Boot latch — on `globalThis` so dev-server module reloads don't re-run it. On failure the
 *  latch is cleared, so a transient boot error (a KV blip, the bot failing to start) doesn't
 *  memoize a rejected promise that bricks every later `ensureLiveWorld()` for the process. */
async function ensureLiveWorld(): Promise<void> {
  const g = globalThis as { __discordWorldReady?: Promise<void> }
  g.__discordWorldReady ??= boot().catch((err) => {
    g.__discordWorldReady = undefined
    throw err
  })
  await g.__discordWorldReady
}

async function boot(): Promise<void> {
  seedDatabase()

  // Idempotent seeding — `Room.getOrCreate()` replaced the create-and-swallow-the-exists-error
  // dance this used to need (README finding 13, fixed upstream).
  await Room.getOrCreate(GUILD_ROOM_ID, { meta: { name: 'Telefunc HQ' } })
  for (const channel of q.listChannels()) {
    await Room.getOrCreate<ChannelMeta, MemberMeta>(
      channelRoomId(channel.id),
      channel.kind === 'voice'
        ? // `size` is enforced by the voice rooms' `onJoin` guard (see guards.ts); `isolated`
          // gives each member their own upstream key (removes publish contention on Cloudflare).
          { meta: { kind: 'voice', name: channel.name }, size: VOICE_CHANNEL_SIZE, isolated: true }
        : { meta: { kind: 'text', name: channel.name, topic: channel.topic } },
    )
  }

  // Lazy import: the bot builds on rooms + guards, which import this module.
  await (await import('./bot')).startBot()
}

function seedDatabase(): void {
  if (q.theBotUser() === undefined) {
    q.insertUser({
      id: randomUUID(),
      name: BOT_NAME,
      color: BOT_COLOR,
      password_hash: '', // never logs in
      is_admin: 0,
      is_bot: 1,
      created_at: Date.now(),
    })
  }
  if (q.listChannels().length === 0) {
    const now = Date.now()
    q.insertChannel({ id: randomUUID(), kind: 'text', name: 'general', topic: 'Anything goes', created_at: now })
    q.insertChannel({ id: randomUUID(), kind: 'text', name: 'help', topic: 'Ask RoomBot: !help', created_at: now })
    q.insertChannel({ id: randomUUID(), kind: 'voice', name: 'lounge', topic: '', created_at: now })
  }
}
