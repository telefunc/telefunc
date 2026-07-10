export { startBot }

// RoomBot — a server-side guild member. The point: the *same* Room API drives it (join,
// subscribe, publish), there's just no browser attached. It greets newcomers in #general and
// answers a few commands in every text channel. (Its DM replies live in dms.telefunc.ts — DMs
// are server-delivered in this app, they never ride the member-to-member lane.)

import { eq } from 'drizzle-orm'
import { Room, type LocalParticipant } from 'telefunc'
import { db } from '../database/db'
import { channels, users } from '../database/schema'
import { asChannelMeta, asMemberMeta, type ChannelPublish, type GuildAnnouncement } from '../shared/types'
import { BANNED_WORDS, getGuardedChannel, getGuardedGuild } from './guards'
import { BOT_COLOR, BOT_NAME, channelRoomId } from './rooms'

const HELP = ['!help', '!ping — pong', '!members — who is online', '!roll — roll a d20'].join(' · ')

async function startBot(): Promise<void> {
  const botUser = db.select().from(users).where(eq(users.isBot, true)).limit(1).all()[0]
  if (botUser === undefined) throw new Error('Bot user missing — seed the database first')
  const identity = { userId: botUser.id, name: BOT_NAME, color: BOT_COLOR, bot: true }

  const guild = await getGuardedGuild()
  await guild.join({ ...identity, status: 'online' })

  // Watch every text channel — current ones now (awaited: the first user's join event must
  // find the bot already able to speak in #general), future ones as the guild announces them.
  await Promise.all(
    db
      .select()
      .from(channels)
      .all()
      .map((channel) => watchChannel(channelRoomId(channel.id), guild, identity)),
  )
  guild.onAnnounce((data) => {
    const event = data as GuildAnnouncement
    if (event.kind === 'channel-created') void watchChannel(event.channelId, guild, identity)
  })

  // Greet each user's first appearance — in #general, not by DM: a reactive DM can race the
  // joiner's listeners (private deliveries are live-only), while a channel message is persisted
  // by the guard and replayed from history. See README finding on early messages.
  const greeted = new Set<string>()
  const generalId = db.select().from(channels).where(eq(channels.name, 'general')).all()[0]?.id
  guild.onJoin((member) => {
    const meta = asMemberMeta(member.meta)
    if (meta.bot || greeted.has(meta.userId)) return
    greeted.add(meta.userId)
    if (generalId) void say(channelRoomId(generalId), `Welcome @${meta.name}! Type !help in any channel.`)
  })
}

// --- Text channels: one bot membership each, commands via subscribe ---

const channelBots = new Map<string, LocalParticipant>() // channel room ID → the bot's membership

async function watchChannel(
  roomId: string,
  guild: Room,
  identity: { userId: string; name: string; color: string; bot: boolean },
): Promise<void> {
  if (channelBots.has(roomId)) return
  const channel = await getGuardedChannel(roomId)
  if (asChannelMeta(channel.meta).kind !== 'text') return

  const botMe = await channel.join(identity)
  channelBots.set(roomId, botMe)
  channel.onClose(() => channelBots.delete(roomId)) // channel deleted

  channel.subscribe((data, _info, from) => {
    const published = data as ChannelPublish
    if (published.kind !== 'chat') return
    if (asMemberMeta(from.meta).bot) return // never react to bots (that includes myself)
    void handleCommand(published.text, roomId, guild, asMemberMeta(from.meta).name)
  })
}

async function say(roomId: string, text: string): Promise<void> {
  const botMe = channelBots.get(roomId)
  if (botMe === undefined) return
  await botMe.publish({ kind: 'chat', id: crypto.randomUUID(), text } satisfies ChannelPublish)
}

async function handleCommand(text: string, roomId: string, guild: Room, fromName: string): Promise<void> {
  if (!text.startsWith('!')) return
  const [command = ''] = text.slice(1).split(' ')
  try {
    switch (command) {
      case 'help':
        return await say(roomId, HELP)
      case 'ping':
        return await say(roomId, 'pong')
      case 'members':
        return await say(roomId, `${guild.count} member connection(s) online right now`) // live, event-driven
      case 'roll':
        return await say(roomId, `@${fromName} rolled a ${1 + Math.floor(Math.random() * 20)}`)
      default:
        return await say(roomId, `Unknown command !${command} — try !help`)
    }
  } catch (err) {
    await say(roomId, `That didn't work: ${errorText(err)}`).catch(() => {})
  }
}

/** Guard errors quote the banned word — mask it or the bot's own report gets guard-rejected. */
function errorText(err: unknown): string {
  const text = err instanceof Error ? err.message : String(err)
  return text.replace(/[a-z]+/gi, (word) =>
    BANNED_WORDS.includes(word.toLowerCase()) ? `${word[0]}${'*'.repeat(word.length - 1)}` : word,
  )
}
