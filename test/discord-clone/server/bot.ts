export { startBot }

// RoomBot — a server-side guild member. The point: the *same* Room API drives it (join,
// subscribe, publish), there's just no browser attached. It greets newcomers in #general and
// answers a few commands in every text channel. (Its DM replies live in dms.telefunc.ts — DMs
// are server-delivered in this app, they never ride the member-to-member lane.)

import type { LocalParticipant } from 'telefunc'
import * as q from '../database/queries'
import type { ChannelPublish, GuildAnnouncement, GuildRoom, MemberMeta } from '../shared/types'
import { BANNED_WORDS, getGuardedChannel, getGuardedGuild } from './guards'
import { BOT_COLOR, BOT_NAME, channelRoomId } from './rooms'

const HELP = ['!help', '!ping — pong', '!members — who is online', '!roll — roll a d20'].join(' · ')

async function startBot(): Promise<void> {
  const botUser = q.theBotUser()
  if (botUser === undefined) throw new Error('Bot user missing — seed the database first')
  const meta: MemberMeta = { name: BOT_NAME, color: BOT_COLOR, status: 'online', bot: true }

  const guild = await getGuardedGuild()
  await guild.join(meta, { identity: botUser.id })

  // Watch every text channel — current ones now (awaited: the first user's join event must
  // find the bot already able to speak in #general), future ones as the guild announces them.
  await Promise.all(q.listChannels().map((channel) => watchChannel(channelRoomId(channel.id), guild, botUser.id, meta)))
  guild.onAnnounce((data) => {
    const event = data as GuildAnnouncement
    if (event.kind === 'channel-created') void watchChannel(event.channelId, guild, botUser.id, meta)
  })

  // Greet each user's first appearance — in #general, not by DM: a reactive DM can race the
  // joiner's listeners (private deliveries are live-only), while a channel message is persisted
  // by the guard and replayed from history. See README finding on early messages.
  const greeted = new Set<string>()
  const generalId = q.getChannelByName('general')?.id
  guild.onJoin((member) => {
    if (member.meta.bot || member.identity === null || greeted.has(member.identity)) return
    greeted.add(member.identity)
    if (generalId) void say(channelRoomId(generalId), `Welcome @${member.meta.name}! Type !help in any channel.`)
  })
}

// --- Text channels: the bot is a hidden participant in each channel, commands via subscribe ---

const channelBots = new Map<string, LocalParticipant>() // channel room ID → the bot's hidden participant

async function watchChannel(roomId: string, guild: GuildRoom, botUserId: string, meta: MemberMeta): Promise<void> {
  if (channelBots.has(roomId)) return
  const channel = await getGuardedChannel(roomId)
  if (channel.meta.kind !== 'text') return

  // The bot joins each channel as a **hidden participant** (`{ hidden: true }`) — a full participant
  // (it publishes replies and subscribes to commands) but excluded from presence, so it doesn't
  // occupy a seat in the channel's count/roster. Exactly the "a bot, a command sink" case hidden
  // participants are for. The bot stays a *visible* member of the guild room (its guild join is a
  // normal identity join) — Discord shows bots in the member list.
  const botMe = await channel.join(meta, { identity: botUserId, hidden: true })
  channelBots.set(roomId, botMe)
  channel.onClose(() => channelBots.delete(roomId)) // channel deleted

  channel.subscribe((published, _info, from) => {
    // `published` is typed `ChannelPublish` — the guarded channel carries its publish type (finding 20).
    if (published.kind !== 'chat') return
    if (from.meta.bot) return // never react to bots (that includes myself)
    void handleCommand(published.text, roomId, guild, from.meta.name)
  })
}

async function say(roomId: string, text: string): Promise<void> {
  const botMe = channelBots.get(roomId)
  if (botMe === undefined) return
  await botMe.publish({ kind: 'chat', id: crypto.randomUUID(), text } satisfies ChannelPublish)
}

async function handleCommand(text: string, roomId: string, guild: GuildRoom, fromName: string): Promise<void> {
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
