export { onGetChat, onGetMembersChat, onSignIn, onWatchChat, onWhoIsOnline, onGetChatMember }

import { Room, Abort } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'

async function onGetChat(chatId: string) {
  return await Room.getOrCreate<Record<string, never>, { name: string }>(chatId)
}

/** A members-only chat: each join waits for the membership service to say the name is a member. */
async function onGetMembersChat(chatId: string) {
  const chat = await onGetChat(chatId)
  Room.guard(chat, {
    onBeforeJoin: async ({ meta }) => {
      if (!(await isMember(chatId, meta.name))) throw Abort(`${meta.name} is not a member`)
    },
  })
  return chat
}

/** The membership service, which answers once the test runs `chatMember_<chatId>_<name>` with `runServerAction()`. */
function isMember(chatId: string, name: string): Promise<boolean> {
  const key = `chatMember_${chatId}_${name}`
  cleanupState[key] = 'asked'
  return new Promise((resolve) => onServerAction(key, () => resolve(true)))
}

/** The server joins you under your account, and tells the chat when your page drops and comes back. */
async function onSignIn(chatId: string, name: string) {
  const chat = await onGetChat(chatId)
  const me = await chat.join({ meta: { name }, identity: name })
  me.onDisconnect(() => void Room.announce(chatId, `${name} went away`))
  me.onReconnect(() => void Room.announce(chatId, `${name} is back`))
  return { chat, me }
}

/** Watch without being seen. */
async function onWatchChat(chatId: string) {
  const chat = await onGetChat(chatId)
  return { chat, me: await chat.join({ meta: { name: 'Viewer' }, hidden: true }) }
}

async function onWhoIsOnline(chatId: string) {
  const members = await Room.getParticipants(chatId)
  return members.map(({ meta, isConnected }) => `${meta.name}${isConnected ? '' : ' (away)'}`).sort()
}

async function onGetChatMember(chatId: string, memberId: string) {
  const chat = await onGetChat(chatId)
  return { chat, member: await chat.getParticipant(memberId) }
}
