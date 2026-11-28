export { onJoinChat, onLoadHistory, onJoinChatWriter, onWriterPublishes, onHoldSave, onReleaseSave, onSavedTexts }

import { Room, type LocalParticipant } from '@telefunc/room'

// The Room guide's chat example ("Load chat history" in docs/pages/room/+Page.mdx) as written there, with `db` a list in
// memory and `getContext().user` an argument. A test may hold the save of one message, and joins a writer on the server.

type ChatMessage = { id: string; text: string }
type SavedMessage = ChatMessage & { roomId: string; authorId: string | null }

const saved = new Map<string, SavedMessage[]>()
const heldSaves = new Map<string, PromiseWithResolvers<void>>()
const db = {
  messages: {
    async insert(message: SavedMessage) {
      await heldSaves.get(message.text)?.promise
      const list = saved.get(message.roomId) ?? []
      saved.set(message.roomId, [...list, message])
    },
    async latest(roomId: string, count: number) {
      return (saved.get(roomId) ?? []).slice(-count)
    },
  },
}

async function onJoinChat(roomId: string, user: { id: string; name: string }) {
  const room = await Room.getOrCreate<Record<string, never>, { name: string }, ChatMessage>(roomId)
  Room.guard(room, {
    anyoneCanJoin: true,
    // Saved before it is sent, so that a history loaded once it was sent has it.
    onBeforePublish: async (from, message) => {
      if (!isChatMessage(message)) return // TypeScript's narrowing; the room's type has the shield refuse any other
      await db.messages.insert({ ...message, roomId, authorId: from.identity })
    },
  })
  const me = await room.join({ meta: { name: user.name }, identity: user.id })
  return { room, me }
}

function isChatMessage(data: unknown): data is ChatMessage {
  return (
    typeof data === 'object' &&
    data !== null &&
    'id' in data &&
    typeof data.id === 'string' &&
    'text' in data &&
    typeof data.text === 'string'
  )
}

async function onLoadHistory(roomId: string) {
  return await db.messages.latest(roomId, 50) // oldest first
}

/** A server member who sends through the example's `onJoinChat()`, so its messages are saved as a page's are. */
const writers = new Map<string, LocalParticipant<{ name: string }, ChatMessage>>()

async function onJoinChatWriter(roomId: string) {
  const { me } = await onJoinChat(roomId, { id: 'writer', name: 'Writer' })
  writers.set(roomId, me)
}

/** Resolves once each is sent, but for one whose save is held: that one is sent once the test releases its save. */
async function onWriterPublishes(roomId: string, texts: string[]) {
  const writer = writers.get(roomId)
  if (!writer) throw new Error(`No writer joined chat ${roomId}`)
  for (const text of texts) {
    const sent = writer.publish({ id: crypto.randomUUID(), text })
    if (heldSaves.has(text)) void sent.catch((err) => console.error('chat-history: a held message failed to send', err))
    else await sent
  }
}

async function onHoldSave(text: string) {
  heldSaves.set(text, Promise.withResolvers<void>())
}

async function onReleaseSave(text: string) {
  heldSaves.get(text)?.resolve()
  heldSaves.delete(text)
}

async function onSavedTexts(roomId: string) {
  return (saved.get(roomId) ?? []).map((message) => message.text)
}
