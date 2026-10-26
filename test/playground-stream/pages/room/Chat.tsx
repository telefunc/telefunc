export { Chat }

import React, { useEffect, useState, useSyncExternalStore } from 'react'
import { onGetChat, onSignIn, onWatchChat, onWhoIsOnline, onGetChatMember } from './Chat.telefunc'

type ChatRoom = Awaited<ReturnType<typeof onGetChat>>
type Me = Awaited<ReturnType<ChatRoom['join']>>
type Profile = Awaited<ReturnType<typeof onGetChatMember>>

/** A chat at `?chat=<id>&as=<name>`: join as a guest, sign in, or watch unseen. */
function Chat() {
  const [joined, setJoined] = useState<{ chat: ChatRoom; me: Me } | null>(null)
  const [params, setParams] = useState<URLSearchParams | null>(null)
  useEffect(() => setParams(new URLSearchParams(location.search)), [])
  const chatId = params?.get('chat')
  if (!chatId) return null
  const name = params!.get('as')!
  if (joined) return <ChatView chat={joined.chat} me={joined.me} />
  const joinAsGuest = async () => {
    const chat = await onGetChat(chatId)
    setJoined({ chat, me: await chat.join({ meta: { name } }) })
  }
  return (
    <div>
      <h2>Chat</h2>
      <button id="chat-join" onClick={joinAsGuest}>
        Join as a guest
      </button>
      <button id="chat-sign-in" onClick={async () => setJoined(await onSignIn(chatId, name))}>
        Sign in
      </button>
      <button id="chat-watch" onClick={async () => setJoined(await onWatchChat(chatId))}>
        Watch
      </button>
    </div>
  )
}

function ChatView({ chat, me }: { chat: ChatRoom; me: Me }) {
  const { participants, count } = useSyncExternalStore(chat.onChange, chat.snapshot)
  const [isConnected, setIsConnected] = useState(me.isConnected)
  const [activity, setActivity] = useState<string[]>([])
  const [online, setOnline] = useState<string[] | null>(null)
  const [profile, setProfile] = useState<Profile | null>(null)
  const log = (line: string) => setActivity((lines) => [...lines, line])
  useEffect(() => {
    const unlisten = [
      me.onDisconnect(() => setIsConnected(me.isConnected)),
      me.onReconnect(() => setIsConnected(me.isConnected)),
      chat.onAnnounce((text) => log(String(text))),
      chat.onLeave((member) => log(`${member.meta.name} left`)),
    ]
    return () => unlisten.forEach((off) => off())
  }, [chat, me])
  return (
    <div>
      <h2>Chat</h2>
      <p id="chat-status">{isConnected ? 'Connected' : 'Reconnecting…'}</p>
      <p id="chat-count">{count} in the chat</p>
      <ul id="chat-members">
        {participants.map((member) => (
          <li key={member.id}>
            <MemberActivity chat={chat} id={member.id} name={member.meta.name} log={log} />
            <button type="button" onClick={async () => setProfile(await onGetChatMember(chat.id, member.id))}>
              {member.meta.name}
              {member.id === me.id && ' (you)'}
              {!member.isConnected && ' (reconnecting…)'}
            </button>
          </li>
        ))}
      </ul>
      {profile && <ProfileCard profile={profile} />}
      <button id="chat-who" onClick={async () => setOnline(await onWhoIsOnline(chat.id))}>
        Who's online
      </button>
      {online && <p id="chat-online">{online.join(', ')}</p>}
      <ul id="chat-activity">
        {activity.map((line, i) => (
          <li key={i}>{line}</li>
        ))}
      </ul>
    </div>
  )
}

/** Tells the chat's activity when a member's page drops and comes back. */
function MemberActivity({
  chat,
  id,
  name,
  log,
}: { chat: ChatRoom; id: string; name: string; log: (line: string) => void }) {
  useEffect(() => {
    let unlisten = () => {}
    void chat.getParticipant(id).then((member) => {
      if (!member) return
      const offDisconnect = member.onDisconnect(() => log(`${name} lost connection`))
      const offReconnect = member.onReconnect(() => log(`${name} is connected again`))
      unlisten = () => {
        offDisconnect()
        offReconnect()
      }
    })
    return () => unlisten()
  }, [chat, id])
  return null
}

function ProfileCard({ profile: { chat, member } }: { profile: Profile }) {
  useSyncExternalStore(chat.onChange, chat.snapshot)
  if (!member) return null
  return <p id="chat-profile">{`${member.meta.name}: ${member.isConnected ? 'online' : 'reconnecting…'}`}</p>
}
