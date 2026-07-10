export { ChannelView, MessageRow, timeOf }

import React, { useEffect, useRef, useState } from 'react'
import type { ChatMessage } from '../../shared/types'
import { loadOlderMessages, sendMessage, sendTyping, setTopic, useApp } from '../store'
import { IconHash } from './Icons'

function ChannelView() {
  const activeChannelId = useApp((s) => s.activeChannelId)
  const channels = useApp((s) => s.channels)
  const messages = useApp((s) => s.messages)
  const typing = useApp((s) => s.typing)
  const hasOlder = useApp((s) => s.hasOlder)

  const channel = channels.find((c) => c.id === activeChannelId)
  const thread = activeChannelId === null ? [] : (messages[activeChannelId] ?? [])
  const typingNames = activeChannelId === null ? [] : (typing[activeChannelId] ?? [])

  const bottomRef = useRef<HTMLDivElement>(null)
  const lastId = thread[thread.length - 1]?.id
  useEffect(() => {
    bottomRef.current?.scrollIntoView()
  }, [lastId, typingNames.length])

  if (channel === undefined) return <main className="flex-1" />

  return (
    <main className="flex-1 min-w-0 flex flex-col">
      <Header
        key={channel.id} // reset any in-progress topic edit when switching channels
        name={channel.name}
        topic={channel.topic}
        memberCount={channel.memberCount}
      />

      <div id="messages" className="flex-1 min-h-0 overflow-y-auto px-4 py-3">
        {hasOlder[channel.id] && (
          <button
            id="load-older"
            onClick={() => void loadOlderMessages(channel.id)}
            className="block mx-auto mb-3 text-[12px] text-[#949ba4] hover:text-white bg-[#2b2d31] rounded-full px-4 py-1.5 cursor-pointer"
          >
            Load older messages
          </button>
        )}
        {thread.map((message) => (
          <MessageRow key={message.id} message={message} />
        ))}
        <div ref={bottomRef} />
      </div>

      <div id="typing" className="shrink-0 h-6 px-5 text-[12px] text-[#949ba4] italic">
        {typingNames.length > 0 && `${typingNames.join(' and ')} ${typingNames.length === 1 ? 'is' : 'are'} typing…`}
      </div>

      <footer className="shrink-0 px-4 pb-5">
        <form
          onSubmit={(e) => {
            e.preventDefault()
            const input = e.currentTarget.elements.namedItem('message') as HTMLInputElement
            void sendMessage(input.value)
            input.value = ''
          }}
        >
          <input
            id="composer"
            name="message"
            autoComplete="off"
            placeholder={`Message #${channel.name} — try !help`}
            onChange={sendTyping} // ephemeral publish, throttled in the store
            className="w-full bg-[#383a40] rounded-lg px-4 py-2.5 text-[14px] text-[#dbdee1] outline-none placeholder:text-[#5c5e66]"
          />
        </form>
      </footer>
    </main>
  )
}

function Header({ name, topic, memberCount }: { name: string; topic: string; memberCount: number }) {
  const [editing, setEditing] = useState(false)
  return (
    <header className="shrink-0 h-12 flex items-center gap-2 px-4 border-b border-[#26282c] shadow-sm">
      <span className="text-[#80848e]">
        <IconHash />
      </span>
      <span id="channel-name" className="font-semibold text-white">
        {name}
      </span>
      <span className="w-px h-5 bg-[#3f4147] mx-1" />
      {editing ? (
        <form
          className="flex-1"
          onSubmit={(e) => {
            e.preventDefault()
            const input = e.currentTarget.elements.namedItem('topic') as HTMLInputElement
            void setTopic(input.value) // Room.update() under the hood — watch it propagate to every window
            setEditing(false)
          }}
        >
          <input
            id="topic-input"
            name="topic"
            defaultValue={topic}
            autoFocus
            autoComplete="off"
            placeholder="Set a topic…"
            onBlur={() => setEditing(false)}
            className="w-full bg-[#1e1f22] rounded-[4px] px-2 py-1 text-[13px] text-[#dbdee1] outline-none"
          />
        </form>
      ) : (
        <button
          id="channel-topic"
          title="Edit topic"
          onClick={() => setEditing(true)}
          className="flex-1 text-left text-[13px] text-[#949ba4] truncate cursor-pointer hover:text-[#dbdee1]"
        >
          {topic || 'Set a topic…'}
        </button>
      )}
      {/* Channel membership = connections with it open (they can publish). The bot counts too. */}
      <span
        id="channel-members"
        className="shrink-0 text-[12px] text-[#80848e]"
        title="connections with this channel open"
      >
        {memberCount} here
      </span>
    </header>
  )
}

function MessageRow({ message }: { message: ChatMessage }) {
  return (
    <div className="flex items-start gap-3 py-1 hover:bg-[#2e3035] rounded px-1 -mx-1">
      <span className="w-8 h-8 rounded-full shrink-0 mt-0.5" style={{ background: message.author.color }} />
      <div className="min-w-0">
        <div className="flex items-baseline gap-2">
          <span className="font-medium text-white">{message.author.name}</span>
          {message.author.bot && (
            <span className="text-[9px] font-semibold bg-[#5865f2] text-white rounded px-1 py-px">BOT</span>
          )}
          <span className="text-[11px] text-[#5c5e66]">{timeOf(message.at)}</span>
        </div>
        <div className="text-[#dbdee1] break-words [overflow-wrap:anywhere]">{message.text}</div>
      </div>
    </div>
  )
}

function timeOf(at: number): string {
  return new Date(at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}
