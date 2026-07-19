export { Sidebar }

import React, { useState } from 'react'
import { joinCall, leaveCall, toggleMute } from '../call'
import type { ChannelSnapshot, Member } from '../store'
import {
  announce,
  createChannel,
  deleteChannel,
  logout,
  openCall,
  openChannel,
  openDm,
  setStatus,
  useApp,
} from '../store'
import { IconLogOut, IconMic, IconMicOff, IconPhoneOff, IconPlus, IconVideo, IconVolume, IconX } from './Icons'

/** The second column: the guild's channels — or, on the Home rail, the DM conversation list. */
function Sidebar() {
  const view = useApp((s) => s.view)
  return (
    <nav className="shrink-0 w-60 bg-[#2b2d31] flex flex-col">
      {view.kind === 'dm' ? <DmList /> : <GuildChannels />}
      <CallDock />
      <UserFooter />
    </nav>
  )
}

// ---------------------------------------------------------------------------
// Guild mode: text & voice channels
// ---------------------------------------------------------------------------

function GuildChannels() {
  const channels = useApp((s) => s.channels)
  const me = useApp((s) => s.me)
  const textChannels = channels.filter((c) => c.kind === 'text')
  const voiceChannels = channels.filter((c) => c.kind === 'voice')

  return (
    <>
      <header className="shrink-0 h-12 flex items-center px-4 font-semibold text-white border-b border-[#1f2124] shadow-sm">
        Telefunc HQ
      </header>
      <div className="flex-1 min-h-0 overflow-y-auto px-2 py-3">
        <Section title="Text channels" addId="add-text-channel" onAdd={(name) => void createChannel('text', name)}>
          {textChannels.map((channel) => (
            <TextChannelRow key={channel.id} channel={channel} isAdmin={me?.admin === true} />
          ))}
        </Section>

        <div className="mt-4" />
        <Section title="Voice channels" addId="add-voice-channel" onAdd={(name) => void createChannel('voice', name)}>
          {voiceChannels.map((channel) => (
            <VoiceChannelRow key={channel.id} channel={channel} />
          ))}
        </Section>

        {me?.admin && (
          <>
            <div className="mt-4" />
            <div className="px-2 pb-1 text-[11px] font-semibold tracking-wide text-[#949ba4] uppercase">Moderation</div>
            <form
              className="px-2"
              onSubmit={(e) => {
                e.preventDefault()
                const input = e.currentTarget.elements.namedItem('announcement') as HTMLInputElement
                if (input.value.trim()) void announce(input.value)
                input.value = ''
              }}
            >
              <input
                id="announce-input"
                name="announcement"
                placeholder="Announce to everyone…"
                autoComplete="off"
                className="w-full bg-[#1e1f22] rounded-[4px] px-2 py-1.5 text-[12px] text-[#dbdee1] outline-none placeholder:text-[#5c5e66]"
              />
            </form>
          </>
        )}
      </div>
    </>
  )
}

/** Section header with a + that reveals an inline create form. */
function Section({
  title,
  addId,
  onAdd,
  children,
}: {
  title: string
  addId: string
  onAdd: (name: string) => void
  children: React.ReactNode
}) {
  const [adding, setAdding] = useState(false)
  return (
    <div>
      <div className="flex items-center px-2 pb-1">
        <span className="flex-1 text-[11px] font-semibold tracking-wide text-[#949ba4] uppercase">{title}</span>
        <button
          id={addId}
          title={`Create ${title.toLowerCase().replace(/s$/, '')}`}
          onClick={() => setAdding((v) => !v)}
          className="text-[#949ba4] hover:text-white cursor-pointer"
        >
          <IconPlus size={14} />
        </button>
      </div>
      {adding && (
        <form
          className="px-2 pb-1"
          onSubmit={(e) => {
            e.preventDefault()
            const input = e.currentTarget.elements.namedItem('name') as HTMLInputElement
            if (input.value.trim()) onAdd(input.value)
            input.value = ''
            setAdding(false)
          }}
        >
          <input
            id={`${addId}-name`}
            name="name"
            autoFocus
            autoComplete="off"
            placeholder="new-channel"
            onBlur={() => setAdding(false)}
            className="w-full bg-[#1e1f22] rounded-[4px] px-2 py-1.5 text-[13px] text-[#dbdee1] outline-none placeholder:text-[#5c5e66]"
          />
        </form>
      )}
      {children}
    </div>
  )
}

function TextChannelRow({ channel, isAdmin }: { channel: ChannelSnapshot; isAdmin: boolean }) {
  const activeChannelId = useApp((s) => s.activeChannelId)
  const view = useApp((s) => s.view)
  const unread = useApp((s) => s.unread[channel.id] === true)
  const active = view.kind === 'channel' && activeChannelId === channel.id
  return (
    <div
      data-channel={channel.name}
      data-active={active || undefined}
      className={`group flex items-center gap-1.5 px-2 py-1.5 rounded-[4px] cursor-pointer ${
        active ? 'bg-[#404249] text-white' : 'text-[#949ba4] hover:bg-[#35373c] hover:text-[#dbdee1]'
      }`}
      onClick={() => void openChannel(channel.id)}
    >
      <span className="text-[#80848e] shrink-0 font-medium">#</span>
      <span className={`flex-1 truncate ${unread && !active ? 'text-white font-medium' : ''}`}>{channel.name}</span>
      {/* Unread dot — fed by the room's body-free activity signal, like Discord's plain-unread pill. */}
      {unread && (
        <span data-unread-for={channel.name} className="shrink-0 w-2 h-2 rounded-full bg-[#f2f3f5]" title="unread" />
      )}
      {isAdmin && channel.name !== 'general' && (
        <button
          data-delete-channel={channel.name}
          title="Delete channel"
          className="hidden group-hover:block shrink-0 text-[#949ba4] hover:text-[#fa777c] cursor-pointer"
          onClick={(e) => {
            e.stopPropagation()
            void deleteChannel(channel.id)
          }}
        >
          <IconX size={14} />
        </button>
      )}
    </div>
  )
}

function VoiceChannelRow({ channel }: { channel: ChannelSnapshot }) {
  const call = useApp((s) => s.call)
  const connected = call?.channelId === channel.id
  return (
    <div data-channel={channel.name} className="rounded-[4px]">
      <div
        className="flex items-center gap-1.5 px-2 py-1.5 rounded-[4px] cursor-pointer text-[#949ba4] hover:bg-[#35373c] hover:text-[#dbdee1]"
        // Click to join — or, if already connected, to bring the call into the main pane.
        onClick={() => (connected ? openCall() : void joinCall(channel.id).then(openCall))}
        data-join-voice={channel.name}
      >
        <span className="shrink-0 text-[#80848e]">
          <IconVolume size={16} />
        </span>
        <span className="flex-1 truncate">{channel.name}</span>
        <span className="text-[11px] text-[#80848e]">
          {channel.occupants.length}/{channel.size}
        </span>
      </div>
      {/* Everyone sees who's connected — guild-wide presence; no media flows to observers. */}
      {channel.occupants.map((occupant) => (
        <div
          key={occupant.participantId}
          data-voice-occupant={occupant.name}
          className="flex items-center gap-1.5 pl-7 pr-2 py-0.5 text-[13px] text-[#949ba4]"
        >
          <span className="w-2 h-2 rounded-full shrink-0" style={{ background: occupant.color }} />
          <span className="truncate flex-1">{occupant.name}</span>
          {occupant.camera && (
            <span className="text-[#23a559]" title="camera on">
              <IconVideo size={13} />
            </span>
          )}
          {occupant.muted && (
            <span data-muted className="text-[#80848e]" title="muted">
              <IconMicOff size={13} />
            </span>
          )}
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Home mode: direct-message conversations
// ---------------------------------------------------------------------------

function DmList() {
  const dmThreads = useApp((s) => s.dmThreads)
  const dmUnread = useApp((s) => s.dmUnread)
  const members = useApp((s) => s.members)
  const view = useApp((s) => s.view)
  const conversations = Object.values(dmThreads).sort((a, b) => b.lastAt - a.lastAt)
  const activeUserId = view.kind === 'dm' ? view.withUserId : null

  return (
    <>
      <header className="shrink-0 h-12 flex items-center px-4 font-semibold text-white border-b border-[#1f2124] shadow-sm">
        Direct Messages
      </header>
      <div className="flex-1 min-h-0 overflow-y-auto px-2 py-3">
        {conversations.length === 0 && (
          <div className="px-2 text-[13px] text-[#5c5e66]">
            No conversations yet — message someone from the member list.
          </div>
        )}
        {conversations.map((thread) => {
          const online = members.find((m) => m.userId === thread.userId)
          const unread = dmUnread[thread.userId] ?? 0
          return (
            <div
              key={thread.userId}
              data-dm-conversation={thread.name}
              className={`flex items-center gap-2.5 px-2 py-1.5 rounded-[4px] cursor-pointer ${
                activeUserId === thread.userId ? 'bg-[#404249] text-white' : 'text-[#949ba4] hover:bg-[#35373c]'
              }`}
              onClick={() => void openDm(thread.userId, thread.name)}
            >
              <span className="w-8 h-8 rounded-full shrink-0" style={{ background: online?.color ?? '#5c5e66' }} />
              <span className="flex-1 min-w-0">
                <span className="block text-[14px] text-[#dbdee1] truncate">{thread.name}</span>
                <span className="block text-[11px] text-[#80848e] truncate">{thread.lastText}</span>
              </span>
              {unread > 0 && (
                <span
                  data-dm-unread-for={thread.name}
                  className="shrink-0 min-w-[18px] h-[18px] px-1 rounded-full bg-[#f23f43] text-white text-[11px] font-semibold flex items-center justify-center"
                >
                  {unread}
                </span>
              )}
            </div>
          )
        })}
      </div>
    </>
  )
}

// ---------------------------------------------------------------------------
// Call dock & user footer
// ---------------------------------------------------------------------------

/** Shown while connected to voice: status, quick mute, disconnect. Full controls live in the call view. */
function CallDock() {
  const call = useApp((s) => s.call)
  const channels = useApp((s) => s.channels)
  if (!call) return null
  const channel = channels.find((c) => c.id === call.channelId)
  return (
    <div id="call-dock" className="shrink-0 px-3 py-2 bg-[#232428] border-t border-[#1f2124]">
      <div className="flex items-center gap-2">
        <button onClick={openCall} className="flex-1 min-w-0 text-left cursor-pointer" title="Open call">
          <span className="block text-[13px] font-medium text-[#23a559]">Voice connected</span>
          <span className="block text-[11px] text-[#949ba4] truncate">{channel?.name}</span>
        </button>
        <button
          id="dock-mute"
          title={call.muted ? 'Unmute' : 'Mute'}
          onClick={() => void toggleMute()}
          className={`p-1.5 rounded cursor-pointer hover:bg-[#35373c] ${call.muted ? 'text-[#f23f43]' : 'text-[#dbdee1]'}`}
        >
          {call.muted ? <IconMicOff /> : <IconMic />}
        </button>
        <button
          id="dock-leave"
          title="Disconnect"
          onClick={() => void leaveCall()}
          className="p-1.5 rounded cursor-pointer text-[#dbdee1] hover:bg-[#35373c] hover:text-[#f23f43]"
        >
          <IconPhoneOff />
        </button>
      </div>
    </div>
  )
}

function UserFooter() {
  const me = useApp((s) => s.me)
  if (!me) return null
  return (
    <footer className="shrink-0 flex items-center gap-2 px-3 py-2.5 bg-[#232428]">
      <span className="relative shrink-0">
        <span className="block w-8 h-8 rounded-full" style={{ background: me.color }} />
        <span
          className="absolute -bottom-0.5 -right-0.5 w-3 h-3 rounded-full border-2 border-[#232428]"
          style={{ background: { online: '#23a559', idle: '#f0b232', dnd: '#f23f43' }[me.status] }}
        />
      </span>
      <div className="flex-1 min-w-0">
        <div className="text-[13px] font-medium text-white truncate">
          {me.name} {me.admin && <span className="text-[10px] text-[#5865f2] font-semibold">OWNER</span>}
        </div>
        {/* Status rides my participant metadata — a full replace under the hood (store.setStatus). */}
        <select
          id="status-select"
          value={me.status}
          onChange={(e) => void setStatus(e.target.value as Member['status'])}
          className="bg-transparent text-[11px] text-[#949ba4] outline-none cursor-pointer"
        >
          <option value="online">Online</option>
          <option value="idle">Idle</option>
          <option value="dnd">Do Not Disturb</option>
        </select>
      </div>
      <button
        id="logout"
        title="Log out"
        onClick={() => void logout()}
        className="p-1.5 rounded text-[#949ba4] hover:bg-[#35373c] hover:text-white cursor-pointer"
      >
        <IconLogOut size={16} />
      </button>
    </footer>
  )
}
