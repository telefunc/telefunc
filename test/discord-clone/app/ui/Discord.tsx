export { Discord }

import React, { useEffect, useState } from 'react'
import { bootOnce, openDmHome, openGuild, useApp } from '../store'
import { AuthScreen } from './AuthScreen'
import { CallView } from './CallView'
import { ChannelView } from './ChannelView'
import { DmView } from './DmView'
import { IconMessage } from './Icons'
import { MembersList } from './MembersList'
import { Sidebar } from './Sidebar'

// All Room wiring lives in the store and runs from user events — components only render
// snapshots. That's also what keeps <React.StrictMode>'s double-mounted effects from
// double-joining anything (the boot itself is latched).

function Discord() {
  const app = useApp()
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => {
    setHydrated(true)
    bootOnce()
  }, [])

  return (
    <div
      id={hydrated ? 'hydrated' : undefined}
      className="h-screen flex flex-col bg-[#313338] text-[#dbdee1] text-[14px] antialiased"
    >
      {app.phase === 'boot' && (
        <div className="flex-1 flex items-center justify-center text-[#5c5e66]">Connecting…</div>
      )}
      {app.phase === 'auth' && <AuthScreen />}
      {app.phase === 'kicked' && (
        <FullScreenNotice id="kicked-screen" title="You were kicked" detail={`by ${app.kickedBy ?? 'a moderator'}`} />
      )}
      {app.phase === 'disconnected' && (
        <FullScreenNotice id="disconnected-screen" title="Connection lost" detail="Reload to rejoin" />
      )}
      {app.phase === 'ready' && (
        <>
          {app.banner && (
            <div id="banner" className="shrink-0 bg-[#5865f2] text-white text-[13px] px-4 py-1.5 text-center">
              {app.banner.text} <span className="opacity-75">— {app.banner.by}</span>
            </div>
          )}
          <div className="flex-1 min-h-0 flex">
            <GuildRail />
            <Sidebar />
            <MainPane />
            {app.view.kind === 'channel' && <MembersList />}
          </div>
        </>
      )}
      {app.toast && (
        <div
          id="toast"
          className="fixed bottom-6 left-1/2 -translate-x-1/2 bg-[#1e1f22] text-[#f2f3f5] text-[13px] px-4 py-2.5 rounded-lg shadow-xl border border-[#3f4147] max-w-[480px] text-center z-50"
        >
          {app.toast}
        </div>
      )}
    </div>
  )
}

function MainPane() {
  const view = useApp((s) => s.view)
  if (view.kind === 'call') return <CallView />
  if (view.kind === 'dm') {
    if (view.withUserId === null) {
      return (
        <main className="flex-1 min-w-0 flex items-center justify-center text-[#5c5e66]">
          Pick a conversation — or message someone from a channel's member list.
        </main>
      )
    }
    return <DmView withUserId={view.withUserId} />
  }
  return <ChannelView />
}

/** The far-left rail: Home (direct messages) and the guild. */
function GuildRail() {
  const view = useApp((s) => s.view)
  const dmUnread = useApp((s) => s.dmUnread)
  const onDms = view.kind === 'dm'
  const dmBadge = Object.values(dmUnread).reduce((sum, n) => sum + n, 0)

  return (
    <nav className="shrink-0 w-[72px] bg-[#1e1f22] flex flex-col items-center py-3 gap-2">
      <RailButton id="rail-home" title="Direct Messages" active={onDms} onClick={openDmHome} badge={dmBadge}>
        <IconMessage size={22} />
      </RailButton>
      <div className="w-8 h-px bg-[#35363c]" />
      <RailButton id="rail-guild" title="Telefunc HQ" active={!onDms} onClick={openGuild}>
        <span className="font-semibold text-[15px]">HQ</span>
      </RailButton>
    </nav>
  )
}

function RailButton({
  id,
  title,
  active,
  onClick,
  badge,
  children,
}: {
  id: string
  title: string
  active: boolean
  onClick: () => void
  badge?: number
  children: React.ReactNode
}) {
  return (
    <button
      id={id}
      title={title}
      onClick={onClick}
      className={`relative w-12 h-12 flex items-center justify-center cursor-pointer transition-all ${
        active
          ? 'bg-[#5865f2] text-white rounded-[16px]'
          : 'bg-[#313338] text-[#dbdee1] rounded-3xl hover:bg-[#5865f2] hover:text-white hover:rounded-[16px]'
      }`}
    >
      {children}
      {badge !== undefined && badge > 0 && (
        <span
          id={`${id}-badge`}
          className="absolute -bottom-0.5 -right-0.5 min-w-[18px] h-[18px] px-1 rounded-full bg-[#f23f43] text-white text-[11px] font-semibold flex items-center justify-center border-2 border-[#1e1f22]"
        >
          {badge}
        </span>
      )}
    </button>
  )
}

function FullScreenNotice({ id, title, detail }: { id: string; title: string; detail: string }) {
  return (
    <div id={id} className="flex-1 flex flex-col items-center justify-center gap-2">
      <div className="text-[18px] font-semibold text-white">{title}</div>
      <div className="text-[14px] text-[#949ba4]">{detail}</div>
      <button
        onClick={() => location.reload()}
        className="mt-4 bg-[#5865f2] hover:bg-[#4752c4] text-white text-[13px] font-medium rounded-[4px] px-4 py-2 cursor-pointer"
      >
        Back to login
      </button>
    </div>
  )
}
