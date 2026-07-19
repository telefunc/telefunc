export { MembersList }

import React from 'react'
import type { Member } from '../store'
import { kickUser, openDm, useApp } from '../store'
import { IconMessage, IconUserX } from './Icons'

const STATUS_DOT = { online: '#23a559', idle: '#f0b232', dnd: '#f23f43' } as const

/** The guild roster — one row per user (presence-deduped across tabs), live status dots. */
function MembersList() {
  const members = useApp((s) => s.members)
  const me = useApp((s) => s.me)
  const dmUnread = useApp((s) => s.dmUnread)
  return (
    <aside className="shrink-0 w-56 bg-[#2b2d31] overflow-y-auto px-2 py-3">
      <div className="px-2 pb-1 text-[11px] font-semibold tracking-wide text-[#949ba4] uppercase">
        Members — {members.length}
      </div>
      {members.map((member) => (
        <MemberRow
          key={member.userId}
          member={member}
          isSelf={member.userId === me?.userId}
          canKick={me?.admin === true && member.userId !== me.userId && !member.bot}
          unread={dmUnread[member.userId] ?? 0}
        />
      ))}
    </aside>
  )
}

function MemberRow({
  member,
  isSelf,
  canKick,
  unread,
}: {
  member: Member
  isSelf: boolean
  canKick: boolean
  unread: number
}) {
  return (
    <div
      data-member={member.name}
      className="group flex items-center gap-2 px-2 py-1.5 rounded-[4px] hover:bg-[#35373c]"
      title={`joined ${new Date(member.joinedAt).toLocaleTimeString()}`}
    >
      <span className="relative shrink-0">
        <span className="block w-7 h-7 rounded-full" style={{ background: member.color }} />
        <span
          data-status={member.status}
          className="absolute -bottom-0.5 -right-0.5 w-3 h-3 rounded-full border-2 border-[#2b2d31]"
          style={{ background: STATUS_DOT[member.status] }}
        />
      </span>
      <span className="flex-1 truncate text-[#dbdee1]">
        {member.name}
        {isSelf && <span className="text-[#5c5e66]"> (you)</span>}
      </span>
      {member.bot && <span className="text-[9px] font-semibold bg-[#5865f2] text-white rounded px-1 py-px">BOT</span>}
      {member.admin && !member.bot && <span className="text-[10px] text-[#5865f2] font-semibold">OWNER</span>}
      {unread > 0 && (
        <span className="shrink-0 min-w-[18px] h-[18px] px-1 rounded-full bg-[#f23f43] text-white text-[11px] font-semibold flex items-center justify-center">
          {unread}
        </span>
      )}
      {!isSelf && (
        <button
          data-dm={member.name}
          title="Message"
          onClick={() => void openDm(member.userId, member.name)}
          className="hidden group-hover:block text-[#949ba4] hover:text-white cursor-pointer"
        >
          <IconMessage size={15} />
        </button>
      )}
      {canKick && (
        <button
          data-kick={member.name}
          title="Kick"
          onClick={() => void kickUser(member.userId)}
          className="hidden group-hover:block text-[#949ba4] hover:text-[#fa777c] cursor-pointer"
        >
          <IconUserX size={15} />
        </button>
      )}
    </div>
  )
}
