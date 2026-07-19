export { DmView }

import React, { useEffect, useRef } from 'react'
import { openDmHome, sendDm, useApp } from '../store'
import { timeOf } from './ChannelView'
import { IconArrowLeft } from './Icons'

/** One conversation. DMs are DB-backed and server-delivered — they work while the other
 *  side is offline (they'll read it from history when they come back). */
function DmView({ withUserId }: { withUserId: string }) {
  const thread = useApp((s) => s.dmThreads[withUserId])
  const members = useApp((s) => s.members)
  const me = useApp((s) => s.me)
  const messages = thread?.messages ?? []
  const other = members.find((m) => m.userId === withUserId)

  const bottomRef = useRef<HTMLDivElement>(null)
  const lastId = messages[messages.length - 1]?.id
  useEffect(() => {
    bottomRef.current?.scrollIntoView()
  }, [lastId])

  return (
    <main className="flex-1 min-w-0 flex flex-col">
      <header className="shrink-0 h-12 flex items-center gap-2 px-4 border-b border-[#26282c] shadow-sm">
        <button
          id="dm-close"
          title="All conversations"
          onClick={openDmHome}
          className="text-[#949ba4] hover:text-white cursor-pointer"
        >
          <IconArrowLeft size={16} />
        </button>
        <span className="text-[#80848e]">@</span>
        <span id="dm-name" className="font-semibold text-white">
          {thread?.name ?? '…'}
        </span>
        {other === undefined && <span className="text-[12px] text-[#5c5e66]">— offline, they'll see it later</span>}
      </header>

      <div id="dm-messages" className="flex-1 min-h-0 overflow-y-auto px-4 py-3 flex flex-col gap-1">
        {messages.map((message) => {
          const mine = message.fromId === me?.userId
          return (
            <div key={message.id} className={`max-w-[70%] ${mine ? 'self-end' : 'self-start'}`}>
              <div
                className={`px-3 py-2 rounded-xl text-[14px] break-words [overflow-wrap:anywhere] ${
                  mine ? 'bg-[#5865f2] text-white rounded-br-sm' : 'bg-[#383a40] text-[#dbdee1] rounded-bl-sm'
                }`}
              >
                {message.text}
              </div>
              <div className={`text-[10px] text-[#5c5e66] mt-0.5 ${mine ? 'text-right' : ''}`}>
                {timeOf(message.at)}
              </div>
            </div>
          )
        })}
        <div ref={bottomRef} />
      </div>

      <footer className="shrink-0 px-4 pb-5">
        <form
          onSubmit={(e) => {
            e.preventDefault()
            const input = e.currentTarget.elements.namedItem('dm') as HTMLInputElement
            void sendDm(input.value)
            input.value = ''
          }}
        >
          <input
            id="dm-composer"
            name="dm"
            autoComplete="off"
            placeholder={`Message @${thread?.name ?? ''}`}
            className="w-full bg-[#383a40] rounded-lg px-4 py-2.5 text-[14px] text-[#dbdee1] outline-none placeholder:text-[#5c5e66]"
          />
        </form>
      </footer>
    </main>
  )
}
