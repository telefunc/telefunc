export { Chat }

import React, { useEffect, useRef, useState } from 'react'
import { RED } from '../../shared/constants'
import { sendChat, useApp } from '../store'

/** All-chat, over the room's `publish`/`subscribe` lane. (Team-only chat would leak on a room-wide
 *  publish — see README finding "No audience-scoped publish".) */
function Chat({ id, compact }: { id: string; compact?: boolean }) {
  const chat = useApp((s) => s.chat)
  const [text, setText] = useState('')
  const scroller = useRef<HTMLDivElement>(null)

  useEffect(() => {
    scroller.current?.scrollTo(0, scroller.current.scrollHeight)
  }, [chat])

  return (
    <div className={`flex flex-col ${compact ? 'h-40' : 'h-full'}`}>
      <div ref={scroller} id={`${id}-log`} className="flex-1 overflow-y-auto space-y-1 pr-1 text-sm">
        {chat.map((m, i) => (
          <div key={i} className="leading-snug">
            <span
              className="font-semibold"
              style={{ color: m.team === RED ? '#ff4d5e' : m.team === 0 ? m.color : '#4d9dff' }}
            >
              {m.from}
            </span>
            <span className="text-steel-300">: {m.text}</span>
          </div>
        ))}
      </div>
      <form
        onSubmit={(e) => {
          e.preventDefault()
          sendChat(text)
          setText('')
        }}
        className="mt-2"
      >
        <input
          id={`${id}-input`}
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="Message all commanders…"
          maxLength={160}
          className="w-full px-3 py-1.5 rounded-lg bg-steel-900 border border-steel-700 text-steel-200 text-sm outline-none focus:border-team-blue"
        />
      </form>
    </div>
  )
}
