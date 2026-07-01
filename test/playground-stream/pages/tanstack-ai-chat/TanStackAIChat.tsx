export { TanStackAIChat }

import React, { useEffect, useRef } from 'react'
import { useChat } from '@tanstack/ai-react'
import type { UIMessage } from '@tanstack/ai-react'
import { withContext } from 'telefunc/client'
import { onChat } from './TanStackAIChat.telefunc'

function getTextContent(msg: UIMessage): string {
  return msg.parts
    .filter((p): p is { type: 'text'; content: string } => p.type === 'text')
    .map((p) => p.content)
    .join('')
}

function TanStackAIChat() {
  const { messages, sendMessage, isLoading, stop, clear } = useChat({
    connection: {
      connect: (messages, _data, signal) => withContext(onChat, { signal })(messages as UIMessage[]),
    },
  })
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, isLoading])

  const empty = messages.length === 0

  return (
    <div className="h-screen flex flex-col overflow-hidden bg-white">
      {/* Header */}
      <header className="shrink-0 h-12 flex items-center justify-between px-6 border-b border-zinc-100">
        <span className="text-[15px] font-semibold text-zinc-900 tracking-tight">TanStack AI Chat</span>
        {!empty && (
          <button
            onClick={clear}
            className="text-[13px] text-zinc-500 hover:text-zinc-800 transition-colors cursor-pointer"
          >
            New chat
          </button>
        )}
      </header>

      {/* Messages */}
      <main className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden">
        {empty ? (
          <div className="flex flex-col items-center justify-center h-full gap-2 text-zinc-400">
            <svg
              width="32"
              height="32"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
            </svg>
            <span className="text-sm">How can I help you today?</span>
            <span className="text-xs text-zinc-300 mt-1">Powered by TanStack AI + Telefunc</span>
          </div>
        ) : (
          <div className="max-w-[720px] mx-auto px-6 py-8">
            {messages.map((msg) => {
              const text = getTextContent(msg)
              if (!text) return null
              if (msg.role === 'user') {
                return (
                  <div key={msg.id} className="mb-8">
                    <div className="flex justify-end mb-3">
                      <div className="bg-zinc-900 text-white px-4 py-2.5 rounded-[20px] rounded-br-[4px] max-w-[70%] text-sm leading-relaxed break-words [overflow-wrap:anywhere]">
                        {text}
                      </div>
                    </div>
                  </div>
                )
              }
              return (
                <div key={msg.id} className="mb-8">
                  <div className="text-sm leading-[1.7] text-zinc-800">
                    {text}
                    {isLoading && msg === messages[messages.length - 1] && (
                      <span className="inline-block w-[2px] h-[17px] bg-zinc-900 ml-0.5 align-text-bottom animate-pulse" />
                    )}
                  </div>
                </div>
              )
            })}
            <div ref={bottomRef} />
          </div>
        )}
      </main>

      {/* Input */}
      <footer className="shrink-0 px-6 pt-4 pb-6">
        <form
          className="max-w-[720px] mx-auto relative"
          onSubmit={(e) => {
            e.preventDefault()
            const form = e.currentTarget
            const prompt = new FormData(form).get('prompt') as string
            if (prompt.trim()) {
              sendMessage(prompt.trim())
              form.reset()
            }
          }}
        >
          <input
            name="prompt"
            placeholder="Message..."
            autoComplete="off"
            disabled={isLoading}
            className="w-full border border-zinc-200 rounded-2xl py-3.5 pl-5 pr-20 text-sm outline-none transition-[border-color,box-shadow] duration-150 focus:border-zinc-400 focus:ring-1 focus:ring-zinc-400 disabled:opacity-50 disabled:bg-zinc-50"
          />
          {isLoading ? (
            <button
              type="button"
              onClick={stop}
              className="absolute right-2 top-1/2 -translate-y-1/2 rounded-[10px] px-4 py-2 text-[13px] font-medium text-white bg-red-500 hover:bg-red-600 transition-colors duration-150 cursor-pointer"
            >
              Stop
            </button>
          ) : (
            <button
              type="submit"
              className="absolute right-2 top-1/2 -translate-y-1/2 rounded-[10px] px-4 py-2 text-[13px] font-medium text-white bg-zinc-900 hover:bg-zinc-700 transition-colors duration-150 cursor-pointer"
            >
              Send
            </button>
          )}
        </form>
      </footer>
    </div>
  )
}
