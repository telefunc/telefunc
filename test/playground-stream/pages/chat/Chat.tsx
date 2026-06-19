export { Chat }

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { onGetHistory, onSendMessage, onClearHistory } from './Chat.telefunc'
import type { Message } from './Chat.telefunc'

function useChat() {
  const [messages, setMessages] = useState<Message[]>([])
  const [current, setCurrent] = useState<Message | null>(null)
  const [isStreaming, setIsStreaming] = useState(false)
  const [error, setError] = useState<{ message: string } | null>(null)
  const versionRef = useRef(0)

  // Load history on mount
  useEffect(() => {
    onGetHistory().then(setMessages)
  }, [])

  const send = useCallback((prompt: string) => {
    const version = ++versionRef.current
    const isCurrent = () => version === versionRef.current

    setError(null)
    setIsStreaming(true)
    setCurrent({ prompt, response: '' })
    ;(async () => {
      try {
        const gen = onSendMessage(prompt)
        let response = ''
        for await (const word of gen) {
          if (!isCurrent()) break
          response += (response ? ' ' : '') + word
          setCurrent({ prompt, response })
        }
        if (isCurrent()) {
          setMessages((prev) => [...prev, { prompt, response }])
          setCurrent(null)
        }
      } catch (err) {
        if (isCurrent()) setError({ message: err instanceof Error ? err.message : String(err) })
      } finally {
        if (isCurrent()) setIsStreaming(false)
      }
    })()
  }, [])

  const abort = useCallback(() => {
    versionRef.current++
    setCurrent(null)
    setIsStreaming(false)
  }, [])

  const clear = useCallback(() => {
    setMessages([])
    setCurrent(null)
    onClearHistory()
  }, [])

  return { history: messages, current, isStreaming, error, send, abort, clear }
}

const MessageBubble = React.memo(function MessageBubble({ msg }: { msg: Message }) {
  return (
    <div className="mb-8">
      <div className="flex justify-end mb-3">
        <div className="bg-zinc-900 text-white px-4 py-2.5 rounded-[20px] rounded-br-[4px] max-w-[70%] text-sm leading-relaxed break-words [overflow-wrap:anywhere]">
          {msg.prompt}
        </div>
      </div>
      <div className="text-sm leading-[1.7] text-zinc-800">{msg.response}</div>
    </div>
  )
})

function Chat() {
  const { history, current, isStreaming, error, send, abort, clear } = useChat()
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [history, current, isStreaming])

  const empty = history.length === 0 && !current

  return (
    <div className="h-screen flex flex-col overflow-hidden bg-white">
      {/* Header */}
      <header className="shrink-0 h-12 flex items-center justify-between px-6 border-b border-zinc-100">
        <span className="text-[15px] font-semibold text-zinc-900 tracking-tight">Chat</span>
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
          </div>
        ) : (
          <div className="max-w-[720px] mx-auto px-6 py-8">
            {history.map((msg, i) => (
              <MessageBubble key={i} msg={msg} />
            ))}
            {current && (
              <div className="mb-8">
                <div className="flex justify-end mb-3">
                  <div className="bg-zinc-900 text-white px-4 py-2.5 rounded-[20px] rounded-br-[4px] max-w-[70%] text-sm leading-relaxed break-words [overflow-wrap:anywhere]">
                    {current.prompt}
                  </div>
                </div>
                <div className="text-sm leading-[1.7] text-zinc-800">
                  {current.response}
                  <span className="inline-block w-[2px] h-[17px] bg-zinc-900 ml-0.5 align-text-bottom animate-pulse" />
                </div>
              </div>
            )}
            <div ref={bottomRef} />
          </div>
        )}
      </main>

      {/* Error */}
      {error && (
        <div className="shrink-0 px-6 py-2 bg-red-50 text-red-600 text-[13px] text-center">{error.message}</div>
      )}

      {/* Input */}
      <footer className="shrink-0 px-6 pt-4 pb-6">
        <form
          className="max-w-[720px] mx-auto relative"
          onSubmit={(e) => {
            e.preventDefault()
            const form = e.currentTarget
            const prompt = new FormData(form).get('prompt') as string
            if (prompt.trim()) {
              send(prompt.trim())
              form.reset()
            }
          }}
        >
          <input
            name="prompt"
            placeholder="Message..."
            autoComplete="off"
            disabled={isStreaming}
            className="w-full border border-zinc-200 rounded-2xl py-3.5 pl-5 pr-20 text-sm outline-none transition-[border-color,box-shadow] duration-150 focus:border-zinc-400 focus:ring-1 focus:ring-zinc-400 disabled:opacity-50 disabled:bg-zinc-50"
          />
          {isStreaming ? (
            <button
              type="button"
              onClick={abort}
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
