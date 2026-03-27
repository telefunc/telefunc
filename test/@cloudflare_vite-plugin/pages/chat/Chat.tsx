export { ChatDemo }

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { onJoinChat } from './Chat.telefunc'

type ChatMessage = { user: string; text: string; ts: number }

function ChatDemo() {
  const [username, setUsername] = useState('')
  const [joined, setJoined] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const channelRef = useRef<Awaited<ReturnType<typeof onJoinChat>>['channel'] | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)

  const join = useCallback(async () => {
    const name = username.trim()
    if (!name) return
    const { channel } = await onJoinChat(name)
    channelRef.current = channel

    channel.subscribe((msg) => {
      setMessages((prev) => [...prev, msg])
    })

    channel.onClose(() => {
      setMessages((prev) => [...prev, { user: 'system', text: 'Disconnected', ts: Date.now() }])
      setJoined(false)
      channelRef.current = null
    })

    setJoined(true)
  }, [username])

  const send = useCallback(() => {
    const text = input.trim()
    if (!text || !channelRef.current || channelRef.current.isClosed) return
    const msg: ChatMessage = { user: username, text, ts: Date.now() }
    channelRef.current.publish(msg)
    setInput('')
  }, [input, username])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  useEffect(() => {
    return () => {
      channelRef.current?.close()
    }
  }, [])

  if (!joined) {
    return (
      <div style={{ padding: '20px 0' }}>
        <h1 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12 }}>Chat (pub/sub demo)</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && join()}
            placeholder="Pick a username..."
            style={{ flex: 1, padding: '6px 10px', border: '1px solid #ccc', borderRadius: 4 }}
          />
          <button type="button" onClick={join} style={{ padding: '6px 16px' }}>
            Join
          </button>
        </div>
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '80vh' }}>
      <div style={{ padding: '12px 0', borderBottom: '1px solid #eee', marginBottom: 8 }}>
        <h1 style={{ fontSize: 18, fontWeight: 600 }}>Chat (pub/sub demo) &mdash; {username}</h1>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', fontSize: 14 }}>
        {messages.map((msg, i) => (
          <div key={i} style={{ marginBottom: 6 }}>
            {msg.user === 'system' ? (
              <span style={{ color: '#999', fontStyle: 'italic' }}>{msg.text}</span>
            ) : (
              <>
                <strong style={{ color: msg.user === username ? '#2563eb' : '#16a34a' }}>{msg.user}</strong>: {msg.text}
              </>
            )}
          </div>
        ))}
        <div ref={bottomRef} />
      </div>

      <div style={{ padding: '8px 0', borderTop: '1px solid #eee', display: 'flex', gap: 8 }}>
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && send()}
          placeholder="Type a message..."
          style={{ flex: 1, padding: '6px 10px', border: '1px solid #ccc', borderRadius: 4 }}
        />
        <button type="button" onClick={send} style={{ padding: '6px 16px' }}>
          Send
        </button>
      </div>
    </div>
  )
}
