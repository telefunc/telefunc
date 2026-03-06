export { ChannelDemo }

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { onChannelInit } from './Channel.telefunc'

type LogEntry = {
  id: number
  direction: 'in' | 'out' | 'system'
  text: string
  time: string
}

function timestamp() {
  return new Date().toLocaleTimeString('en-US', { hour12: false, fractionalSecondDigits: 3 })
}

function ChannelDemo() {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [connected, setConnected] = useState(false)
  const [echoInput, setEchoInput] = useState('')
  const channelRef = useRef<Awaited<ReturnType<typeof onChannelInit>>['channel'] | null>(null)
  const idRef = useRef(0)
  const bottomRef = useRef<HTMLDivElement>(null)

  const addLog = useCallback((direction: LogEntry['direction'], text: string) => {
    setLogs((prev) => [...prev, { id: ++idRef.current, direction, text, time: timestamp() }])
  }, [])

  const connect = useCallback(async () => {
    if (channelRef.current && !channelRef.current.isClosed) return
    addLog('system', 'Calling onChannelInit()...')

    const { channel, serverTime } = await onChannelInit()
    channelRef.current = channel

    addLog('system', `Connected! Server time: ${new Date(serverTime).toISOString()}`)
    setConnected(true)

    channel.listen((msg) => {
      addLog('in', JSON.stringify(msg))
    })

    channel.send({ type: 'ping' })
    addLog('out', JSON.stringify({ type: 'ping' }))
  }, [addLog])

  const disconnect = useCallback(() => {
    if (channelRef.current) {
      channelRef.current.close()
      channelRef.current = null
      setConnected(false)
      addLog('system', 'Disconnected')
    }
  }, [addLog])

  const sendEcho = useCallback(() => {
    if (channelRef.current?.isClosed !== false || !echoInput.trim()) return
    const msg = { type: 'echo' as const, text: echoInput.trim() }
    channelRef.current.send(msg)
    addLog('out', JSON.stringify(msg))
    setEchoInput('')
  }, [echoInput, addLog])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  useEffect(() => {
    return () => {
      channelRef.current?.close()
    }
  }, [])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '80vh' }}>
      <div style={{ padding: '12px 0', borderBottom: '1px solid #eee', marginBottom: 8 }}>
        <h1 style={{ fontSize: 18, fontWeight: 600, marginBottom: 12 }}>Channel Test</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            type="button"
            id="connect-btn"
            onClick={connect}
            disabled={connected}
            style={{ padding: '4px 12px', cursor: connected ? 'not-allowed' : 'pointer' }}
          >
            Connect
          </button>
          <button
            type="button"
            id="disconnect-btn"
            onClick={disconnect}
            disabled={!connected}
            style={{ padding: '4px 12px', cursor: !connected ? 'not-allowed' : 'pointer' }}
          >
            Disconnect
          </button>
        </div>
      </div>

      {connected && (
        <div style={{ padding: '8px 0', borderBottom: '1px solid #eee', marginBottom: 8, display: 'flex', gap: 8 }}>
          <input
            type="text"
            id="echo-input"
            value={echoInput}
            onChange={(e) => setEchoInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && sendEcho()}
            placeholder="Type a message to echo..."
            style={{ flex: 1, padding: '4px 8px', border: '1px solid #ccc', borderRadius: 4 }}
          />
          <button type="button" id="send-echo-btn" onClick={sendEcho} style={{ padding: '4px 12px' }}>
            Send Echo
          </button>
        </div>
      )}

      <div id="log" style={{ flex: 1, overflowY: 'auto', fontFamily: 'monospace', fontSize: 12 }}>
        {logs.map((log) => (
          <div key={log.id} style={{ marginBottom: 4, display: 'flex', gap: 8 }}>
            <span style={{ color: '#999', flexShrink: 0 }}>{log.time}</span>
            <span
              style={{
                color: log.direction === 'in' ? '#16a34a' : log.direction === 'out' ? '#2563eb' : '#666',
                fontStyle: log.direction === 'system' ? 'italic' : undefined,
              }}
            >
              {log.direction === 'in' ? '◂' : log.direction === 'out' ? '▸' : '●'} {log.text}
            </span>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}
