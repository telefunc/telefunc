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
    // Already have an active channel — skip.
    if (channelRef.current && !channelRef.current.isClosed) return
    addLog('system', 'Calling onChannelInit()...')

    const { channel, serverTime } = await onChannelInit()
    channelRef.current = channel

    addLog('system', `Connected! Server time: ${new Date(serverTime).toISOString()}`)
    setConnected(true)

    channel.listen((msg) => {
      addLog('in', JSON.stringify(msg))
      // Return ack value to the server
      return `client-ack:${msg.type}`
    })

    // Send a ping to get a welcome
    const ack = await channel.send({ type: 'ping' })
    addLog('out', JSON.stringify({ type: 'ping' }))
    addLog('system', `ack from server: ${JSON.stringify(ack)}`)
  }, [addLog])

  const disconnect = useCallback(() => {
    if (channelRef.current) {
      channelRef.current.close()
      channelRef.current = null
      setConnected(false)
      addLog('system', 'Disconnected')
    }
  }, [addLog])

  const sendEcho = useCallback(async () => {
    if (channelRef.current?.isClosed !== false || !echoInput.trim()) return
    const msg = { type: 'echo' as const, text: echoInput.trim() }
    setEchoInput('')
    addLog('out', JSON.stringify(msg))
    const ack = await channelRef.current.send(msg)
    addLog('system', `ack from server: ${JSON.stringify(ack)}`)
  }, [echoInput, addLog])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      channelRef.current?.close()
    }
  }, [])

  return (
    <div className="flex flex-col h-full">
      <div className="p-4 border-b border-zinc-200">
        <h1 className="text-lg font-semibold mb-3">Channel Test</h1>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={connect}
            disabled={connected}
            className="px-3 py-1.5 text-sm rounded bg-emerald-600 text-white disabled:opacity-40 hover:bg-emerald-700"
          >
            Connect
          </button>
          <button
            type="button"
            onClick={disconnect}
            disabled={!connected}
            className="px-3 py-1.5 text-sm rounded bg-red-600 text-white disabled:opacity-40 hover:bg-red-700"
          >
            Disconnect
          </button>
        </div>
      </div>

      {connected && (
        <div className="p-4 border-b border-zinc-200">
          <div className="flex gap-2">
            <input
              type="text"
              value={echoInput}
              onChange={(e) => setEchoInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && sendEcho()}
              placeholder="Type a message to echo..."
              className="flex-1 px-3 py-1.5 text-sm border border-zinc-300 rounded focus:outline-none focus:border-zinc-500"
            />
            <button
              type="button"
              onClick={sendEcho}
              className="px-3 py-1.5 text-sm rounded bg-zinc-800 text-white hover:bg-zinc-900"
            >
              Send Echo
            </button>
          </div>
        </div>
      )}

      <div className="flex-1 overflow-y-auto p-4 font-mono text-xs">
        {logs.map((log) => (
          <div key={log.id} className="mb-1 flex gap-2">
            <span className="text-zinc-400 shrink-0">{log.time}</span>
            <span
              className={
                log.direction === 'in'
                  ? 'text-emerald-600'
                  : log.direction === 'out'
                    ? 'text-blue-600'
                    : 'text-zinc-500 italic'
              }
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
