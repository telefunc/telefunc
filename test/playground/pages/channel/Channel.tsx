export { ChannelDemo }

import React, { useCallback, useEffect, useRef, useState } from 'react'
import { Abort as TelefuncAbort, abort } from 'telefunc/client'
import {
  onChannelInit,
  onChannelAbortTest,
  onChannelPerSendAck,
  onChannelHookInstrument,
  onChannelBinary,
  onChannelClientAbortInstrument,
  onChannelMulti,
  onChannelAckListenerAbort,
  onChannelServerPendingAckAbort,
  onChannelAbortThenSend,
  onChannelPendingAckAbort,
  onChannelClientAbortThenSend,
  onChannelClientPendingAckClose,
  onChannelUpstreamReconnect,
} from './Channel.telefunc'

// ── Types ────────────────────────────────────────────────────────────────────

type LogEntry = {
  id: number
  direction: 'in' | 'out' | 'system'
  text: string
  time: string
}

type ChannelState = {
  connected: boolean
  onOpenFired: boolean
  mainChannelOnCloseFiredClean: boolean | null
  tickCount: number
  /** Server-side count field from the most recent tick message. */
  lastTickServerCount: number | null
  /** True if any tick arrived with count <= the previous tick's count (detects dups/replay). */
  tickWentBackward: boolean
  pingAck: string | null
  welcomeReceived: boolean
  lastEchoText: string | null
  lastEchoAck: string | null
  isClosedAfterClose: boolean | null
  serverAbortReceived: { isAbort: boolean; abortValue: unknown } | null
  clientAbortClosed: boolean | null
  noAckSendVoid: boolean | null
  perSendAck: string | null
  hookServerMessages: string[]
  hookClientOnCloseClean: boolean | null
  hookChannelId: string | null
  binaryRoundTripOk: boolean | null
  binaryByteCount: number | null
  multiCh1LastVal: number | null
  multiCh2LastVal: number | null
  /** ch1 values are strictly +1 each time (proves correct demux, no skips, no repeats). */
  multiCh1IsMonotonic: boolean | null
  /** ch2 values are strictly +100 each time. */
  multiCh2IsMonotonic: boolean | null
  clientAbortServerChannelId: string | null
  earlyCloseChannelId: string | null
  ackListenerAbortErr: { isAbort: boolean; abortValue: unknown } | null
  serverPendingAckAbortChannelId: string | null
  abortThenSendChannelId: string | null
  pendingAckAbortChannelId: string | null
  clientAbortThenSendErr: string | null
  clientPendingAckCloseErr: string | null
  /** Channel ID for the upstream reconnect test channel. */
  upstreamReconnectChannelId: string | null
}

const initialState: ChannelState = {
  connected: false,
  onOpenFired: false,
  mainChannelOnCloseFiredClean: null,
  tickCount: 0,
  lastTickServerCount: null,
  tickWentBackward: false,
  pingAck: null,
  welcomeReceived: false,
  lastEchoText: null,
  lastEchoAck: null,
  isClosedAfterClose: null,
  serverAbortReceived: null,
  clientAbortClosed: null,
  noAckSendVoid: null,
  perSendAck: null,
  hookServerMessages: [],
  hookClientOnCloseClean: null,
  hookChannelId: null,
  binaryRoundTripOk: null,
  binaryByteCount: null,
  multiCh1LastVal: null,
  multiCh2LastVal: null,
  multiCh1IsMonotonic: null,
  multiCh2IsMonotonic: null,
  clientAbortServerChannelId: null,
  earlyCloseChannelId: null,
  ackListenerAbortErr: null,
  serverPendingAckAbortChannelId: null,
  abortThenSendChannelId: null,
  pendingAckAbortChannelId: null,
  clientAbortThenSendErr: null,
  clientPendingAckCloseErr: null,
  upstreamReconnectChannelId: null,
}

function timestamp() {
  return new Date().toLocaleTimeString('en-US', { hour12: false, fractionalSecondDigits: 3 })
}

// ── Component ────────────────────────────────────────────────────────────────

function ChannelDemo() {
  const [hydrated, setHydrated] = useState(false)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [connected, setConnected] = useState(false)
  const [echoInput, setEchoInput] = useState('')
  const [channelState, setChannelState] = useState<ChannelState>(initialState)

  const idRef = useRef(0)
  const bottomRef = useRef<HTMLDivElement>(null)

  // Channel refs — kept so cleanup on unmount can close them
  const channelRef = useRef<Awaited<ReturnType<typeof onChannelInit>>['channel'] | null>(null)
  const initResultRef = useRef<Awaited<ReturnType<typeof onChannelInit>> | null>(null)
  const abortChannelRef = useRef<Awaited<ReturnType<typeof onChannelAbortTest>>['channel'] | null>(null)
  const perSendChannelRef = useRef<Awaited<ReturnType<typeof onChannelPerSendAck>>['channel'] | null>(null)
  const hookChannelRef = useRef<Awaited<ReturnType<typeof onChannelHookInstrument>>['channel'] | null>(null)
  const binaryChannelRef = useRef<Awaited<ReturnType<typeof onChannelBinary>>['channel'] | null>(null)
  const multiCh1Ref = useRef<Awaited<ReturnType<typeof onChannelMulti>>['channel1'] | null>(null)
  const multiCh2Ref = useRef<Awaited<ReturnType<typeof onChannelMulti>>['channel2'] | null>(null)
  const clientAbortServerChannelRef = useRef<
    Awaited<ReturnType<typeof onChannelClientAbortInstrument>>['channel'] | null
  >(null)
  const ackListenerAbortChannelRef = useRef<Awaited<ReturnType<typeof onChannelAckListenerAbort>>['channel'] | null>(
    null,
  )
  const serverPendingAckAbortChannelRef = useRef<
    Awaited<ReturnType<typeof onChannelServerPendingAckAbort>>['channel'] | null
  >(null)
  const abortThenSendChannelRef = useRef<Awaited<ReturnType<typeof onChannelAbortThenSend>>['channel'] | null>(null)
  const pendingAckAbortChannelRef = useRef<Awaited<ReturnType<typeof onChannelPendingAckAbort>>['channel'] | null>(null)
  const clientAbortThenSendChannelRef = useRef<
    Awaited<ReturnType<typeof onChannelClientAbortThenSend>>['channel'] | null
  >(null)
  const clientPendingAckCloseChannelRef = useRef<
    Awaited<ReturnType<typeof onChannelClientPendingAckClose>>['channel'] | null
  >(null)
  const upstreamReconnectChannelRef = useRef<Awaited<ReturnType<typeof onChannelUpstreamReconnect>>['channel'] | null>(
    null,
  )
  const upstreamReconnectSeqRef = useRef(0)

  useEffect(() => setHydrated(true), [])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  useEffect(() => {
    return () => {
      channelRef.current?.close()
      abortChannelRef.current?.close()
      perSendChannelRef.current?.close()
      hookChannelRef.current?.close()
      binaryChannelRef.current?.close()
      multiCh1Ref.current?.close()
      multiCh2Ref.current?.close()
      clientAbortServerChannelRef.current?.close()
      ackListenerAbortChannelRef.current?.close()
      serverPendingAckAbortChannelRef.current?.close()
      abortThenSendChannelRef.current?.close()
      pendingAckAbortChannelRef.current?.close()
      clientAbortThenSendChannelRef.current?.close()
      clientPendingAckCloseChannelRef.current?.close()
      upstreamReconnectChannelRef.current?.close()
    }
  }, [])

  const addLog = useCallback((direction: LogEntry['direction'], text: string) => {
    setLogs((prev) => [...prev, { id: ++idRef.current, direction, text, time: timestamp() }])
  }, [])

  // ── Handlers ─────────────────────────────────────────────────────────────

  const connect = useCallback(async () => {
    if (channelRef.current && !channelRef.current.isClosed) return
    addLog('system', 'Calling onChannelInit()...')
    const result = await onChannelInit()
    initResultRef.current = result
    const { channel, serverTime } = result
    channelRef.current = channel
    addLog('system', `Connected! Server time: ${new Date(serverTime).toISOString()}`)
    setConnected(true)
    setChannelState((s) => ({ ...s, connected: true }))
    channel.onOpen(() => {
      addLog('system', 'onOpen fired')
      setChannelState((s) => ({ ...s, onOpenFired: true }))
    })
    channel.onClose((err) => {
      addLog('system', `onClose fired — err=${err ? err.message : 'none'}`)
      setChannelState((s) => ({ ...s, mainChannelOnCloseFiredClean: err === undefined }))
    })
    channel.listen((msg) => {
      addLog('in', JSON.stringify(msg))
      if (msg.type === 'tick')
        setChannelState((s) => ({
          ...s,
          tickCount: s.tickCount + 1,
          lastTickServerCount: msg.count,
          tickWentBackward:
            s.tickWentBackward || (s.lastTickServerCount !== null && msg.count <= s.lastTickServerCount),
        }))
      if (msg.type === 'welcome') setChannelState((s) => ({ ...s, welcomeReceived: true }))
      if (msg.type === 'echo') setChannelState((s) => ({ ...s, lastEchoText: msg.text }))
      return `client-ack:${msg.type}`
    })
    const ack = await channel.send({ type: 'ping' })
    addLog('out', JSON.stringify({ type: 'ping' }))
    addLog('system', `ack from server: ${JSON.stringify(ack)}`)
    setChannelState((s) => ({ ...s, pingAck: ack as string }))
  }, [addLog])

  const disconnect = useCallback(() => {
    const ch = channelRef.current
    if (!ch) return
    ch.close()
    setChannelState((s) => ({ ...s, connected: false, isClosedAfterClose: ch.isClosed }))
    channelRef.current = null
    setConnected(false)
    addLog('system', 'Disconnected')
  }, [addLog])

  const sendEcho = useCallback(async () => {
    if (channelRef.current?.isClosed !== false || !echoInput.trim()) return
    const msg = { type: 'echo' as const, text: echoInput.trim() }
    setEchoInput('')
    addLog('out', JSON.stringify(msg))
    const ack = await channelRef.current.send(msg)
    addLog('system', `ack from server: ${JSON.stringify(ack)}`)
    setChannelState((s) => ({ ...s, lastEchoAck: ack as string }))
  }, [echoInput, addLog])

  const sendEchoNoAck = useCallback(() => {
    if (channelRef.current?.isClosed !== false) return
    const msg = { type: 'echo' as const, text: 'no-ack-test' }
    addLog('out', `${JSON.stringify(msg)} [no-ack]`)
    const ret = channelRef.current.send(msg, { ack: false })
    setChannelState((s) => ({ ...s, noAckSendVoid: ret === undefined }))
  }, [addLog])

  const testClientAbort = useCallback(() => {
    const result = initResultRef.current
    const ch = channelRef.current
    if (!result || !ch) {
      addLog('system', 'No active connection to abort')
      return
    }
    ch.onClose((err) =>
      setChannelState((s) => ({ ...s, clientAbortClosed: ch.isClosed && err instanceof TelefuncAbort })),
    )
    addLog('system', 'Calling abort(result)...')
    initResultRef.current = null
    abort(result)
    channelRef.current = null
    setConnected(false)
    addLog('system', 'abort(result) called')
  }, [addLog])

  const testServerAbort = useCallback(async () => {
    addLog('system', 'Starting server-abort test...')
    const { channel } = await onChannelAbortTest()
    abortChannelRef.current = channel
    channel.onClose((err) => {
      const e = err as any
      addLog(
        'system',
        `server-abort onClose: isAbort=${e instanceof TelefuncAbort} abortValue=${JSON.stringify(e?.abortValue)}`,
      )
      setChannelState((s) => ({
        ...s,
        serverAbortReceived: { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null },
      }))
    })
  }, [addLog])

  const testPerSendAck = useCallback(async () => {
    addLog('system', 'Starting per-send ack test...')
    const { channel } = await onChannelPerSendAck()
    perSendChannelRef.current = channel
    const ack = await channel.send('per-send-test', { ack: true })
    addLog('system', `per-send ack: ${JSON.stringify(ack)}`)
    setChannelState((s) => ({ ...s, perSendAck: ack as string }))
    channel.close()
  }, [addLog])

  const testHookInstrument = useCallback(async () => {
    addLog('system', 'Starting hook instrumentation test...')
    const { channel, channelId } = await onChannelHookInstrument()
    hookChannelRef.current = channel
    setChannelState((s) => ({ ...s, hookChannelId: channelId }))
    channel.onClose((err) => {
      addLog('system', `hook-channel onClose fired — err=${err ? err.message : 'none'}`)
      setChannelState((s) => ({ ...s, hookClientOnCloseClean: err === undefined }))
      hookChannelRef.current = null
    })
    channel.listen((msg) => {
      const key = `${msg.type}:${msg.hook}`
      addLog('in', `[hook-channel] ${key}`)
      setChannelState((s) => ({ ...s, hookServerMessages: [...s.hookServerMessages, key] }))
      if (msg.hook === 'onOpen') channel.close()
    })
  }, [addLog])

  const testMultiChannel = useCallback(async () => {
    addLog('system', 'Starting multi-channel test...')
    const { channel1, channel2 } = await onChannelMulti()
    multiCh1Ref.current = channel1
    multiCh2Ref.current = channel2
    let ch1Prev = 0
    let ch2Prev = 0

    channel1.listen((val) => {
      addLog('in', `[multiCh1] ${val}`)
      const expected = ch1Prev + 1
      ch1Prev = val
      setChannelState((s) => ({
        ...s,
        multiCh1LastVal: val,
        multiCh1IsMonotonic: s.multiCh1IsMonotonic !== false && val === expected,
      }))
    })
    channel2.listen((val) => {
      addLog('in', `[multiCh2] ${val}`)
      const expected = ch2Prev + 100
      ch2Prev = val
      setChannelState((s) => ({
        ...s,
        multiCh2LastVal: val,
        multiCh2IsMonotonic: s.multiCh2IsMonotonic !== false && val === expected,
      }))
      if (val >= 300) {
        channel1.close()
        channel2.close()
        multiCh1Ref.current = null
        multiCh2Ref.current = null
      }
    })
  }, [addLog])

  const testClientAbortServer = useCallback(async () => {
    addLog('system', 'Starting client-abort-server test...')
    const { channel, channelId } = await onChannelClientAbortInstrument()
    clientAbortServerChannelRef.current = channel
    setChannelState((s) => ({ ...s, clientAbortServerChannelId: channelId }))
    channel.abort()
    clientAbortServerChannelRef.current = null
    addLog('system', `client abort() sent for channel ${channelId}`)
  }, [addLog])

  const testEarlyClose = useCallback(async () => {
    addLog('system', 'Starting early-close test...')
    const { channel, channelId } = await onChannelHookInstrument()
    setChannelState((s) => ({ ...s, earlyCloseChannelId: channelId }))
    channel.close()
    addLog('system', `early close() sent for channel ${channelId}`)
  }, [addLog])

  const testBinary = useCallback(async () => {
    addLog('system', 'Starting binary round-trip test...')
    const { channel } = await onChannelBinary()
    binaryChannelRef.current = channel
    const PATTERN_SIZE = 256
    const REPEAT = 4096 // 1 MB
    const pattern = new Uint8Array(PATTERN_SIZE)
    for (let i = 0; i < PATTERN_SIZE; i++) pattern[i] = i
    const chunks = Array.from({ length: REPEAT }, () => pattern)
    const sent = new Uint8Array(await new Blob(chunks).arrayBuffer())
    channel.listenBinary((received) => {
      let ok = received.length === sent.length
      if (ok) {
        for (let i = 0; i < sent.length; i++) {
          if (received[i] !== sent[i]) {
            ok = false
            break
          }
        }
      }
      addLog('system', `binary round-trip: ${received.length} bytes received, match=${ok}`)
      setChannelState((s) => ({ ...s, binaryRoundTripOk: ok, binaryByteCount: received.length }))
      channel.close()
      binaryChannelRef.current = null
    })
    channel.sendBinary(sent)
    addLog('out', `[binary] sent ${sent.length} bytes (1 MB)`)
  }, [addLog])

  const testAckListenerAbort = useCallback(async () => {
    addLog('system', 'Starting ack-listener-abort test...')
    const { channel } = await onChannelAckListenerAbort()
    ackListenerAbortChannelRef.current = channel
    try {
      await channel.send('trigger', { ack: true })
      addLog('system', 'ack-listener-abort: send() unexpectedly resolved')
    } catch (err: any) {
      addLog(
        'system',
        `ack-listener-abort: send() rejected — isAbort=${err instanceof TelefuncAbort} abortValue=${JSON.stringify(err?.abortValue)}`,
      )
      setChannelState((s) => ({
        ...s,
        ackListenerAbortErr: { isAbort: err instanceof TelefuncAbort, abortValue: err?.abortValue ?? null },
      }))
    } finally {
      ackListenerAbortChannelRef.current = null
    }
  }, [addLog])

  const testServerPendingAckAbort = useCallback(async () => {
    addLog('system', 'Starting server-pending-ack-abort test...')
    const { channel, channelId } = await onChannelServerPendingAckAbort()
    serverPendingAckAbortChannelRef.current = channel
    setChannelState((s) => ({ ...s, serverPendingAckAbortChannelId: channelId }))
    channel.onClose((err) => {
      const e = err as any
      addLog('system', `server-pending-ack-abort onClose: isAbort=${e instanceof TelefuncAbort}`)
      serverPendingAckAbortChannelRef.current = null
    })
  }, [addLog])

  const testAbortThenSend = useCallback(async () => {
    addLog('system', 'Starting server abort-then-send test...')
    const { channel, channelId } = await onChannelAbortThenSend()
    abortThenSendChannelRef.current = channel
    setChannelState((s) => ({ ...s, abortThenSendChannelId: channelId }))
    channel.onClose(() => {
      abortThenSendChannelRef.current = null
    })
  }, [addLog])

  const testPendingAckAbort = useCallback(async () => {
    addLog('system', 'Starting server pending-ack-abort test...')
    const { channel, channelId } = await onChannelPendingAckAbort()
    pendingAckAbortChannelRef.current = channel
    setChannelState((s) => ({ ...s, pendingAckAbortChannelId: channelId }))
    channel.onClose(() => {
      pendingAckAbortChannelRef.current = null
    })
  }, [addLog])

  const testClientAbortThenSend = useCallback(async () => {
    addLog('system', 'Starting client abort-then-send test...')
    const { channel } = await onChannelClientAbortThenSend()
    clientAbortThenSendChannelRef.current = channel
    channel.onOpen(() => {
      channel.abort()
      try {
        channel.send('test')
      } catch (err: any) {
        setChannelState((s) => ({ ...s, clientAbortThenSendErr: err?.name ?? 'unknown' }))
      } finally {
        clientAbortThenSendChannelRef.current = null
      }
    })
  }, [addLog])

  const testClientPendingAckClose = useCallback(async () => {
    addLog('system', 'Starting client pending-ack-close test...')
    const { channel } = await onChannelClientPendingAckClose()
    clientPendingAckCloseChannelRef.current = channel
    channel.onOpen(async () => {
      const p = channel.send('test', { ack: true })
      channel.close()
      try {
        await p
      } catch (err: any) {
        setChannelState((s) => ({ ...s, clientPendingAckCloseErr: err?.name ?? 'unknown' }))
      } finally {
        clientPendingAckCloseChannelRef.current = null
      }
    })
  }, [addLog])

  const testUpstreamReconnectOpen = useCallback(async () => {
    addLog('system', 'Opening upstream-reconnect channel...')
    const { channel, channelId } = await onChannelUpstreamReconnect()
    upstreamReconnectChannelRef.current = channel
    upstreamReconnectSeqRef.current = 0
    setChannelState((s) => ({ ...s, upstreamReconnectChannelId: channelId }))
    addLog('system', `upstream channel ready: ${channelId}`)
  }, [addLog])

  const testUpstreamReconnectSend = useCallback(() => {
    const ch = upstreamReconnectChannelRef.current
    if (!ch || ch.isClosed) return
    const n = ++upstreamReconnectSeqRef.current
    ch.send(n)
    addLog('out', `[upstream] seq ${n}`)
  }, [addLog])

  // ── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col h-full">
      {hydrated && <span id="hydrated" />}
      <pre id="channel-state">{JSON.stringify(channelState)}</pre>

      <div className="p-4 border-b border-zinc-200">
        <h1 className="text-lg font-semibold mb-3">Channel Test</h1>
        <div className="flex gap-2 flex-wrap">
          <button
            id="channel-connect"
            type="button"
            onClick={connect}
            disabled={connected}
            className="px-3 py-1.5 text-sm rounded bg-emerald-600 text-white disabled:opacity-40 hover:bg-emerald-700"
          >
            Connect
          </button>
          <button
            id="channel-disconnect"
            type="button"
            onClick={disconnect}
            disabled={!connected}
            className="px-3 py-1.5 text-sm rounded bg-red-600 text-white disabled:opacity-40 hover:bg-red-700"
          >
            Disconnect
          </button>
          <button
            id="channel-send-no-ack"
            type="button"
            onClick={sendEchoNoAck}
            disabled={!connected}
            className="px-3 py-1.5 text-sm rounded bg-amber-600 text-white disabled:opacity-40 hover:bg-amber-700"
          >
            Send No-Ack
          </button>
          <button
            id="channel-test-server-abort"
            type="button"
            onClick={testServerAbort}
            className="px-3 py-1.5 text-sm rounded bg-purple-600 text-white hover:bg-purple-700"
          >
            Test Server Abort
          </button>
          <button
            id="channel-test-client-abort"
            type="button"
            onClick={testClientAbort}
            disabled={!connected}
            className="px-3 py-1.5 text-sm rounded bg-orange-600 text-white disabled:opacity-40 hover:bg-orange-700"
          >
            Test Client abort()
          </button>
          <button
            id="channel-test-per-send-ack"
            type="button"
            onClick={testPerSendAck}
            className="px-3 py-1.5 text-sm rounded bg-teal-600 text-white hover:bg-teal-700"
          >
            Test Per-send Ack
          </button>
          <button
            id="channel-test-hooks"
            type="button"
            onClick={testHookInstrument}
            className="px-3 py-1.5 text-sm rounded bg-indigo-600 text-white hover:bg-indigo-700"
          >
            Test Hooks
          </button>
          <button
            id="channel-test-binary"
            type="button"
            onClick={testBinary}
            className="px-3 py-1.5 text-sm rounded bg-cyan-600 text-white hover:bg-cyan-700"
          >
            Test Binary
          </button>
          <button
            id="channel-test-multi"
            type="button"
            onClick={testMultiChannel}
            className="px-3 py-1.5 text-sm rounded bg-pink-600 text-white hover:bg-pink-700"
          >
            Test Multi Channel
          </button>
          <button
            id="channel-test-client-abort-server"
            type="button"
            onClick={testClientAbortServer}
            className="px-3 py-1.5 text-sm rounded bg-rose-600 text-white hover:bg-rose-700"
          >
            Test Client Abort → Server
          </button>
          <button
            id="channel-test-early-close"
            type="button"
            onClick={testEarlyClose}
            className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
          >
            Test Early Close
          </button>
          <button
            id="channel-test-ack-listener-abort"
            type="button"
            onClick={testAckListenerAbort}
            className="px-3 py-1.5 text-sm rounded bg-fuchsia-600 text-white hover:bg-fuchsia-700"
          >
            Test Ack Listener Abort
          </button>
          <button
            id="channel-test-server-pending-ack-abort"
            type="button"
            onClick={testServerPendingAckAbort}
            className="px-3 py-1.5 text-sm rounded bg-violet-600 text-white hover:bg-violet-700"
          >
            Test Server Pending Ack Abort
          </button>
          <button
            id="channel-test-abort-then-send"
            type="button"
            onClick={testAbortThenSend}
            className="px-3 py-1.5 text-sm rounded bg-lime-600 text-white hover:bg-lime-700"
          >
            Test Server Abort Then Send
          </button>
          <button
            id="channel-test-pending-ack-abort"
            type="button"
            onClick={testPendingAckAbort}
            className="px-3 py-1.5 text-sm rounded bg-yellow-600 text-white hover:bg-yellow-700"
          >
            Test Server Pending Ack Abort (2)
          </button>
          <button
            id="channel-test-client-abort-then-send"
            type="button"
            onClick={testClientAbortThenSend}
            className="px-3 py-1.5 text-sm rounded bg-green-600 text-white hover:bg-green-700"
          >
            Test Client Abort Then Send
          </button>
          <button
            id="channel-test-client-pending-ack-close"
            type="button"
            onClick={testClientPendingAckClose}
            className="px-3 py-1.5 text-sm rounded bg-sky-600 text-white hover:bg-sky-700"
          >
            Test Client Pending Ack Close
          </button>
          <button
            id="channel-test-upstream-open"
            type="button"
            onClick={testUpstreamReconnectOpen}
            className="px-3 py-1.5 text-sm rounded bg-teal-600 text-white hover:bg-teal-700"
          >
            Open Upstream Channel
          </button>
          <button
            id="channel-test-upstream-send"
            type="button"
            onClick={testUpstreamReconnectSend}
            className="px-3 py-1.5 text-sm rounded bg-teal-700 text-white hover:bg-teal-800"
          >
            Send Upstream Seq
          </button>
        </div>
      </div>

      {connected && (
        <div className="p-4 border-b border-zinc-200">
          <div className="flex gap-2">
            <input
              id="channel-echo-input"
              type="text"
              value={echoInput}
              onChange={(e) => setEchoInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && sendEcho()}
              placeholder="Type a message to echo..."
              className="flex-1 px-3 py-1.5 text-sm border border-zinc-300 rounded focus:outline-none focus:border-zinc-500"
            />
            <button
              id="channel-echo-send"
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
