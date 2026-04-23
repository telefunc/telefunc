export {
  onChannelInit,
  onChannelAbortTest,
  onChannelPerSendAck,
  onChannelHookInstrument,
  onChannelBinary,
  onChannelClientAbortInstrument,
  onChannelMulti,
  onChannelAckListenerAbort,
  onChannelAckListenerBug,
  onChannelClientAckListenerBug,
  onChannelServerPendingAckAbort,
  onChannelAbortThenSend,
  onChannelPendingAckAbort,
  onChannelClientAbortThenSend,
  onChannelClientPendingAckCloseReconnect,
  onChannelClientPendingAckClose,
  onChannelServerPendingAckCloseReconnectOpen,
  onChannelUpstreamReconnect,
  onChannelNoListenerAckServer,
  onChannelNoListenerAckClient,
  onChannelShieldClient,
  onChannelShieldServerAck,
}

import { channel, Abort } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type Ack = string
type ClientToServer = (msg: ClientMessage) => Ack
type ServerToClient = (msg: ServerMessage) => Ack
type CloseError = { message?: string; abortValue?: unknown } | undefined

function exerciseChannelTypeApi() {
  const ch = channel<(msg: string) => undefined, (msg: number) => Promise<void>>({ ack: true })

  const ackFromClient: Promise<void> = ch.send(1)
  void ackFromClient

  ch.listen((msg) => {
    const text: string = msg
    void text
    return undefined
  })

  const oneWay = channel<never, (msg: string) => void>()
  const ackFromServer: Promise<void> = oneWay.send('hello', { ack: true })
  void ackFromServer
}

function formatCloseError(err: CloseError): string {
  if (!err) return 'none'
  if (err instanceof Abort) return `abort:${JSON.stringify(err.abortValue)}`
  return err.message ?? 'unknown'
}

async function onChannelInit() {
  const ch = channel<ClientToServer, ServerToClient>({ ack: true })
  const swallowClosedChannel = (err: any) => {
    if (ch.isClosed && err?.name === 'ChannelClosedError') return
    throw err
  }

  ch.onClose(() => {
    clearInterval(intervalId)
    console.log('[server] channel closed')
  })
  ch.onOpen(() => {
    console.log('[server] channel opened')
  })

  ch.listen((msg) => {
    console.log('[server] received:', msg)
    if (msg.type === 'echo') {
      void ch.send({ type: 'echo', text: msg.text }).catch(swallowClosedChannel)
    }
    if (msg.type === 'ping') {
      void ch.send({ type: 'welcome' }).catch(swallowClosedChannel)
    }
    // Return ack value to the client
    return `server-ack:${msg.type}`
  })

  let count = 0
  const intervalId = setInterval(async () => {
    const n = ++count
    try {
      const ack = await ch.send({ type: 'tick', count: n })
      console.log(`[server] tick #${n} acked by client:`, ack)
    } catch (err: any) {
      if (ch.isClosed && err?.name === 'ChannelClosedError') return
      throw err
    }
  }, 1000)

  return {
    channel: ch.client,
    serverTime: Date.now(),
  }
}

/**
 * Creates a channel that the server aborts (with a structured value) after a short delay.
 * Tests: server-side abort(value) -> client onClose receives { isAbort: true, abortValue }.
 */
async function onChannelAbortTest() {
  const ch = channel<never, (msg: string) => void>()
  setTimeout(() => {
    ch.abort({ reason: 'test-abort', code: 42 })
  }, 400)
  return { channel: ch.client }
}

/**
 * Creates a channel WITHOUT channel-wide ack mode.
 * Server listener returns an ack string so the client can test per-send { ack: true }.
 */
async function onChannelPerSendAck() {
  const ch = channel<(msg: string) => string, (msg: string) => void>()
  ch.listen((msg) => `ack:${msg}`)
  return { channel: ch.client }
}

/**
 * Full hook instrumentation channel.
 *
 * Server tracks onOpen/onClose in cleanupState (keyed by channel ID).
 * Server also sends a { type: 'server-hook', hook } message to the client when onOpen fires,
 * so the client can react in-band without polling.
 *
 * cleanupState keys:
 *   hook_<id>_serverOnOpen  = 'false' | 'true'
 *   hook_<id>_serverOnClose = 'false' | 'true'
 *   hook_<id>_serverOnCloseErr = 'none' | 'abort:<json>' | error message
 */
type HookServerMsg = { type: 'server-hook'; hook: string }

async function onChannelHookInstrument() {
  const ch = channel<never, (msg: HookServerMsg) => void>()
  const id = ch.id

  cleanupState[`hook_${id}_serverOnOpen`] = 'false'
  cleanupState[`hook_${id}_serverOnClose`] = 'false'
  cleanupState[`hook_${id}_serverOnCloseErr`] = 'none'

  ch.onOpen(() => {
    cleanupState[`hook_${id}_serverOnOpen`] = 'true'
    // Notify client in-band so it knows onOpen fired and can proceed to close
    ch.send({ type: 'server-hook', hook: 'onOpen' })
  })

  ch.onClose((err) => {
    cleanupState[`hook_${id}_serverOnClose`] = 'true'
    cleanupState[`hook_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Binary echo channel: server echoes back every binary frame it receives.
 * Used to test sendBinary/listenBinary round-trip with byte-exact verification.
 */
async function onChannelBinary() {
  const ch = channel()
  ch.listenBinary((data) => {
    ch.sendBinary(data)
  })
  return { channel: ch.client }
}
/**
 * Tracks server-side onClose after a client-initiated abort.
 * cleanupState keys:
 *   clientAbort_<id>_serverOnClose = 'false' | 'true'
 *   clientAbort_<id>_serverOnCloseErr = 'pending' | 'none' | 'abort:<json>' | error message
 */
async function onChannelClientAbortInstrument() {
  const ch = channel()
  const id = ch.id
  cleanupState[`clientAbort_${id}_serverOnClose`] = 'false'
  cleanupState[`clientAbort_${id}_serverOnCloseErr`] = 'pending'
  ch.onClose((err) => {
    cleanupState[`clientAbort_${id}_serverOnClose`] = 'true'
    cleanupState[`clientAbort_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: ch.client, channelId: id }
}

/**
 * Channel whose listener throws Abort when it receives an ack message.
 * Used to verify that a pending `send({ ack: true })` promise rejects with
 * { isAbort: true, abortValue } when the server listener aborts mid-ack.
 */
async function onChannelAckListenerAbort() {
  const ch = channel<(msg: string) => void, never>()
  ch.listen(() => {
    throw Abort({ reason: 'listener-abort', code: 7 })
  })
  return { channel: ch.client }
}

async function onChannelAckListenerBug() {
  const ch = channel<(msg: string) => string, never>()
  ch.listen((msg) => {
    if (msg === 'bug') throw new Error('server-listener-bug')
    return `ack:${msg}`
  })
  return { channel: ch.client }
}

async function onChannelClientAckListenerBug() {
  const ch = channel<never, (msg: string) => string>()
  const id = ch.id
  cleanupState[`clientAckBug_${id}_rejected`] = 'false'
  cleanupState[`clientAckBug_${id}_followupAck`] = 'pending'

  ch.onOpen(async () => {
    try {
      await ch.send('bug', { ack: true })
    } catch {
      cleanupState[`clientAckBug_${id}_rejected`] = 'true'
    }

    try {
      const ack = await ch.send('ok', { ack: true })
      cleanupState[`clientAckBug_${id}_followupAck`] = String(ack)
    } catch (err: any) {
      cleanupState[`clientAckBug_${id}_followupAck`] = err?.message ?? err?.name ?? 'unknown'
    }
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Tests concurrent pending ack rejection on the server side.
 *
 * On open, server does `await ch.send(data, { ack: true })`. Before the
 * client can respond, the server calls `ch.abort()`. `_shutdown()` must
 * reject the in-flight ack promise — verified via cleanupState.
 *
 * cleanupState keys:
 *   serverPendingAck_<id>_rejected  = 'false' | 'true'
 *   serverPendingAck_<id>_isAbort   = 'false' | 'true'
 */
async function onChannelServerPendingAckAbort() {
  const ch = channel<never, (msg: string) => void>()
  const id = ch.id

  cleanupState[`serverPendingAck_${id}_rejected`] = 'false'
  cleanupState[`serverPendingAck_${id}_isAbort`] = 'false'

  ch.onOpen(async () => {
    // Abort before the client can ack
    setTimeout(() => ch.abort({ reason: 'abort-while-ack-pending' }), 30)
    try {
      await ch.send('awaiting-ack', { ack: true })
    } catch (err: any) {
      cleanupState[`serverPendingAck_${id}_rejected`] = 'true'
      cleanupState[`serverPendingAck_${id}_isAbort`] = err instanceof Abort ? 'true' : 'false'
    }
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Case 1: abort() then send() — send() must throw ChannelClosedError synchronously.
 *
 * cleanupState keys:
 *   abortThenSend_<id>_thrown       = 'false' | 'true'
 *   abortThenSend_<id>_isClosedErr  = 'false' | 'true'
 */
async function onChannelAbortThenSend() {
  const ch = channel<never, (msg: string) => void>()
  const id = ch.id
  cleanupState[`abortThenSend_${id}_thrown`] = 'false'
  cleanupState[`abortThenSend_${id}_isClosedErr`] = 'false'

  ch.onOpen(() => {
    ch.abort({ reason: 'abort-before-send' })
    try {
      ch.send('test', { ack: true })
    } catch (err: any) {
      cleanupState[`abortThenSend_${id}_thrown`] = 'true'
      cleanupState[`abortThenSend_${id}_isClosedErr`] = err?.message === 'Channel is closed' ? 'true' : 'false'
    }
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Case 2: const p = send() → abort() → await p — promise must reject with abort semantics.
 *
 * cleanupState keys:
 *   pendingAbort_<id>_rejected        = 'false' | 'true'
 *   pendingAbort_<id>_isAbortErr      = 'false' | 'true'
 */
async function onChannelPendingAckAbort() {
  const ch = channel<never, (msg: string) => void>()
  const id = ch.id
  cleanupState[`pendingAbort_${id}_rejected`] = 'false'
  cleanupState[`pendingAbort_${id}_isAbortErr`] = 'false'

  ch.onOpen(async () => {
    const p = ch.send('awaiting-ack', { ack: true })
    ch.abort({ reason: 'abort-during-send', code: 99 })
    try {
      await p
    } catch (err: any) {
      cleanupState[`pendingAbort_${id}_rejected`] = 'true'
      cleanupState[`pendingAbort_${id}_isAbortErr`] = err instanceof Abort ? 'true' : 'false'
    }
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Minimal channel for client-side abort-then-send test.
 * The client connects, calls abort() then send() to verify ChannelClosedError is thrown.
 */
async function onChannelClientAbortThenSend() {
  const ch = channel<(msg: string) => void, never>()
  return { channel: ch.client }
}

/**
 * Minimal channel for client-side pending-ack close test.
 * The client connects, calls send({ ack: true }) to create a pending ack, then close().
 * With graceful close semantics, the pending send waits for an ack until close timeout,
 * then rejects with ChannelClosedError carrying the timeout message.
 */
async function onChannelClientPendingAckClose() {
  const ch = channel<(msg: string) => void, never>()
  ch.listen(() => new Promise(() => {}))
  return { channel: ch.client }
}

/**
 * Channel for reconnect-tolerant client close semantics.
 * The client may go offline, call send({ ack: true }) + close(), then reconnect.
 * The buffered ack request is delivered after reconcile, the server replies,
 * and the server observes a clean onClose.
 * cleanupState keys:
 *   clientClose_<id>_serverOnClose = 'false' | 'true'
 *   clientClose_<id>_serverOnCloseErr = 'pending' | 'none' | 'abort:<json>' | error message
 */
async function onChannelClientPendingAckCloseReconnect() {
  const ch = channel<(msg: string) => string, never>()
  const id = ch.id
  cleanupState[`clientClose_${id}_serverOnClose`] = 'false'
  cleanupState[`clientClose_${id}_serverOnCloseErr`] = 'pending'
  ch.listen((msg) => `ack:${msg}`)
  ch.onClose((err) => {
    cleanupState[`clientClose_${id}_serverOnClose`] = 'true'
    cleanupState[`clientClose_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: ch.client, channelId: id }
}

// Global store keyed by channel id — survives across telefunc invocations.
const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')
function getServerCloseReconnectStore(): Map<string, ReturnType<typeof channel<(msg: string) => string, never>>> {
  return ((globalThis as any)[SERVER_CLOSE_RECONNECT_STORE_KEY] ??= new Map())
}

/**
 * Channel for reconnect-tolerant server-initiated close semantics.
 * After opening, the test triggers send({ack:true}) + close() via /api/server-close-trigger
 * while the client is offline.  The buffered frames are replayed after reconnect; the
 * client acks; both sides close cleanly.
 *
 * cleanupState keys:
 *   serverClose_<id>_closeResult     = 'pending' | '0' | '1'
 *   serverClose_<id>_serverOnCloseErr = 'pending' | 'none' | error message
 */
async function onChannelServerPendingAckCloseReconnectOpen() {
  const ch = channel<(msg: string) => string, never>()
  const id = ch.id
  cleanupState[`serverClose_${id}_closeResult`] = 'pending'
  cleanupState[`serverClose_${id}_serverOnCloseErr`] = 'pending'
  getServerCloseReconnectStore().set(id, ch)
  ch.onClose((err) => {
    getServerCloseReconnectStore().delete(id)
    cleanupState[`serverClose_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: ch.client, channelId: id }
}

/**
 * Upstream reconnect test channel.
 *
 * The client sends sequential integers (1, 2, 3, …) via the client replay buffer.
 * The server counts them and detects any gap, proving exactly-once delivery in the
 * client→server direction across a reconnect.
 *
 * cleanupState keys:
 *   upstream_<id>_receivedCount  = number of messages received
 *   upstream_<id>_lastSeq        = last sequence value received
 *   upstream_<id>_hasGap         = 'true' if any value arrived out of order or skipped
 */
async function onChannelUpstreamReconnect() {
  const ch = channel<(msg: number) => void, never>()
  const id = ch.id
  cleanupState[`upstream_${id}_receivedCount`] = '0'
  cleanupState[`upstream_${id}_lastSeq`] = '0'
  cleanupState[`upstream_${id}_hasGap`] = 'false'

  let lastSeq = 0
  ch.listen((seq) => {
    const count = Number(cleanupState[`upstream_${id}_receivedCount`]) + 1
    cleanupState[`upstream_${id}_receivedCount`] = String(count)
    if (seq !== lastSeq + 1) {
      cleanupState[`upstream_${id}_hasGap`] = 'true'
    }
    lastSeq = seq
    cleanupState[`upstream_${id}_lastSeq`] = String(seq)
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Returns two independent channels ticking at different rates with distinguishable values.
 *   channel1: sends 1, 2, 3, … every 200 ms
 *   channel2: sends 100, 200, 300, … every 500 ms
 * Used to verify ix-level multiplexing over a shared WS connection.
 */
async function onChannelMulti() {
  const ch1 = channel<never, (msg: number) => void>()
  const ch2 = channel<never, (msg: number) => void>()

  let n1 = 0
  const t1 = setInterval(() => ch1.send(++n1), 200)
  ch1.onClose(() => clearInterval(t1))

  let n2 = 0
  const t2 = setInterval(() => ch2.send((n2 += 100)), 500)
  ch2.onClose(() => clearInterval(t2))

  return { channel1: ch1.client, channel2: ch2.client }
}

/**
 * Server sends with ack but client has no listener.
 * The client should respond with an error, rejecting the server's send.
 */
async function onChannelNoListenerAckServer() {
  const ch = channel<never, (msg: string) => string>()
  const id = ch.id

  cleanupState[`noListenerAck_server_${id}_rejected`] = 'false'
  cleanupState[`noListenerAck_server_${id}_errMsg`] = ''

  ch.onOpen(async () => {
    try {
      await ch.send('hello', { ack: true })
    } catch (err: any) {
      cleanupState[`noListenerAck_server_${id}_rejected`] = 'true'
      cleanupState[`noListenerAck_server_${id}_errMsg`] = err?.message ?? 'unknown'
    }
  })

  return { channel: ch.client, channelId: id }
}

/**
 * Client sends with ack but server has no listener.
 * The server should respond with an error, rejecting the client's send.
 */
async function onChannelNoListenerAckClient() {
  const ch = channel<(msg: string) => string, never>()
  // No ch.listen() — intentionally missing
  return { channel: ch.client }
}

/** Shield validates client-sent data. Typed as `(msg: string) => number`:
 *  - No-ack: valid string arrives, invalid is silently dropped server-side (no client error).
 *  - With-ack: valid returns the length; invalid rejects the client's send with a shield error. */
async function onChannelShieldClient() {
  const ch = channel<(msg: string) => number>()
  const received: string[] = []
  ch.listen((msg) => {
    received.push(msg)
    return msg.length
  })
  return { channel: ch.client, getReceived: async () => received }
}

/** Shield validates the client's ack response when the server sends with `{ ack: true }`.
 *  Typed as server→client `(msg: number) => string`:
 *  - Client listener returns string → server's send resolves with the string.
 *  - Client listener returns a non-string → server's send rejects with a shield error. */
async function onChannelShieldServerAck() {
  const ch = channel<never, (msg: number) => string>({ ack: true })
  let outcome: { ok: boolean; value?: string; error?: string } | null = null
  return {
    channel: ch.client,
    trigger: async () => {
      try {
        const ack = await ch.send(100, { ack: true })
        outcome = { ok: true, value: ack }
      } catch (err: any) {
        outcome = { ok: false, error: err?.message ?? 'unknown' }
      }
    },
    getOutcome: async (): Promise<{ ok: boolean; value?: string; error?: string } | null> => outcome,
  }
}
