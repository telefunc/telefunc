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
}

import { createChannel, Abort } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type Ack = string
type ClientToServer = (msg: ClientMessage) => Ack
type ServerToClient = (msg: ServerMessage) => Ack
type CloseError = { message?: string; abortValue?: unknown } | undefined

function exerciseChannelTypeApi() {
  const channel = createChannel<(msg: string) => undefined, (msg: number) => Promise<void>>({ ack: true })

  const ackFromClient: Promise<void> = channel.send(1)
  void ackFromClient

  channel.listen((msg) => {
    const text: string = msg
    void text
    return undefined
  })

  const oneWay = createChannel<never, (msg: string) => void>()
  const ackFromServer: Promise<void> = oneWay.send('hello', { ack: true })
  void ackFromServer
}

function formatCloseError(err: CloseError): string {
  if (!err) return 'none'
  if (err instanceof Abort) return `abort:${JSON.stringify(err.abortValue)}`
  return err.message ?? 'unknown'
}

async function onChannelInit() {
  const channel = createChannel<ClientToServer, ServerToClient>({ ack: true })
  const swallowClosedChannel = (err: any) => {
    if (channel.isClosed && err?.name === 'ChannelClosedError') return
    throw err
  }

  channel.onClose(() => {
    clearInterval(intervalId)
    console.log('[server] channel closed')
  })
  channel.onOpen(() => {
    console.log('[server] channel opened')
  })

  channel.listen((msg) => {
    console.log('[server] received:', msg)
    if (msg.type === 'echo') {
      void channel.send({ type: 'echo', text: msg.text }).catch(swallowClosedChannel)
    }
    if (msg.type === 'ping') {
      void channel.send({ type: 'welcome' }).catch(swallowClosedChannel)
    }
    // Return ack value to the client
    return `server-ack:${msg.type}`
  })

  let count = 0
  const intervalId = setInterval(async () => {
    const n = ++count
    try {
      const ack = await channel.send({ type: 'tick', count: n })
      console.log(`[server] tick #${n} acked by client:`, ack)
    } catch (err: any) {
      if (channel.isClosed && err?.name === 'ChannelClosedError') return
      throw err
    }
  }, 1000)

  return {
    channel: channel.client,
    serverTime: Date.now(),
  }
}

/**
 * Creates a channel that the server aborts (with a structured value) after a short delay.
 * Tests: server-side abort(value) -> client onClose receives { isAbort: true, abortValue }.
 */
async function onChannelAbortTest() {
  const channel = createChannel<never, (msg: string) => void>()
  setTimeout(() => {
    channel.abort({ reason: 'test-abort', code: 42 })
  }, 400)
  return { channel: channel.client }
}

/**
 * Creates a channel WITHOUT channel-wide ack mode.
 * Server listener returns an ack string so the client can test per-send { ack: true }.
 */
async function onChannelPerSendAck() {
  const channel = createChannel<(msg: string) => string, (msg: string) => void>()
  channel.listen((msg) => `ack:${msg}`)
  return { channel: channel.client }
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
  const channel = createChannel<never, (msg: HookServerMsg) => void>()
  const id = channel.id

  cleanupState[`hook_${id}_serverOnOpen`] = 'false'
  cleanupState[`hook_${id}_serverOnClose`] = 'false'
  cleanupState[`hook_${id}_serverOnCloseErr`] = 'none'

  channel.onOpen(() => {
    cleanupState[`hook_${id}_serverOnOpen`] = 'true'
    // Notify client in-band so it knows onOpen fired and can proceed to close
    channel.send({ type: 'server-hook', hook: 'onOpen' })
  })

  channel.onClose((err) => {
    cleanupState[`hook_${id}_serverOnClose`] = 'true'
    cleanupState[`hook_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Binary echo channel: server echoes back every binary frame it receives.
 * Used to test sendBinary/listenBinary round-trip with byte-exact verification.
 */
async function onChannelBinary() {
  const channel = createChannel()
  channel.listenBinary((data) => {
    channel.sendBinary(data)
  })
  return { channel: channel.client }
}
/**
 * Tracks server-side onClose after a client-initiated abort.
 * cleanupState keys:
 *   clientAbort_<id>_serverOnClose = 'false' | 'true'
 *   clientAbort_<id>_serverOnCloseErr = 'pending' | 'none' | 'abort:<json>' | error message
 */
async function onChannelClientAbortInstrument() {
  const channel = createChannel()
  const id = channel.id
  cleanupState[`clientAbort_${id}_serverOnClose`] = 'false'
  cleanupState[`clientAbort_${id}_serverOnCloseErr`] = 'pending'
  channel.onClose((err) => {
    cleanupState[`clientAbort_${id}_serverOnClose`] = 'true'
    cleanupState[`clientAbort_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: channel.client, channelId: id }
}

/**
 * Channel whose listener throws Abort when it receives an ack message.
 * Used to verify that a pending `send({ ack: true })` promise rejects with
 * { isAbort: true, abortValue } when the server listener aborts mid-ack.
 */
async function onChannelAckListenerAbort() {
  const channel = createChannel<(msg: string) => void, never>()
  channel.listen(() => {
    throw Abort({ reason: 'listener-abort', code: 7 })
  })
  return { channel: channel.client }
}

async function onChannelAckListenerBug() {
  const channel = createChannel<(msg: string) => string, never>()
  channel.listen((msg) => {
    if (msg === 'bug') throw new Error('server-listener-bug')
    return `ack:${msg}`
  })
  return { channel: channel.client }
}

async function onChannelClientAckListenerBug() {
  const channel = createChannel<never, (msg: string) => string>()
  const id = channel.id
  cleanupState[`clientAckBug_${id}_rejected`] = 'false'
  cleanupState[`clientAckBug_${id}_followupAck`] = 'pending'

  channel.onOpen(async () => {
    try {
      await channel.send('bug', { ack: true })
    } catch {
      cleanupState[`clientAckBug_${id}_rejected`] = 'true'
    }

    try {
      const ack = await channel.send('ok', { ack: true })
      cleanupState[`clientAckBug_${id}_followupAck`] = String(ack)
    } catch (err: any) {
      cleanupState[`clientAckBug_${id}_followupAck`] = err?.message ?? err?.name ?? 'unknown'
    }
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Tests concurrent pending ack rejection on the server side.
 *
 * On open, server does `await channel.send(data, { ack: true })`. Before the
 * client can respond, the server calls `channel.abort()`. `_shutdown()` must
 * reject the in-flight ack promise — verified via cleanupState.
 *
 * cleanupState keys:
 *   serverPendingAck_<id>_rejected  = 'false' | 'true'
 *   serverPendingAck_<id>_isAbort   = 'false' | 'true'
 */
async function onChannelServerPendingAckAbort() {
  const channel = createChannel<never, (msg: string) => void>()
  const id = channel.id

  cleanupState[`serverPendingAck_${id}_rejected`] = 'false'
  cleanupState[`serverPendingAck_${id}_isAbort`] = 'false'

  channel.onOpen(async () => {
    // Abort before the client can ack
    setTimeout(() => channel.abort({ reason: 'abort-while-ack-pending' }), 30)
    try {
      await channel.send('awaiting-ack', { ack: true })
    } catch (err: any) {
      cleanupState[`serverPendingAck_${id}_rejected`] = 'true'
      cleanupState[`serverPendingAck_${id}_isAbort`] = err instanceof Abort ? 'true' : 'false'
    }
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Case 1: abort() then send() — send() must throw ChannelClosedError synchronously.
 *
 * cleanupState keys:
 *   abortThenSend_<id>_thrown       = 'false' | 'true'
 *   abortThenSend_<id>_isClosedErr  = 'false' | 'true'
 */
async function onChannelAbortThenSend() {
  const channel = createChannel<never, (msg: string) => void>()
  const id = channel.id
  cleanupState[`abortThenSend_${id}_thrown`] = 'false'
  cleanupState[`abortThenSend_${id}_isClosedErr`] = 'false'

  channel.onOpen(() => {
    channel.abort({ reason: 'abort-before-send' })
    try {
      channel.send('test', { ack: true })
    } catch (err: any) {
      cleanupState[`abortThenSend_${id}_thrown`] = 'true'
      cleanupState[`abortThenSend_${id}_isClosedErr`] = err?.message === 'Channel is closed' ? 'true' : 'false'
    }
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Case 2: const p = send() → abort() → await p — promise must reject with abort semantics.
 *
 * cleanupState keys:
 *   pendingAbort_<id>_rejected        = 'false' | 'true'
 *   pendingAbort_<id>_isAbortErr      = 'false' | 'true'
 */
async function onChannelPendingAckAbort() {
  const channel = createChannel<never, (msg: string) => void>()
  const id = channel.id
  cleanupState[`pendingAbort_${id}_rejected`] = 'false'
  cleanupState[`pendingAbort_${id}_isAbortErr`] = 'false'

  channel.onOpen(async () => {
    const p = channel.send('awaiting-ack', { ack: true })
    channel.abort({ reason: 'abort-during-send', code: 99 })
    try {
      await p
    } catch (err: any) {
      cleanupState[`pendingAbort_${id}_rejected`] = 'true'
      cleanupState[`pendingAbort_${id}_isAbortErr`] = err instanceof Abort ? 'true' : 'false'
    }
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Minimal channel for client-side abort-then-send test.
 * The client connects, calls abort() then send() to verify ChannelClosedError is thrown.
 */
async function onChannelClientAbortThenSend() {
  const channel = createChannel<(msg: string) => void, never>()
  return { channel: channel.client }
}

/**
 * Minimal channel for client-side pending-ack close test.
 * The client connects, calls send({ ack: true }) to create a pending ack, then close().
 * With graceful close semantics, the pending send waits for an ack until close timeout,
 * then rejects with ChannelClosedError carrying the timeout message.
 */
async function onChannelClientPendingAckClose() {
  const channel = createChannel<(msg: string) => void, never>()
  return { channel: channel.client }
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
  const channel = createChannel<(msg: string) => string, never>()
  const id = channel.id
  cleanupState[`clientClose_${id}_serverOnClose`] = 'false'
  cleanupState[`clientClose_${id}_serverOnCloseErr`] = 'pending'
  channel.listen((msg) => `ack:${msg}`)
  channel.onClose((err) => {
    cleanupState[`clientClose_${id}_serverOnClose`] = 'true'
    cleanupState[`clientClose_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: channel.client, channelId: id }
}

// Global store keyed by channel id — survives across telefunc invocations.
const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')
function getServerCloseReconnectStore(): Map<string, ReturnType<typeof createChannel<(msg: string) => string, never>>> {
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
  const channel = createChannel<(msg: string) => string, never>()
  const id = channel.id
  cleanupState[`serverClose_${id}_closeResult`] = 'pending'
  cleanupState[`serverClose_${id}_serverOnCloseErr`] = 'pending'
  getServerCloseReconnectStore().set(id, channel)
  channel.onClose((err) => {
    getServerCloseReconnectStore().delete(id)
    cleanupState[`serverClose_${id}_serverOnCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: channel.client, channelId: id }
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
  const channel = createChannel<(msg: number) => void, never>()
  const id = channel.id
  cleanupState[`upstream_${id}_receivedCount`] = '0'
  cleanupState[`upstream_${id}_lastSeq`] = '0'
  cleanupState[`upstream_${id}_hasGap`] = 'false'

  let lastSeq = 0
  channel.listen((seq) => {
    const count = Number(cleanupState[`upstream_${id}_receivedCount`]) + 1
    cleanupState[`upstream_${id}_receivedCount`] = String(count)
    if (seq !== lastSeq + 1) {
      cleanupState[`upstream_${id}_hasGap`] = 'true'
    }
    lastSeq = seq
    cleanupState[`upstream_${id}_lastSeq`] = String(seq)
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Returns two independent channels ticking at different rates with distinguishable values.
 *   channel1: sends 1, 2, 3, … every 200 ms
 *   channel2: sends 100, 200, 300, … every 500 ms
 * Used to verify ix-level multiplexing over a shared WS connection.
 */
async function onChannelMulti() {
  const ch1 = createChannel<never, (msg: number) => void>()
  const ch2 = createChannel<never, (msg: number) => void>()

  let n1 = 0
  const t1 = setInterval(() => ch1.send(++n1), 200)
  ch1.onClose(() => clearInterval(t1))

  let n2 = 0
  const t2 = setInterval(() => ch2.send((n2 += 100)), 500)
  ch2.onClose(() => clearInterval(t2))

  return { channel1: ch1.client, channel2: ch2.client }
}
