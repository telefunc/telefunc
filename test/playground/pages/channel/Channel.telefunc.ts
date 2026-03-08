export {
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
}

import { createChannel, Abort } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type Ack = string

async function onChannelInit() {
  const channel = createChannel<ServerMessage, ClientMessage, Ack, Ack>({ ack: true })

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
      channel.send({ type: 'echo', text: msg.text })
    }
    if (msg.type === 'ping') {
      channel.send({ type: 'welcome' })
    }
    // Return ack value to the client
    return `server-ack:${msg.type}`
  })

  let count = 0
  const intervalId = setInterval(async () => {
    const n = ++count
    const ack = await channel.send({ type: 'tick', count: n })
    console.log(`[server] tick #${n} acked by client:`, ack)
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
  const channel = createChannel<string, never>()
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
  const channel = createChannel<string, string>()
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
  const channel = createChannel<HookServerMsg, never>()
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
    const e = err as any
    cleanupState[`hook_${id}_serverOnCloseErr`] = err
      ? e?.isAbort
        ? `abort:${JSON.stringify(e.abortValue)}`
        : err.message
      : 'none'
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
    const e = err as any
    cleanupState[`clientAbort_${id}_serverOnClose`] = 'true'
    cleanupState[`clientAbort_${id}_serverOnCloseErr`] = err
      ? e?.isAbort
        ? `abort:${JSON.stringify(e.abortValue)}`
        : err.message
      : 'none'
  })
  return { channel: channel.client, channelId: id }
}

/**
 * Channel whose listener throws Abort when it receives an ack message.
 * Used to verify that a pending `send({ ack: true })` promise rejects with
 * { isAbort: true, abortValue } when the server listener aborts mid-ack.
 */
async function onChannelAckListenerAbort() {
  const channel = createChannel<never, string>()
  channel.listen(() => {
    throw Abort({ reason: 'listener-abort', code: 7 })
  })
  return { channel: channel.client }
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
  const channel = createChannel<string, never>()
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
      cleanupState[`serverPendingAck_${id}_isAbort`] = err?.isAbort ? 'true' : 'false'
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
  const channel = createChannel<string, never>()
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
 * Case 2: const p = send() → abort() → await p — promise must reject with ChannelClosedError.
 *
 * cleanupState keys:
 *   pendingAbort_<id>_rejected        = 'false' | 'true'
 *   pendingAbort_<id>_isClosedErr     = 'false' | 'true'
 */
async function onChannelPendingAckAbort() {
  const channel = createChannel<string, never>()
  const id = channel.id
  cleanupState[`pendingAbort_${id}_rejected`] = 'false'
  cleanupState[`pendingAbort_${id}_isClosedErr`] = 'false'

  channel.onOpen(async () => {
    const p = channel.send('awaiting-ack', { ack: true })
    channel.abort({ reason: 'abort-during-send', code: 99 })
    try {
      await p
    } catch (err: any) {
      cleanupState[`pendingAbort_${id}_rejected`] = 'true'
      cleanupState[`pendingAbort_${id}_isClosedErr`] = err?.message === 'Channel is closed' ? 'true' : 'false'
    }
  })

  return { channel: channel.client, channelId: id }
}

/**
 * Minimal channel for client-side abort-then-send test.
 * The client connects, calls abort() then send() to verify ChannelClosedError is thrown.
 */
async function onChannelClientAbortThenSend() {
  const channel = createChannel<never, string>()
  return { channel: channel.client }
}

/**
 * Minimal channel for client-side pending-ack close test.
 * The client connects, calls send({ ack: true }) to create a pending ack, then close().
 * The awaited promise must reject with ChannelClosedError.
 */
async function onChannelClientPendingAckClose() {
  const channel = createChannel<never, string>()
  return { channel: channel.client }
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
  const channel = createChannel<never, number>()
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
  const ch1 = createChannel<number, never>()
  const ch2 = createChannel<number, never>()

  let n1 = 0
  const t1 = setInterval(() => ch1.send(++n1), 200)
  ch1.onClose(() => clearInterval(t1))

  let n2 = 0
  const t2 = setInterval(() => ch2.send((n2 += 100)), 500)
  ch2.onClose(() => clearInterval(t2))

  return { channel1: ch1.client, channel2: ch2.client }
}
