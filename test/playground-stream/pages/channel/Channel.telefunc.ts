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
  onChannelPeerShorterClose,
  onChannelLongerCloseAfterPeer,
  onChannelServerPendingAckCloseReconnectOpen,
  onChannelUpstreamReconnect,
  onChannelNoListenerAckServer,
  onChannelNoListenerAckClient,
  onChannelShieldClient,
  onChannelShieldServerAck,
  onChannelSelfRemovingListener,
  onChannelUnserializableAnswer,
  onChannelOversize,
  onChannelHeldBinary,
  onChannelUnattachedFlood,
  onChannelEndedValues,
  onChannelSlowLink,
  onChannelBinaryBurst,
  onChannelPaced,
  onChannelTextBurst,
  onChannelOpenBurst,
  onChannelSmallPush,
  onChannelManyListeners,
  onChannelLinkShare,
  onChannelSlowUplink,
  onChannelOpenAnswer,
  onChannelOpenHello,
  onChannelConfigChecks,
  onChannelBudgets,
  onChannelAwaitedFromOffline,
  onChannelOfflineBudgets,
  onChannelAtLimit,
  onChannelOwnBytes,
  onChannelCreditFailure,
  onChannelAckOwnership,
  onChannelApiProbe,
  onChannelIteratorAck,
  onChannelLateCallbacks,
  onChannelOpenAbort,
  onChannelLateOpenAbort,
}

import { prepareIteratorAck, sendIteratorAck, type IteratorAckOptions } from './iteratorAckProbe'
import { sendOwnBytes, listenOwnBytesAlone, listenOwnBytesBeside } from './ownBytesProbe'

import { Channel, Abort, BroadcastChannel, config } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type Ack = string
type ClientToServer = (msg: ClientMessage) => Ack
type ServerToClient = (msg: ServerMessage) => Ack
type CloseError = { message?: string; abortValue?: unknown } | undefined

function exerciseChannelTypeApi() {
  const ch = new Channel<(msg: string) => undefined, (msg: number) => Promise<void>>({ ack: true })

  const ackFromClient: Promise<void> = ch.send(1)
  void ackFromClient

  ch.listen((msg) => {
    const text: string = msg
    void text
    return undefined
  })

  const oneWay = new Channel<never, (msg: string) => void>()
  const ackFromServer: Promise<void> = oneWay.send('hello', { ack: true })
  void ackFromServer
}

function formatCloseError(err: CloseError): string {
  if (!err) return 'none'
  if (err instanceof Abort) return `abort:${JSON.stringify(err.abortValue)}`
  return err.message ?? 'unknown'
}

async function onChannelInit() {
  const ch = new Channel<ClientToServer, ServerToClient>({ ack: true })
  const swallowClosedChannel = (err: any) => {
    if (ch.isClosed && (err?.name === 'ChannelClosedError' || err instanceof Abort)) return
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
      swallowClosedChannel(err)
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
  const ch = new Channel<never, (msg: string) => void>()
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
  const ch = new Channel<(msg: string) => string, (msg: string) => void>()
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
  const ch = new Channel<never, (msg: HookServerMsg) => void>()
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
  const ch = new Channel()
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
  const ch = new Channel()
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
  const ch = new Channel<(msg: string) => void, never>()
  ch.listen(() => {
    throw Abort({ reason: 'listener-abort', code: 7 })
  })
  return { channel: ch.client }
}

async function onChannelAckListenerBug() {
  const ch = new Channel<(msg: string) => string, never>()
  ch.listen((msg) => {
    if (msg === 'bug') throw new Error('server-listener-bug')
    return `ack:${msg}`
  })
  return { channel: ch.client }
}

async function onChannelClientAckListenerBug() {
  const ch = new Channel<never, (msg: string) => string>()
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
 * client can respond, the server calls `ch.abort()`. `shutdown()` must
 * reject the in-flight ack promise — verified via cleanupState.
 *
 * cleanupState keys:
 *   serverPendingAck_<id>_rejected  = 'false' | 'true'
 *   serverPendingAck_<id>_isAbort   = 'false' | 'true'
 */
async function onChannelServerPendingAckAbort() {
  const ch = new Channel<never, (msg: string) => void>()
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
  const ch = new Channel<never, (msg: string) => void>()
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
  const ch = new Channel<never, (msg: string) => void>()
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
  const ch = new Channel<(msg: string) => void, never>()
  return { channel: ch.client }
}

/**
 * Minimal channel for client-side pending-ack close test.
 * The client connects, calls send({ ack: true }) to create a pending ack, then close().
 * With graceful close semantics, the pending send waits for an ack until close timeout,
 * then rejects with ChannelClosedError carrying the timeout message.
 */
async function onChannelClientPendingAckClose() {
  const ch = new Channel<(msg: string) => void, never>()
  ch.listen(() => new Promise(() => {}))
  return { channel: ch.client }
}

/**
 * A close the page cuts short. The page sends a message with an ack the server never answers; the server tells the
 * page it closes and closes within 60 s, waiting on that ack; the page answers with a close within 1 s.
 * cleanupState keys:
 *   peerShorterClose_<id>_closeMs    = how long the server's close() took
 *   peerShorterClose_<id>_onCloseErr = 'none' | error message
 */
async function onChannelPeerShorterClose() {
  const ch = new Channel<(msg: string) => void, (msg: string) => void>()
  const id = ch.id
  ch.listen(() => {
    void ch.send('closing')
    const start = performance.now()
    void ch.close({ timeout: 60_000 }).then(() => {
      cleanupState[`peerShorterClose_${id}_closeMs`] = String(Math.round(performance.now() - start))
    })
    return new Promise(() => {})
  })
  ch.onClose((err) => {
    cleanupState[`peerShorterClose_${id}_onCloseErr`] = formatCloseError(err as CloseError)
  })
  return { channel: ch.client, channelId: id }
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
  const ch = new Channel<(msg: string) => string, never>()
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

/**
 * A close() the page makes after the server's. The page asks the server to close within 1 s; the page's onClose never
 * settles, so its close lasts until its deadline, which its own close() within 60 s must not push back.
 */
async function onChannelLongerCloseAfterPeer() {
  const ch = new Channel<(msg: string) => void, never>()
  ch.listen(() => {
    void ch.close({ timeout: 1_000 })
  })
  return { channel: ch.client }
}

// Global store keyed by channel id — survives across telefunc invocations.
const SERVER_CLOSE_RECONNECT_STORE_KEY = Symbol.for('telefunc__serverCloseReconnectStore')
function getServerCloseReconnectStore(): Map<string, Channel<never, (msg: string) => string>> {
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
  const ch = new Channel<never, (msg: string) => string>()
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
  const ch = new Channel<(msg: number) => void, never>()
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
  const ch1 = new Channel<never, (msg: number) => void>()
  const ch2 = new Channel<never, (msg: number) => void>()

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
  const ch = new Channel<never, (msg: string) => string>()
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
  const ch = new Channel<(msg: string) => string, never>()
  // No ch.listen() — intentionally missing
  return { channel: ch.client }
}

/** Shield validates client-sent data. Typed as `(msg: string) => number`:
 *  - No-ack: valid string arrives, invalid is silently dropped server-side (no client error).
 *  - With-ack: valid returns the length; invalid rejects the client's send with a shield error. */
async function onChannelShieldClient() {
  const ch = new Channel<(msg: string) => number>()
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
  const ch = new Channel<never, (msg: number) => string>({ ack: true })
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

const WIRE_LIMIT_BYTES = 64 * 1024 * 1024

function settle(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    (value) => `ok:${String(value)}`,
    (err: any) => `error:${err?.message}`,
  )
}

/** Each end listens twice, and its first listener stops listening on its first message. */
async function onChannelSelfRemovingListener() {
  const ch = new Channel<(msg: string) => void, (msg: string) => void>()
  const serverSeen: string[] = []
  const unlisten = ch.listen((msg) => {
    serverSeen.push(`once:${msg}`)
    unlisten()
  })
  ch.listen((msg) => {
    serverSeen.push(`other:${msg}`)
  })
  ch.onOpen(async () => {
    await ch.send('one')
    await ch.send('two')
  })
  return { channel: ch.client, getServerSeen: async () => serverSeen }
}

/** Each end answers 'cyclic' with an object that can't be serialized. */
async function onChannelUnserializableAnswer() {
  const ch = new Channel<(msg: string) => unknown, (msg: string) => unknown>()
  const id = ch.id
  ch.listen((msg) => {
    if (msg !== 'cyclic') return `ack:${msg}`
    const cyclic: { self?: unknown } = {}
    cyclic.self = cyclic
    return cyclic
  })
  ch.onOpen(async () => {
    cleanupState[`unserializable_${id}_serverSend`] = await settle(ch.send('cyclic', { ack: true }))
    cleanupState[`unserializable_${id}_serverFollowUp`] = await settle(ch.send('ok', { ack: true }))
  })
  return { channel: ch.client, channelId: id }
}

/** The server asks for an answer the page makes larger than the server takes; the page publishes as large on `broadcast`. */
async function onChannelOversize() {
  const ch = new Channel<(msg: string) => string, (msg: string) => string>()
  const id = ch.id
  ch.listen((msg) => `ack:${msg}`)
  ch.onOpen(async () => {
    cleanupState[`oversize_${id}_serverSend`] = await settle(ch.send('big', { ack: true }))
    cleanupState[`oversize_${id}_serverFollowUp`] = await settle(ch.send('ok', { ack: true }))
  })
  return {
    channel: ch.client,
    channelId: id,
    limit: WIRE_LIMIT_BYTES,
    broadcast: new BroadcastChannel({ key: `oversize:${crypto.randomUUID()}` }),
  }
}

/** Sends binary before the page attaches, reusing each buffer for the next send. */
async function onChannelHeldBinary() {
  const ch = new Channel()
  const bytes = new Uint8Array(3)
  const buffer = Buffer.alloc(3)
  const sends: Array<[Uint8Array, number, boolean]> = [
    [bytes, 1, false],
    [bytes, 2, true],
    [buffer, 3, false],
    [buffer, 4, true],
  ]
  for (const [data, value, ack] of sends) {
    data.fill(value)
    void (ack ? ch.sendBinary(data, { ack: true }) : ch.sendBinary(data))
  }
  bytes.fill(9)
  buffer.fill(9)
  return { channel: ch.client }
}

/** Sends 100,000 empty messages of each kind to an unattached channel; counts rejections by error. */
async function onChannelUnattachedFlood() {
  const rejected: Record<'text' | 'binary', Record<string, number>> = { text: {}, binary: {} }
  for (const kind of ['text', 'binary'] as const) {
    const ch = new Channel<never, string>()
    const counts: Record<string, number> = {}
    for (let i = 0; i < 100_000; i++) {
      const send = kind === 'text' ? ch.send('') : ch.sendBinary(new Uint8Array(0))
      send.catch((err: Error) => {
        counts[err.name] = (counts[err.name] ?? 0) + 1
      })
    }
    await new Promise((resolve) => setImmediate(resolve))
    rejected[kind] = { ...counts }
    ch.abort()
  }
  return rejected
}

/** A channel, a broadcast and a generator, each for the page to end. */
async function onChannelEndedValues() {
  async function* numbers() {
    yield 1
    yield 2
  }
  return {
    channel: new Channel().client,
    broadcast: new BroadcastChannel({ key: `ended-values:${crypto.randomUUID()}` }),
    generator: numbers(),
  }
}

/** Sends the page as many bytes as its message asks for. */
async function onChannelSlowLink() {
  const ch = new Channel<(bytes: number) => Promise<void>, never>()
  ch.listen(async (bytes) => {
    await ch.sendBinary(new Uint8Array(bytes))
  })
  return { channel: ch.client }
}

/**
 * At the page's message, sends that many 1 MiB messages, each with its index in its first 4 bytes, in one loop:
 * awaiting each, or not.
 * cleanupState keys:
 *   binaryBurst_<id> = why the first refused send was refused, if one was
 * Then sends `end`, once one fits.
 */
async function onChannelBinaryBurst() {
  const ch = new Channel<(run: { count: number; awaited: boolean }) => void, string>()
  const refused = (err: Error) => {
    cleanupState[`binaryBurst_${ch.id}`] ??= err.message
  }
  ch.listen(async ({ count, awaited }) => {
    for (let i = 0; i < count; i++) {
      const message = new Uint8Array(1024 * 1024)
      new DataView(message.buffer).setUint32(0, i)
      if (awaited) await ch.sendBinary(message).catch(refused)
      else void ch.sendBinary(message).catch(refused)
    }
    while (!(await sendFitted(ch, 'end'))) await new Promise((resolve) => setTimeout(resolve, 100))
  })
  return { channel: ch.client, channelId: ch.id }
}

async function sendFitted(ch: Channel<any, string>, message: string): Promise<boolean> {
  try {
    await ch.send(message)
    return true
  } catch {
    return false
  }
}

/**
 * At the page's message, sends `burst` 1 MiB messages at once, then a message of `tick` bytes every `everyMs` ms,
 * `ticks` times, none of them awaited, until one is refused.
 * cleanupState keys:
 *   paced_<id> = why that send was refused, or `done`
 */
async function onChannelPaced() {
  const ch = new Channel<(run: { burst: number; tick: number; everyMs: number; ticks: number }) => void, never>()
  ch.listen(async ({ burst, tick, everyMs, ticks }) => {
    let refusal: string | undefined
    const refused = (err: Error) => {
      refusal ??= err.message
    }
    for (let i = 0; i < burst; i++) void ch.sendBinary(new Uint8Array(1024 * 1024)).catch(refused)
    for (let i = 0; i < ticks && refusal === undefined; i++) {
      await new Promise((resolve) => setTimeout(resolve, everyMs))
      void ch.sendBinary(new Uint8Array(tick)).catch(refused)
    }
    cleanupState[`paced_${ch.id}`] = refusal ?? 'done'
  })
  return { channel: ch.client, channelId: ch.id }
}

/**
 * At the page's message, sends that many text messages, each starting with its index, in one loop: of 16 KiB, without
 * awaiting them or awaiting each, or of 1 KiB with `{ ack: true }`, awaiting the acks together.
 * cleanupState keys:
 *   textBurst_<id>_error = why the first refused send was refused, if one was
 *   textBurst_<id>_acked = how many of the ack-requesting sends resolved
 * After the acks settle, sends `end`.
 */
async function onChannelTextBurst() {
  const ch = new Channel<(run: { count: number; mode: 'unawaited' | 'awaited' | 'acks' }) => void, string>()
  const id = ch.id
  ch.listen(async ({ count, mode }) => {
    const sends: Promise<unknown>[] = []
    for (let i = 0; i < count; i++) {
      const message = String(i).padEnd(mode === 'acks' ? 64 * 1024 : 16 * 1024)
      if (mode === 'acks') sends.push(ch.send(message, { ack: true }))
      else if (mode === 'awaited') await ch.send(message)
      else void ch.send(message)
    }
    if (mode !== 'acks') return
    const results = await Promise.allSettled(sends)
    const firstRefused = results.find((r) => r.status === 'rejected')
    if (firstRefused) cleanupState[`textBurst_${id}_error`] = firstRefused.reason.message
    cleanupState[`textBurst_${id}_acked`] = String(results.filter((r) => r.status === 'fulfilled').length)
    void ch.send('end')
  })
  return { channel: ch.client, channelId: id }
}

/** As it opens, sends 500 small messages without awaiting them. */
async function onChannelOpenBurst() {
  const ch = new Channel<never, number>()
  ch.onOpen(() => {
    for (let seq = 0; seq < 500; seq++) void ch.send(seq).catch(() => {})
  })
  return { channel: ch.client }
}

/** At the page's message, sends that many small messages without awaiting them. */
async function onChannelSmallPush() {
  const ch = new Channel<(count: number) => Promise<void>, (seq: number) => void>()
  ch.listen(async (count) => {
    for (let seq = 0; seq < count; seq++) void ch.send(seq)
  })
  return { channel: ch.client }
}

/** At the page's message, adds that many listeners and removes them, and answers with the milliseconds it took. */
async function onChannelManyListeners() {
  const ch = new Channel<(count: number) => number, never>()
  ch.listen((count) => {
    const start = performance.now()
    const removes = Array.from({ length: count }, () => ch.listen(() => 0))
    for (const remove of removes) remove()
    return Math.round(performance.now() - start)
  })
  return { channel: ch.client }
}

/** A download channel that sends the bytes the page asks for, awaiting each 64 KiB, and a channel that answers pings. */
async function onChannelLinkShare() {
  const download = new Channel<(bytes: number) => void, never>()
  download.listen(async (bytes) => {
    for (let sent = 0; sent < bytes && !download.isClosed; sent += 64 * 1024)
      await download.sendBinary(new Uint8Array(64 * 1024))
  })
  const ping = new Channel<(msg: string) => string, never>()
  ping.listen(() => 'pong')
  return { download: download.client, ping: ping.client }
}

/** An upload channel that answers with the bytes it received, and a channel that ticks every 100 ms until the page closes it. */
async function onChannelSlowUplink() {
  const upload = new Channel<(msg: 'received') => number, never>()
  let received = 0
  upload.listenBinary((data) => {
    received += data.byteLength
  })
  upload.listen(() => received)
  const ticks = new Channel<never, (tick: number) => void>()
  let tick = 0
  const timer = setInterval(() => {
    if (ticks.isClosed) return clearInterval(timer)
    void ticks.send(tick++).catch(() => {})
  }, 100)
  return { upload: upload.client, ticks: ticks.client }
}

/** A channel that answers. */
async function onChannelOpenAnswer() {
  const ch = new Channel<(msg: string) => string, never>()
  ch.listen(() => 'answer')
  return { channel: ch.client }
}

async function onChannelOpenHello() {
  const ch = new Channel<never, string>()
  ch.onOpen(() => void ch.send('hello').catch(() => {}))
  return { channel: ch.client }
}

/** Registers an onOpen once the channel opened and an onClose once it closed: each runs at once, the onClose with what
 *  the channel ended with. */
async function onChannelLateCallbacks() {
  const ch = new Channel<never, string>()
  const id = ch.id
  ch.onOpen(() => {
    setTimeout(() => {
      let ranAtOnce = false
      ch.onOpen(() => {
        ranAtOnce = true
      })
      cleanupState[`late_${id}_onOpen`] = String(ranAtOnce)
      void ch.send('opened').catch(() => {})
    })
  })
  ch.onClose(() => {
    setTimeout(() => {
      let closeErr = 'not run'
      ch.onClose((err) => {
        closeErr = formatCloseError(err as CloseError)
      })
      cleanupState[`late_${id}_onClose`] = closeErr
    })
  })
  return { channel: ch.client, channelId: id }
}

/** Its first onOpen throws an Abort: the channel aborts with that value, and the onOpen after it doesn't run. */
async function onChannelOpenAbort() {
  const ch = new Channel<never, never>()
  const id = ch.id
  cleanupState[`openAbort_${id}_second`] = 'not run'
  ch.onOpen(() => {
    throw Abort('stop')
  })
  ch.onOpen(() => {
    cleanupState[`openAbort_${id}_second`] = 'run'
  })
  return { channel: ch.client, channelId: id }
}

async function onChannelApiProbe() {
  const ch = new Channel<string, string>()
  ch.onOpen(async () => {
    await ch.opened
    for await (const message of ch.messages()) {
      if (message.type === 'text') await ch.send(message.data)
      else await ch.sendBinary(message.data)
    }
  })
  return { channel: ch.client }
}

async function onChannelAckOwnership() {
  const ch = new Channel<string, string>()
  ch.listen(() => 'answer')
  ch.listen((data) => (data === 'conflict' ? 'other' : undefined))
  return { channel: ch.client }
}

/** What each assignment `config.channel` refuses says, now that a channel ran; `null` for one it accepts. */
async function onChannelConfigChecks() {
  new Channel<never, never>().abort()
  const assign = (channel: object) => {
    try {
      config.channel = { pingInterval: 1000, ...channel }
      return null
    } catch (err) {
      return (err as Error).message
    }
  }
  return {
    same: assign({}),
    changed: assign({ reconnectTimeout: 61_000 }),
    reconnectTimeout: assign({ reconnectTimeout: 999 }),
    pingInterval: assign({ pingInterval: 999 }),
    maxPendingBytes: assign({ maxPendingBytes: 2 * 1024 * 1024 - 1 }),
    removed: assign({ bufferLimit: 1 }),
  }
}

async function onChannelBudgets() {
  const bytes = new Channel<never, string>()
  const refused = (err: Error) => {
    cleanupState[`budget_${bytes.id}`] ??= err.message
  }
  bytes.onOpen(() => {
    void bytes.send('x'.repeat(4 * 1024)).then(() => (cleanupState[`budget_${bytes.id}_credit`] = 'accepted'))
    for (let i = 0; i < 6000; i++) void bytes.send('x'.repeat(1024)).catch(refused)
  })
  return { bytes: bytes.client }
}

/**
 * With a `maxPendingBytes` of 2 MiB, awaits a send of 1 KiB before the page attaches, one of 5 MiB, and one of 1 KiB.
 * cleanupState: awaitedFromOffline_<id> = for each send, `sent` or why it was refused
 */
async function onChannelAwaitedFromOffline() {
  const ch = new Channel<never, string>()
  void (async () => {
    const sends: string[] = []
    for (const bytes of [1024, 5 * 1024 * 1024, 1024]) {
      try {
        await ch.sendBinary(new Uint8Array(bytes))
        sends.push('sent')
      } catch (err) {
        sends.push((err as Error).message)
      }
    }
    cleanupState[`awaitedFromOffline_${ch.id}`] = JSON.stringify(sends)
  })()
  return { channel: ch.client, channelId: ch.id }
}

/** One channel per kind of text, each sending as its first send what fits `maxPendingBytes` to the byte, and one per
 *  kind sending one byte more. cleanupState: atLimit_<kind>_<fits|over> = `accepted`, or why the send was refused. */
async function onChannelAtLimit() {
  // 2 MiB is these 2,096,631 UTF-8 bytes, the two quotes around them, a frame's 7 bytes and a held send's 512.
  const texts = {
    twoBytes: `${'é'.repeat(1_048_315)}x`,
    threeBytes: '€'.repeat(698_877),
    fourBytes: `${'😀'.repeat(524_157)}xxx`,
  }
  const open = (name: string, text: string) => {
    const ch = new Channel<never, string>()
    ch.onOpen(() => {
      cleanupState[`atLimit_${name}`] = 'accepted'
      ch.send(text, { ack: true }).catch((err: Error) => {
        // The page has no listener to answer the send that went.
        if (err.name === 'ChannelOverflowError') cleanupState[`atLimit_${name}`] = err.message
      })
    })
    return ch.client
  }
  const channels = Object.entries(texts).flatMap(([kind, text]) => [
    open(`${kind}_fits`, text),
    open(`${kind}_over`, `${text}x`),
  ])
  return channels
}

/** cleanupState: ownBytes_<alone|beside> = how many frames from the page weren't its listeners' own. */
async function onChannelOwnBytes() {
  const alone = new Channel<never, never>()
  const beside = new Channel<never, never>()
  listenOwnBytesAlone(alone, (notOwn) => void (cleanupState.ownBytes_alone = String(notOwn)))
  listenOwnBytesBeside(beside, (notOwn) => void (cleanupState.ownBytes_beside = String(notOwn)))
  for (const ch of [alone, beside]) ch.onOpen(() => void sendOwnBytes(ch))
  return { alone: alone.client, beside: beside.client }
}

async function onChannelOfflineBudgets() {
  const ch = new Channel<never, string>()
  // Sent before the page attaches: they wait in the channel's buffer until it does.
  const sends = Array.from({ length: 3 }, () => ch.send('x'.repeat(800 * 1024), { ack: true }))
  void Promise.allSettled(sends).then((results) => {
    cleanupState[`offline_budget_${ch.id}`] = JSON.stringify(
      results.map((r) => (r.status === 'rejected' ? `${r.reason.name}: ${r.reason.message}` : 'answered')),
    )
  })
  return { channel: ch.client }
}

async function onChannelCreditFailure() {
  const ch = new Channel<string, string>()
  ch.listen(() => new Promise<void>(() => {}))
  ch.onOpen(() => {
    const sends = Array.from({ length: 300 }, () => ch.send('x'))
    ch.abort('capacity-failure')
    void Promise.allSettled(sends).then((results) => {
      cleanupState[`credit_${ch.id}`] = String(
        results.some((r) => r.status === 'rejected' && r.reason.abortValue === 'capacity-failure'),
      )
    })
  })
  return { channel: ch.client }
}

async function onChannelLateOpenAbort() {
  const ch = new Channel<never, never>()
  ch.onOpen(() => {
    setTimeout(() =>
      ch.onOpen(() => {
        throw Abort('late-stop')
      }),
    )
  })
  return { channel: ch.client }
}

async function onChannelIteratorAck() {
  const channel = new Channel<string, string>()
  let receiver: ReturnType<typeof prepareIteratorAck>
  return {
    channel: channel.client,
    prepare: (options: IteratorAckOptions) => {
      receiver = prepareIteratorAck(channel, options)
    },
    primed: () => receiver.primed(),
    cancel: () => receiver.cancel(),
    answers: () => receiver.answers(),
    send: (options: { binary: boolean; prime: boolean }) => sendIteratorAck(channel, options),
  }
}
