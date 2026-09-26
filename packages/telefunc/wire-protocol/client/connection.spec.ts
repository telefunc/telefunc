import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  RECONCILE_TIMEOUT_MS,
} from '../constants.js'
import { ClientConnection } from './connection.js'
import { TAG, encode } from '../shared-ws.js'
import { config, getServerConfig } from '../../node/server/serverConfig.js'

/** Minimal `MuxChannel` — registering one is enough to make the connection open a wire. */
function createChannel(id = crypto.randomUUID()) {
  return {
    id,
    isClosed: false,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onTransportClose() {},
  }
}

/** A `fetch` whose SSE downstream body never emits a byte and never closes: a silently
 *  stalled stream (no error, no FIN), so the RECONCILED frame that clears `reconciling`
 *  never arrives. Counts how many times the downstream (`Accept: text/event-stream`) is
 *  opened, i.e. how many connect attempts ran. */
function createStalledTransport() {
  let sseDownstreamOpens = 0
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const headers = (init.headers ?? {}) as Record<string, string>
    if (headers.Accept === 'text/event-stream') sseDownstreamOpens++
    return new Response(new ReadableStream<Uint8Array>({ start() {} }), { status: 200 })
  }) as unknown as typeof fetch
  return { fetchImpl, getSseDownstreamOpens: () => sseDownstreamOpens }
}

function stalledOptions() {
  return {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: createStalledTransport().fetchImpl,
    connectionKey: crypto.randomUUID(),
  }
}

/** A connection that applied a RECONCILED whose every setting is zero. */
function zeroConfiguredConnection() {
  const options = {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: createStalledTransport().fetchImpl,
    connectionKey: crypto.randomUUID(),
  }
  const connection = ClientConnection.getOrCreate('http://zero.test', createChannel() as never, options) as any
  const ctrl = new Proxy(
    { sessionId: 'zero', open: [], transports: [CHANNEL_TRANSPORT.SSE] },
    { get: (target, key) => Reflect.get(target, key) ?? 0 },
  )
  connection.applyReconciled(ctrl)
  connection.transport.applyReconciledSettings(ctrl)
  return { connection, options }
}

test('channel config preserves zero through server and client resolution', () => {
  config.channel.reconnectTimeout = 0
  expect(getServerConfig().channel.reconnectTimeout).toBe(0)
  const { connection } = zeroConfiguredConnection()
  expect([
    connection.reconnectTimeoutMs,
    connection.idleTimeoutMs,
    connection.clientReplayBufferBytes,
    connection.clientReplayBufferBinaryBytes,
    connection.transport.flushThrottleMs,
    connection.transport.postIdleFlushDelayMs,
  ]).toEqual(Array(6).fill(0))
  connection.dispose()
})

test("a per-call idleTimeout is kept over the server's", () => {
  const options = { ...stalledOptions(), idleTimeout: 0 }
  const connection = ClientConnection.getOrCreate('http://idle.test', createChannel() as never, options) as any
  connection.applyReconciled({ sessionId: 'idle', open: [], idleTimeout: 60_000 }, null)
  expect(connection.idleTimeoutMs).toBe(0)
  connection.dispose()
})

test('a zero replay budget still registers a later channel on the connection', () => {
  const { connection, options } = zeroConfiguredConnection()
  expect(ClientConnection.getOrCreate('http://zero.test', createChannel() as never, options)).toBe(connection)
  connection.dispose()
})

test("a reconnect declares a broadcast's subscriptions, not the toggles queued before it", () => {
  const channel = { ...createChannel(), _reattachState: () => ({ broadcast: { text: true, binary: false } }) }
  const connection = ClientConnection.getOrCreate('http://toggle.test', channel as never, {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: createStalledTransport().fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  // Offline, the listener is swapped: an unsubscribe, then a subscribe. The reconcile entry already says subscribed.
  connection.sendBroadcastUnsubscribe(channel, false)
  connection.sendBroadcastSubscribe(channel, false)
  const { movedBufferedFrames } = connection.stageReconcileBatch()
  const queued: Array<number | undefined> = [...connection.sendBuffer, ...movedBufferedFrames].map(
    ({ frame }: { frame: Uint8Array }) => frame[0],
  )
  expect(queued.filter((tag) => tag === TAG.BROADCAST_SUB || tag === TAG.BROADCAST_UNSUB)).toEqual([])
  connection.dispose()
})

test('an SSE reconnect sends its reconcile, not the reconcile and toggles a failed POST left queued', () => {
  const channel = { ...createChannel(), _reattachState: () => ({ broadcast: { text: true, binary: false } }) }
  const connection = ClientConnection.getOrCreate('http://outbox.test', channel as never, {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: createStalledTransport().fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  // A batch POST that failed carried an older reconcile and an unsubscribe; the channel is subscribed again since.
  connection.transport.outbox.push(
    { frame: encode.reconcile({ open: [] }), deadline: Infinity },
    { frame: encode.broadcastUnsub(0, false), deadline: Infinity },
  )
  const { initialFrames } = connection.transport.stageInitialBatch()
  const tags = initialFrames.map(({ frame }: { frame: Uint8Array }) => frame[0])
  expect(tags.filter((tag: number) => tag === TAG.RECONCILE)).toHaveLength(1)
  expect(tags.filter((tag: number) => tag === TAG.BROADCAST_SUB || tag === TAG.BROADCAST_UNSUB)).toEqual([])
  connection.dispose()
})

test('the channel cap counts the open channels, not every channel the connection opened', () => {
  const options = stalledOptions()
  const connection = ClientConnection.getOrCreate('http://cap.test', createChannel() as never, options) as any
  connection.nextIndex = MAX_CHANNELS_PER_CONNECTION // what 4,095 channels opened and closed on it leave
  for (let open = 1; open < MAX_CHANNELS_PER_CONNECTION; open++) {
    expect(ClientConnection.getOrCreate('http://cap.test', createChannel() as never, options)).toBe(connection)
  }
  expect(() => ClientConnection.getOrCreate('http://cap.test', createChannel() as never, options)).toThrow(
    'Too many channels',
  )
  connection.dispose()
})

test('a connection out of wire indexes hands a new channel to a fresh connection, which its dispose leaves cached', () => {
  const options = stalledOptions()
  const spent = ClientConnection.getOrCreate('http://rotate.test', createChannel() as never, options) as any
  spent.nextIndex = 0x10000
  const fresh = ClientConnection.getOrCreate('http://rotate.test', createChannel() as never, options) as any
  expect(fresh).not.toBe(spent)
  spent.dispose()
  expect(ClientConnection.getOrCreate('http://rotate.test', createChannel() as never, options)).toBe(fresh)
  fresh.dispose()
})

test('a buffered acked binary send is kept in the binary replay lane, not the text one', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://binary-ack.test', channel as never, stalledOptions()) as any
  connection.sendBinaryAckReq(channel, new Uint8Array(1_500_000), () => {}) // over the text lane's 1 MiB, within binary's 2
  connection.drainBufferedFrames(new Set([0]))
  expect(connection.replayBuffers.get(0).getAfter(0)).toHaveLength(1)
  connection.dispose()
})

test('a sent frame stays replayable through the pong deadline and the reconnect timeout after it', () => {
  const channel = createChannel()
  const connection = ClientConnection.getOrCreate('http://replay-age.test', channel as never, stalledOptions()) as any
  const replay = connection.replayBuffers.get(0)
  replay.push(replay.nextSeq(), encode.text(0, 'sent as the wire died', 1))
  replay.evict(Date.now() + 2 * connection.pingIntervalMs + connection.reconnectTimeoutMs)
  expect(replay.getAfter(0)).toHaveLength(1)
  connection.dispose()
})

describe('SSE reconcile watchdog', () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test('reconnects when RECONCILED never arrives on a stalled downstream', async () => {
    vi.useFakeTimers()
    const { fetchImpl, getSseDownstreamOpens } = createStalledTransport()

    ClientConnection.getOrCreate('http://test.local/_telefunc', createChannel() as never, {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // `start()` defers the first openStream by one reconcile window; let it connect.
    await vi.advanceTimersByTimeAsync(100)
    expect(getSseDownstreamOpens()).toBe(1)

    // The downstream is silent, so RECONCILED never lands. Without the watchdog the
    // connection wedges here forever: pings keep flowing but `handlePongTimeout` is
    // suppressed while reconciling, so the dead wire is never noticed. The watchdog must
    // instead time out the reconcile and reconnect.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(getSseDownstreamOpens()).toBeGreaterThanOrEqual(2)
  })
})
