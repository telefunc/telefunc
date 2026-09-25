import { afterEach, describe, expect, test, vi } from 'vitest'

import { CHANNEL_RECONNECT_INITIAL_DELAY_MS, CHANNEL_TRANSPORT, RECONCILE_TIMEOUT_MS } from '../constants.js'
import { ClientConnection } from './connection.js'
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

test.each([Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1])('channel config rejects %s', (value) => {
  expect(() => (config.channel.reconnectTimeout = value)).toThrow('non-negative safe integer')
})
test('channel config preserves zero through server and client resolution', () => {
  config.channel.reconnectTimeout = 0
  expect(getServerConfig().channel.reconnectTimeout).toBe(0)
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
  expect([
    connection.reconnectTimeoutMs,
    connection.idleTimeoutMs,
    connection.clientReplayBufferBytes,
    connection.clientReplayBufferBinaryBytes,
    connection.transport.flushThrottleMs,
    connection.transport.postIdleFlushDelayMs,
  ]).toEqual(Array(6).fill(0))
  // A zero replay budget replays nothing; a later channel on the connection still registers.
  expect(ClientConnection.getOrCreate('http://zero.test', createChannel() as never, options)).toBe(connection)
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
