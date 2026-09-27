import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  RECONCILE_TIMEOUT_MS,
} from '../constants.js'
import { ClientConnection } from './connection.js'

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
