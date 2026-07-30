import { afterEach, describe, expect, test, vi } from 'vitest'

import { CHANNEL_RECONNECT_INITIAL_DELAY_MS, CHANNEL_TRANSPORT, RECONCILE_TIMEOUT_MS } from '../constants.js'
import { ClientConnection } from './connection.js'
import { ClientBroadcast } from './channel.js'
import { ACK_STATUS, type AckResultStatus } from '../shared-ws.js'

afterEach(() => vi.restoreAllMocks())

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

function publishThatSettlesWith(status: AckResultStatus, binary: boolean) {
  const broadcast = Object.assign(Object.create(ClientBroadcast.prototype), {
    _isClosed: false,
    _pendingAcks: new Map(),
    _inflightAcks: 0,
    _closeWaiters: [],
    _connection: {
      sendPublishAckReq: (_channel: unknown, _data: unknown, register: (seq: number) => void) => register(1),
      sendPublishBinaryAckReq: (_channel: unknown, _data: unknown, register: (seq: number) => void) => register(1),
    },
  }) as ClientBroadcast
  const publishing = binary ? broadcast.publishBinary(new Uint8Array([1])) : broadcast.publish('message')
  ;(
    broadcast as unknown as {
      _onPeerAckRes(seq: number, text: string, status: AckResultStatus): void
    }
  )._onPeerAckRes(1, status === ACK_STATUS.ABORT ? JSON.stringify('expected') : 'unexpected publish bug', status)
  return publishing
}

describe.each([
  ['text', false],
  ['binary', true],
] as const)('ClientBroadcast %s publish errors', (_name, binary) => {
  test('reports a plain Error through the client bug pipeline', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await expect(publishThatSettlesWith(ACK_STATUS.ERROR, binary)).rejects.toThrow('unexpected publish bug')
    expect(report).toHaveBeenCalledOnce()
    expect(report.mock.calls[0]?.[0]).toBe('[telefunc:channel-error]')
    expect(report.mock.calls[0]?.[1]).toMatchObject({ message: 'unexpected publish bug' })
  })

  test('keeps an expected Abort quiet', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await expect(publishThatSettlesWith(ACK_STATUS.ABORT, binary)).rejects.toMatchObject({ abortValue: 'expected' })
    expect(report).not.toHaveBeenCalled()
  })
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
