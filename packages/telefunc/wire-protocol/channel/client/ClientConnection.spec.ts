import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { encode } from '../../wire/frame.js'
import { ClientConnection } from './ClientConnection.js'

/** Minimal `MuxChannel` — registering one is enough to make the connection open a wire. */
function createChannel(id = crypto.randomUUID()) {
  return {
    id,
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onEnded() {},
    _attach: {
      state: () => ({}),
      receiveCredit: { onProbeAnswered() {}, acknowledge() {}, answered() {} },
      capWindows() {},
      onBatched() {},
      onUpgradeJoined() {},
    },
  }
}

/** A `fetch` whose SSE downstream body emits only what a test hands it and never closes: a silently
 *  stalled stream (no error, no FIN), so the RECONCILED frame that clears `reconciling`
 *  never arrives. Counts how many times the downstream (`Accept: text/event-stream`) is
 *  opened, i.e. how many connect attempts ran. */
function createStalledTransport() {
  /** Each downstream opened, which delivers what it is handed until the page cancels it. */
  const downstreams: { deliver(chunk: Uint8Array): void }[] = []
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const headers = (init.headers ?? {}) as Record<string, string>
    let cancelled = false
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        if (headers.Accept !== 'text/event-stream') return
        downstreams.push({
          deliver: (chunk) => {
            if (!cancelled) controller.enqueue(chunk)
          },
        })
      },
      cancel: () => {
        cancelled = true
      },
    })
    return new Response(body, { status: 200 })
  }) as unknown as typeof fetch
  return { fetchImpl, downstreams, getSseDownstreamOpens: () => downstreams.length }
}

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
    // connection wedges here forever: pings keep flowing but `onPongTimeout` is
    // suppressed while reconciling, so the dead wire is never noticed. The watchdog must
    // instead time out the reconcile and reconnect.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(getSseDownstreamOpens()).toBeGreaterThanOrEqual(2)
  })

  test('ends the channels at the reconnect timeout when wires open and none reconciles', async () => {
    vi.useFakeTimers()
    const { fetchImpl } = createStalledTransport()
    const ended: unknown[] = []
    const channel = { ...createChannel(), _onEnded: (err: unknown) => ended.push(err) }

    ClientConnection.getOrCreate('http://test.local/_telefunc', channel as never, {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // Each wire opens, its RECONCILE deadline drops it, and the page reconnects.
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_TIMEOUT_MS)
    expect(ended).toEqual([])
    // The timeout runs from the first loss, one deadline in; the first loss past it, a delay and a deadline on, ends them.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_MAX_DELAY_MS + RECONCILE_TIMEOUT_MS)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toBeInstanceOf(NetworkError)
  })

  test('drops a wire that never reconciles while the wire it replaced still delivers', async () => {
    vi.useFakeTimers()
    const { fetchImpl, downstreams } = createStalledTransport()

    ClientConnection.getOrCreate('http://test.local/_telefunc', createChannel() as never, {
      transports: [CHANNEL_TRANSPORT.SSE],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // The first wire's RECONCILE deadline drops it, and the page opens a second.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(downstreams).toHaveLength(2)

    // The first wire's stream still delivers a PONG each second, which says nothing of the second wire.
    const pong = new TextEncoder().encode(`data: ${uint8ArrayToBase64url(encode.pong())}\n\n`)
    const trickle = setInterval(() => downstreams[0]!.deliver(pong), 1000)
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + 2 * CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    clearInterval(trickle)
    expect(downstreams).toHaveLength(3)
  })
})
