import { afterEach, describe, expect, test, vi } from 'vitest'

import { CHANNEL_RECONNECT_INITIAL_DELAY_MS, CHANNEL_TRANSPORT, RECONCILE_TIMEOUT_MS } from '../../constants.js'
import { ClientConnection } from './ClientConnection.js'
import { fakeMuxChannel } from './fakeMuxChannel.fixture.js'

/** A `fetch` whose SSE downstream body emits only what a test hands it and never closes: a silently
 *  stalled stream (no error, no FIN), so the RECONCILED frame that clears `reconciling`
 *  never arrives. Counts how many times the downstream (`Accept: text/event-stream`) is
 *  opened, i.e. how many connect attempts ran. */
function createStalledTransport() {
  /** Each downstream opened, which delivers what it is handed until the page cancels it. */
  const downstreams: { deliver(chunk: Uint8Array): void }[] = []
  const fetchImpl: typeof fetch = async (_input, init) => {
    const accept = new Headers(init?.headers).get('Accept')
    let cancelled = false
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        if (accept !== 'text/event-stream') return
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
    return new Response(body, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
  }
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

    ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), {
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
})
