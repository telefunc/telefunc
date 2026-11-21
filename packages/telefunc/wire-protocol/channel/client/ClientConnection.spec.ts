import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
} from '../../constants.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { ChannelClosedError } from '../channelErrors.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { encode } from '../../wire/frame.js'
import { ClientConnection } from './ClientConnection.js'
import { fakeMuxChannel } from './fakeMuxChannel.fixture.js'

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
    return new Response(body, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
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

  test('ends the channels at the reconnect timeout when wires open and none reconciles', async () => {
    vi.useFakeTimers()
    const { fetchImpl } = createStalledTransport()
    const ended: unknown[] = []
    const channel = fakeMuxChannel({ _onConnectionLost: (err) => ended.push(err) })

    ClientConnection.getOrCreate('http://test.local/_telefunc', channel, {
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

    ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), {
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

/** A WebSocket that opens and delivers only what a test hands it: no RECONCILED ever comes. */
class SilentWebSocket {
  static OPEN = 1
  static opened: SilentWebSocket[] = []
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 1
  bufferedAmount = 0

  constructor() {
    SilentWebSocket.opened.push(this)
    queueMicrotask(() => this.onopen?.())
  }
  send(): void {}
  close(): void {
    this.readyState = 3
  }
  deliver(frame: Uint8Array<ArrayBuffer>): void {
    this.onmessage?.({ data: frame.slice().buffer })
  }
}

describe('WebSocket reconcile watchdog', () => {
  const realWebSocket = globalThis.WebSocket
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    globalThis.WebSocket = realWebSocket
    SilentWebSocket.opened = []
  })

  test('drops a wire that never reconciles while the socket it replaced still delivers', async () => {
    vi.useFakeTimers()
    globalThis.WebSocket = SilentWebSocket as unknown as typeof WebSocket

    ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), {
      transports: [CHANNEL_TRANSPORT.WS],
      fetchImpl: createStalledTransport().fetchImpl,
      connectionKey: crypto.randomUUID(),
    })

    // The first wire's RECONCILE deadline drops it, and the page opens a second.
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    expect(SilentWebSocket.opened).toHaveLength(2)

    // The first socket still delivers a PONG each second, which says nothing of the second wire.
    const trickle = setInterval(() => SilentWebSocket.opened[0]!.deliver(encode.pong()), 1000)
    await vi.advanceTimersByTimeAsync(RECONCILE_TIMEOUT_MS + 2 * CHANNEL_RECONNECT_INITIAL_DELAY_MS + 500)
    clearInterval(trickle)
    expect(SilentWebSocket.opened).toHaveLength(3)
  })
})

describe('a wire the server refused for a frame it could not read', () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test("its event stream's REJECTED event ends the channels with the refusal, and the page opens no other wire", async () => {
    vi.useFakeTimers()
    const { fetchImpl, downstreams } = createStalledTransport()
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onEnded: (err) => ended.push(err) }),
      {
        transports: [CHANNEL_TRANSPORT.SSE],
        fetchImpl,
        connectionKey: crypto.randomUUID(),
      },
    )
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_INITIAL_DELAY_MS)
    expect(downstreams).toHaveLength(1)

    downstreams.at(0)?.deliver(new TextEncoder().encode(`data: ${uint8ArrayToBase64url(encode.rejected())}\n\n`))
    await vi.advanceTimersByTimeAsync(CHANNEL_RECONNECT_TIMEOUT_MS)

    expect(ended).toHaveLength(1)
    expect(ended[0]).toBeInstanceOf(ChannelClosedError)
    expect(ended[0]).toHaveProperty('message', expect.stringContaining('the server refused a frame it could not read'))
    expect(downstreams).toHaveLength(1)
  })
})

describe("a first connect's deadline", () => {
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  test("ends a first connect at the deadline from its start, with a later attempt's refusal after a first open got no answer", async () => {
    vi.useFakeTimers()
    let opens = 0
    const fetchImpl: typeof fetch = async (_input, init) => {
      if (new Headers(init?.headers).get('Accept') !== 'text/event-stream') return new Response(null, { status: 200 })
      if (++opens === 1) throw new TypeError('fetch failed')
      return new Response('<!doctype html>', { status: 502, headers: { 'Content-Type': 'text/html' } })
    }
    const ended: unknown[] = []
    ClientConnection.getOrCreate(
      'http://test.local/_telefunc',
      fakeMuxChannel({ _onConnectionLost: (err) => ended.push(err) }),
      { transports: [CHANNEL_TRANSPORT.SSE], fetchImpl, connectionKey: crypto.randomUUID() },
    )
    // The defaults' window over SSE: 2.5 pings of 5 s, then 60 s.
    await vi.advanceTimersByTimeAsync(2.5 * 5_000 + CHANNEL_RECONNECT_TIMEOUT_MS + 1)
    expect(opens).toBeGreaterThan(2)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toBeInstanceOf(NetworkError)
    expect(ended[0]).toHaveProperty('message', 'Server rejected SSE connection with 502 text/html')
  })
})
