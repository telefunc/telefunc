import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { CHANNEL_TRANSPORT } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { ClientConnection } from './ClientConnection.js'
import { fakeMuxChannel } from './fakeMuxChannel.fixture.js'

const PING_INTERVAL_MS = 5_000
const RECONNECT_TIMEOUT_MS = 20_000
/** How long the server may hold the page's channels after the page lost its wire: until its ping deadline notices,
 *  then `reconnectTimeout` (wire/pingDeadline.ts `reconnectWindowOf`). */
const RECOVERY_WINDOW_MS = 2 * PING_INTERVAL_MS + RECONNECT_TIMEOUT_MS

/** What a socket the page makes does: reconciles `afterMs` after it opens, or closes at once as a refused connect, or
 *  never opens nor closes. */
type Script = { reconcile: { afterMs: number } } | 'refused' | 'hangs'

/** A WebSocket that plays the next script: the page's sockets, when it made each, and whether it closed them. */
class ScriptedWebSocket {
  static OPEN = 1
  static scripts: Script[] = []
  static made: ScriptedWebSocket[] = []
  onopen: (() => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 0
  bufferedAmount = 0
  readonly at = performance.now()
  closedByPage = false

  constructor() {
    ScriptedWebSocket.made.push(this)
    const script = ScriptedWebSocket.scripts.shift() ?? 'refused'
    if (script === 'hangs') return
    if (script === 'refused') {
      queueMicrotask(() => this.drop())
      return
    }
    queueMicrotask(() => {
      this.readyState = 1
      this.onopen?.()
    })
    setTimeout(() => this.deliver(reconciled()), script.reconcile.afterMs)
  }
  send(): void {}
  close(): void {
    this.closedByPage = true
    this.readyState = 3
  }
  /** The wire goes, as when a proxy restarts. */
  drop(): void {
    this.readyState = 3
    this.onclose?.({ code: 1006 })
  }
  deliver(frame: Uint8Array<ArrayBuffer>): void {
    if (this.readyState === 1) this.onmessage?.({ data: frame.slice().buffer })
  }
}

function reconciled() {
  return encode.reconciled({
    sessionId: 'session',
    open: [{ ix: 0, lastSeq: 0 }],
    reconnectTimeout: RECONNECT_TIMEOUT_MS,
    idleTimeout: 60_000,
    pingInterval: PING_INTERVAL_MS,
    maxFrameBytes: 64 * 1024 * 1024,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['ws'],
  })
}

/** A page whose first socket reconciles `handshakeMs` after it opens, then sockets as `then` scripts them; its wire
 *  drops once it reconciled. Returns when the wire dropped, and what ended its channel. */
async function lostWire({ handshakeMs = 0, then }: { handshakeMs?: number; then: Script[] }) {
  ScriptedWebSocket.scripts = [{ reconcile: { afterMs: handshakeMs } }, ...then]
  const ended: unknown[] = []
  const connectionKey = crypto.randomUUID()
  const options = { transports: [CHANNEL_TRANSPORT.WS], fetchImpl: fetch, connectionKey }
  ClientConnection.getOrCreate(
    'http://test.local/_telefunc',
    fakeMuxChannel({ _onConnectionLost: (e) => ended.push(e) }),
    options,
  )
  await vi.advanceTimersByTimeAsync(handshakeMs + 10)
  const first = ScriptedWebSocket.made[0]
  if (!first) throw new Error('the page made no socket')
  first.drop()
  const lostAt = performance.now()
  /** Registers another channel on the same connection. */
  const register = () => ClientConnection.getOrCreate('http://test.local/_telefunc', fakeMuxChannel(), options)
  return { lostAt, ended, register }
}

/** When each socket after the first was made, from the loss. */
const madeSince = (lostAt: number) => ScriptedWebSocket.made.slice(1).map((ws) => Math.round(ws.at - lostAt))

describe("a page's reconnects", () => {
  const realWebSocket = globalThis.WebSocket
  beforeEach(() => {
    vi.useFakeTimers()
    globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    vi.restoreAllMocks()
    globalThis.WebSocket = realWebSocket
    ScriptedWebSocket.made = []
    ScriptedWebSocket.scripts = []
  })

  test('wait a random part of each delay of the backoff, the first and those at the cap too', async () => {
    const random = vi.spyOn(Math, 'random').mockReturnValue(0.25)
    const { lostAt } = await lostWire({ then: Array<Script>(6).fill('refused') })
    await vi.advanceTimersByTimeAsync(10_000)
    // A quarter of 500, 1,000, 2,000, 4,000, then of the 5,000 cap, each after the last refusal.
    expect(madeSince(lostAt).slice(0, 7)).toEqual([125, 375, 875, 1875, 3125, 4375, 5625])
    expect(random).toHaveBeenCalled()
  })

  test('start again from the first delay once a wire reconciled', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
    const { lostAt } = await lostWire({ then: ['refused', { reconcile: { afterMs: 0 } }, 'refused'] })
    // Half of 500, then half of 1,000: the third socket reconciles.
    await vi.advanceTimersByTimeAsync(750 + 10)
    const third = ScriptedWebSocket.made[2]
    if (!third) throw new Error('no third socket')
    third.drop()
    const relostAt = performance.now()
    await vi.advanceTimersByTimeAsync(1_000)
    expect(ScriptedWebSocket.made.slice(3).map((ws) => Math.round(ws.at - relostAt))).toEqual([250, 750])
    expect(madeSince(lostAt).slice(0, 2)).toEqual([250, 750])
  })
})

describe("a page's recovery", () => {
  const realWebSocket = globalThis.WebSocket
  beforeEach(() => {
    vi.useFakeTimers()
    globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket
    vi.spyOn(Math, 'random').mockReturnValue(0.9)
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    vi.restoreAllMocks()
    globalThis.WebSocket = realWebSocket
    ScriptedWebSocket.made = []
    ScriptedWebSocket.scripts = []
  })

  test('ends its channels at the deadline the server holds them to, not before', async () => {
    const { ended } = await lostWire({ then: [] })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
    expect(ended[0]).toMatchObject({ message: 'Connection dropped' })
  })

  test('ends an attempt that never opens at that deadline', async () => {
    const { ended } = await lostWire({ then: ['hangs'] })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS + 1)
    expect(ended).toHaveLength(1)
    expect(ScriptedWebSocket.made.map((ws) => ws.closedByPage)).toEqual([false, true])
  })

  test("starts no attempt that couldn't reconcile before the deadline, as long as the last handshake took", async () => {
    const handshakeMs = 3_000
    const { lostAt, ended } = await lostWire({ handshakeMs, then: [] })
    await vi.advanceTimersByTimeAsync(RECOVERY_WINDOW_MS - 1)
    expect(ended).toEqual([])
    // 0.9 of 500, 1,000, 2,000, 4,000, then of the 5,000 cap, each after the last refusal: the next, at 29,250,
    // couldn't reconcile 3 s later before the deadline at 30,000.
    expect(madeSince(lostAt)).toEqual([450, 1350, 3150, 6750, 11250, 15750, 20250, 24750])
    await vi.advanceTimersByTimeAsync(2)
    expect(ended).toHaveLength(1)
  })

  test('a channel registered while a reconnect waits starts no attempt sooner', async () => {
    const { lostAt, register } = await lostWire({ then: [] })
    await vi.advanceTimersByTimeAsync(100)
    register()
    expect(ScriptedWebSocket.made).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(400)
    expect(madeSince(lostAt)).toEqual([450])
  })
})
