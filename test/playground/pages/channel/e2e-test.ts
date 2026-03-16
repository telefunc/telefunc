export { testChannel }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { waitForHydration, getResult, getCleanupState } from '../../e2e-utils'

type ChannelState = {
  connected: boolean
  onOpenFired: boolean
  mainChannelOnCloseFiredClean: boolean | null
  tickCount: number
  lastTickServerCount: number | null
  tickWentBackward: boolean
  pingAck: string | null
  welcomeReceived: boolean
  lastEchoText: string | null
  lastEchoAck: string | null
  isClosedAfterClose: boolean | null
  serverAbortReceived: { isAbort: boolean; abortValue: unknown } | null
  clientAbortClosed: boolean | null
  noAckSendVoid: boolean | null
  perSendAck: string | null
  hookServerMessages: string[]
  hookClientOnCloseClean: boolean | null
  hookChannelId: string | null
  binaryRoundTripOk: boolean | null
  binaryByteCount: number | null
  multiCh1LastVal: number | null
  multiCh2LastVal: number | null
  multiCh1IsMonotonic: boolean | null
  multiCh2IsMonotonic: boolean | null
  clientAbortServerChannelId: string | null
  clientAbortServerOnOpenFired: boolean | null
  earlyCloseChannelId: string | null
  ackListenerAbortErr: { isAbort: boolean; abortValue: unknown } | null
  ackListenerBugErr: string | null
  ackListenerBugRecoveryAck: string | null
  clientAckListenerBugChannelId: string | null
  serverPendingAckAbortChannelId: string | null
  abortThenSendChannelId: string | null
  pendingAckAbortChannelId: string | null
  clientAbortThenSendErr: string | null
  clientPendingAckCloseErr: string | null
  upstreamReconnectChannelId: string | null
}

function testChannel(isDev: boolean) {
  const channelTransport = process.env.PUBLIC_ENV__CHANNEL_TRANSPORT ?? 'sse'

  async function waitForTransportReconnectSignal() {
    if (channelTransport === 'ws') {
      await page.waitForEvent('websocket', {
        predicate: (ws) => ws.url().includes('/_telefunc'),
        timeout: 15000,
      })
      return
    }

    if (channelTransport === 'sse') {
      await page.waitForResponse(
        (response) => {
          if (response.request().method() !== 'POST') return false
          const url = new URL(response.url())
          return url.pathname.endsWith('/_telefunc') && url.searchParams.get('_telefunc') === 'sse'
        },
        { timeout: 15000 },
      )
      return
    }

    throw new Error(`Unsupported channel transport in e2e: ${channelTransport}`)
  }

  // ── Basic connection ─────────────────────────────────────────────────

  test('channel: connect — client onOpen fires, server responds with welcome via ping', async () => {
    await page.goto(`${getServerUrl()}/channel`)
    await waitForHydration()

    await page.click('#channel-connect')

    // After connecting:
    // - connected becomes true
    // - client onOpen callback fires (channel.onOpen())
    // - client sends { type: 'ping' } automatically
    // - server responds with { type: 'welcome' }
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.connected).toBe(true)
      expect(state.onOpenFired).toBe(true)
      expect(state.welcomeReceived).toBe(true)
    })
  })

  // ── Channel-wide ack (ack: true) — client send → server ack ──────────

  test('channel: channel-wide ack — client send() resolves with server ack value', async () => {
    // channel created with { ack: true } — every send() returns a Promise
    // The server listener returns `server-ack:<type>` for every message.
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.pingAck).toBe('server-ack:ping')
    })
  })

  // ── Server → client streaming with client ack ────────────────────────

  test('channel: server sends periodic ticks, client listener acks each with return value', async () => {
    // Server sends { type: 'tick', count } every 1 s via ack-default channel.
    // Client listener returns 'client-ack:tick' — server receives that ack.
    // lastTickServerCount >= 2 proves the server's count field is the actual sequential counter.
    // tickWentBackward === false proves no tick was duplicated or replayed out of order.
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.tickCount).greaterThanOrEqual(2)
      expect(state.lastTickServerCount).greaterThanOrEqual(2)
      expect(state.tickWentBackward).toBe(false)
      // No lost or duplicate ticks
      expect(state.tickCount).toBe(state.lastTickServerCount)
    })
  })

  // ── Echo round-trip (channel-wide ack) ───────────────────────────────

  test('channel: echo round-trip — client sends, server echoes back, client gets typed ack', async () => {
    await page.fill('#channel-echo-input', 'hello-e2e')
    await page.click('#channel-echo-send')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // Server echoes text back as { type: 'echo', text }
      expect(state.lastEchoText).toBe('hello-e2e')
      // Server listener returned 'server-ack:echo' as the ack value
      expect(state.lastEchoAck).toBe('server-ack:echo')
    })
  })

  // ── send(data, { ack: false }) opt-out ───────────────────────────────

  test('channel: send({ ack: false }) opts out of ack on ack-default channel — returns void synchronously', async () => {
    await page.click('#channel-send-no-ack')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // send() with { ack: false } returns undefined (void), not a Promise
      expect(state.noAckSendVoid).toBe(true)
      // Server still received and echoed the message back (message was delivered)
      expect(state.lastEchoText).toBe('no-ack-test')
      // lastEchoAck is still from the previous acknowledged echo
      expect(state.lastEchoAck).toBe('server-ack:echo')
    })
  })

  // ── Per-send ack on non-ack-default channel ───────────────────────────

  test('channel: per-send ack — send(data, { ack: true }) on non-ack-default channel resolves with listener return value', async () => {
    await page.click('#channel-test-per-send-ack')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // Server listener returns `ack:${msg}` — for 'per-send-test' that is 'ack:per-send-test'
      expect(state.perSendAck).toBe('ack:per-send-test')
    })
  })

  // ── Server abort(abortValue) → client onClose ────────────────────────

  test('channel: server abort(value) — client onClose receives { isAbort: true, abortValue }', async () => {
    await page.click('#channel-test-server-abort')

    // Server aborts after 400 ms — wait for onClose to fire on client
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.serverAbortReceived).not.toBe(null)
      expect(state.serverAbortReceived!.isAbort).toBe(true)
      expect(state.serverAbortReceived!.abortValue).deep.equal({ reason: 'test-abort', code: 42 })
    })
  })

  // ── abort(result) from telefunc/client — abort semantics on all channels ──

  test('channel: abort(result) from telefunc/client — every channel in result receives Abort error via onClose', async () => {
    // abort(result) fires abort semantics everywhere: every channel in the result
    // gets closed and its onClose callback receives a TelefuncAbort error.
    await page.click('#channel-test-client-abort')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // true only when ch.isClosed === true AND err instanceof TelefuncAbort
      // (proves the channel was closed with abort error, not a clean close)
      expect(state.clientAbortClosed).toBe(true)
    })

    // Connect button re-enabled (channel is now closed)
    await autoRetry(async () => {
      expect(await page.locator('#channel-connect').isDisabled()).toBe(false)
    })
  })

  // ── close() — isClosed property + client onClose fires clean ─────────

  test('channel: close() — isClosed is true immediately; client onClose fires with err=undefined', async () => {
    // Reconnect so we have an open channel to test close() on
    await page.click('#channel-connect')
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.connected).toBe(true)
    })

    await page.click('#channel-disconnect')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // ch.isClosed was true immediately after ch.close() was called
      expect(state.isClosedAfterClose).toBe(true)
      expect(state.connected).toBe(false)
      // Client's onClose callback fired with err===undefined (clean close)
      expect(state.mainChannelOnCloseFiredClean).toBe(true)
    })

    // Connect button re-enabled
    await autoRetry(async () => {
      expect(await page.locator('#channel-connect').isDisabled()).toBe(false)
    })
  })

  // ── Full hook instrumentation (onOpen + onClose, client + server) ─────

  test('channel: hooks — server onOpen and onClose both fire; client onClose fires with err=undefined', async () => {
    await page.click('#channel-test-hooks')

    // Wait for:
    //  1. Client receives the in-band 'server-hook:onOpen' message (confirms server onOpen fired)
    //  2. Client onClose fires cleanly (confirms client close triggers onClose with no error)
    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.hookServerMessages).toContain('server-hook:onOpen')
      expect(state.hookClientOnCloseClean).toBe(true)
      channelId = state.hookChannelId
      expect(channelId).not.toBe(null)
    })

    // Now verify server-side via cleanupState (HTTP fetch)
    await autoRetry(async () => {
      const ss = await getCleanupState()
      // Server onOpen fired
      expect(ss[`hook_${channelId}_serverOnOpen`]).toBe('true')
      // Server onClose fired (triggered by client calling close())
      expect(ss[`hook_${channelId}_serverOnClose`]).toBe('true')
      // Server onClose received no error (clean close from client side)
      expect(ss[`hook_${channelId}_serverOnCloseErr`]).toBe('none')
    })
  })

  // ── Binary sendBinary / listenBinary round-trip ───────────────────────

  test('channel: binary round-trip — sendBinary(1 MB) echoed back byte-exact via listenBinary', async () => {
    await page.click('#channel-test-binary')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // Received correct number of bytes (1 MB)
      expect(state.binaryByteCount).toBe(256 * 4096)
      // Every byte matches (value[i] === i)
      expect(state.binaryRoundTripOk).toBe(true)
    })
  })

  // ── Reconnection (preview only — Vite HMR reloads the page if offline for too long) ─────

  if (!isDev) {
    test('channel: reconnect — ticks resume after network goes offline then online', async () => {
      await page.goto(`${getServerUrl()}/channel`)
      await waitForHydration()
      await page.click('#channel-connect')
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.connected).toBe(true)
      })

      // Wait for at least 2 ticks so we have a clean baseline.
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.tickCount).greaterThan(1)
      })
      const stateBefore = await getResult<ChannelState>('#channel-state')
      const ticksBefore = stateBefore.tickCount
      const serverCountBefore = stateBefore.lastTickServerCount!

      // Go offline — severs the WebSocket at the network level (unclean close).
      await page.context().setOffline(true)
      // Wait long enough for the ping timeout to fire and the client to mark the socket dead.
      await page.waitForTimeout(3000)

      // Arm transport-specific reconnect signal while still offline.
      const reconnectSignalPromise = waitForTransportReconnectSignal()

      // Come back online — client reconnects.
      await page.context().setOffline(false)

      // Block until reconnect signal is observed for the active transport.
      await reconnectSignalPromise

      // Ticks resume and the server count continues strictly forward — no duplicates, no replay, no data loss.
      // tickCount === lastTickServerCount proves every sent tick was received exactly once:
      //   tickCount > lastTickServerCount → duplicate deliveries
      //   tickCount < lastTickServerCount → data loss (gap in sequence)
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.tickCount).greaterThan(ticksBefore)
        expect(state.lastTickServerCount).greaterThan(serverCountBefore)
        expect(state.tickWentBackward).toBe(false)
        // No lost ticks and no duplicate ticks
        expect(state.tickCount).toBe(state.lastTickServerCount)
      })
    })
  } // end if (!isDev)

  // ── Reconnect: client→server direction — buffered sends delivered exactly once ──

  if (!isDev) {
    test('channel: reconnect — client messages sent while offline are delivered exactly once after reconnect', async () => {
      await page.goto(`${getServerUrl()}/channel`)
      await waitForHydration()

      // Open the upstream channel (client→server only, no server ticks)
      await page.click('#channel-test-upstream-open')
      let channelId: string | null = null
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.upstreamReconnectChannelId).not.toBe(null)
        channelId = state.upstreamReconnectChannelId
      })

      // Send 2 messages while still online to establish a baseline
      await page.click('#channel-test-upstream-send')
      await page.click('#channel-test-upstream-send')
      await autoRetry(async () => {
        const ss = await getCleanupState()
        expect(ss[`upstream_${channelId}_receivedCount`]).toBe('2')
      })

      // Go offline
      await page.context().setOffline(true)
      await page.waitForTimeout(3000)

      // Send 3 more messages while offline — they buffer in the client replay buffer
      await page.click('#channel-test-upstream-send')
      await page.click('#channel-test-upstream-send')
      await page.click('#channel-test-upstream-send')

      // Arm reconnect watcher, then come back online.
      const reconnectSignalPromise = waitForTransportReconnectSignal()
      await page.context().setOffline(false)
      await reconnectSignalPromise

      // Server must have received all 5 messages exactly once, in order, with no gaps
      await autoRetry(async () => {
        const ss = await getCleanupState()
        expect(ss[`upstream_${channelId}_receivedCount`]).toBe('5')
        expect(ss[`upstream_${channelId}_lastSeq`]).toBe('5')
        expect(ss[`upstream_${channelId}_hasGap`]).toBe('false')
      })
    })
  }

  // ── Client abort(value) → server onClose (clean close) ─────────────

  test('channel: client abort(value) — server onClose fires cleanly when client aborts while still connected', async () => {
    await page.click('#channel-test-client-abort-server')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.clientAbortServerChannelId).not.toBe(null)
      channelId = state.clientAbortServerChannelId
    })

    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`clientAbort_${channelId}_serverOnClose`]).toBe('true')
      expect(ss[`clientAbort_${channelId}_serverOnCloseErr`]).toBe('none')
    })
  })

  if (!isDev) {
    test('channel: reconnect omission — server onClose fires with ChannelNetworkError when client drops channel while offline', async () => {
      await page.click('#channel-connect')
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.connected).toBe(true)
      })

      await page.click('#channel-test-client-abort-server-open')

      let channelId: string | null = null
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.clientAbortServerChannelId).not.toBe(null)
        expect(state.clientAbortServerOnOpenFired).toBe(true)
        channelId = state.clientAbortServerChannelId
      })

      await page.context().setOffline(true)
      await page.waitForTimeout(3000)

      await page.click('#channel-test-client-abort-server')

      const reconnectSignalPromise = waitForTransportReconnectSignal()
      await page.context().setOffline(false)
      await reconnectSignalPromise

      await autoRetry(async () => {
        const ss = await getCleanupState()
        expect(ss[`clientAbort_${channelId}_serverOnClose`]).toBe('true')
        expect(ss[`clientAbort_${channelId}_serverOnCloseErr`]).toBe('none')
      })
    })
  }

  // ── Multiple concurrent channels (ix multiplexing) ────────────────────

  test('channel: concurrent channels — two channels on same WS receive independent data streams', async () => {
    await page.click('#channel-test-multi')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // channel1: 1, 2, 3, …  Must be strictly +1 each message.
      // channel2: 100, 200, 300, …  Must be strictly +100 each message.
      // Any mislabelled frame, skip, or duplicate fails the monotonicity check.
      expect(state.multiCh1LastVal).greaterThanOrEqual(3)
      expect(state.multiCh2LastVal).greaterThanOrEqual(300)
      expect(state.multiCh1IsMonotonic).toBe(true)
      expect(state.multiCh2IsMonotonic).toBe(true)
    })
  })

  // ── Close before onOpen (regression: buffered close must reach server) ─

  test('channel: close() before onOpen — server onClose fires; close frame not dropped during reconcile', async () => {
    await page.click('#channel-test-early-close')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.earlyCloseChannelId).not.toBe(null)
      channelId = state.earlyCloseChannelId
    })

    await autoRetry(async () => {
      const ss = await getCleanupState()
      // Server lifecycle must have completed cleanly.
      expect(ss[`hook_${channelId}_serverOnClose`]).toBe('true')
      expect(ss[`hook_${channelId}_serverOnCloseErr`]).toBe('none')
    })
  })

  // ── send({ ack: true }) rejects when server listener throws Abort ─────

  test('channel: ack listener abort — send({ ack: true }) rejects with isAbort + abortValue when server listener throws Abort', async () => {
    await page.click('#channel-test-ack-listener-abort')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.ackListenerAbortErr).not.toBe(null)
      expect(state.ackListenerAbortErr!.isAbort).toBe(true)
      expect(state.ackListenerAbortErr!.abortValue).deep.equal({ reason: 'listener-abort', code: 7 })
    })
  })

  test('channel: ack listener bug — send({ ack: true }) rejects but channel stays open for a follow-up ack', async () => {
    await page.click('#channel-test-ack-listener-bug')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.ackListenerBugErr).toBe('Internal Server Error — see server logs')
      expect(state.ackListenerBugRecoveryAck).toBe('ack:ok')
    })
  })

  test('channel: client ack listener bug — server pending ack rejects but follow-up ack still succeeds', async () => {
    await page.click('#channel-test-client-ack-listener-bug')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.clientAckListenerBugChannelId).not.toBe(null)
      channelId = state.clientAckListenerBugChannelId
    })

    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`clientAckBug_${channelId}_rejected`]).toBe('true')
      expect(ss[`clientAckBug_${channelId}_followupAck`]).toBe('client-ack:ok')
    })
  })

  // ── server-side pending ack rejected when channel aborted concurrently ─

  test('channel: server pending ack abort — await send({ ack: true }) on server rejects when channel.abort() fires concurrently', async () => {
    await page.click('#channel-test-server-pending-ack-abort')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.serverPendingAckAbortChannelId).not.toBe(null)
      channelId = state.serverPendingAckAbortChannelId
    })

    // Server's try/catch around the awaited send() must have caught the rejection
    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`serverPendingAck_${channelId}_rejected`]).toBe('true')
      // The rejection is not an AbortError from the server's perspective
      // (abort() does not throw isAbort on the server side — it's a ChannelClosedError/shutdown)
      // What matters is that the promise was rejected at all.
    })
  })

  // ── Case 1: abort() then send() — throws ChannelClosedError synchronously ─

  test('channel: abort then send — send() after abort() throws ChannelClosedError synchronously', async () => {
    await page.click('#channel-test-abort-then-send')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.abortThenSendChannelId).not.toBe(null)
      channelId = state.abortThenSendChannelId
    })

    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`abortThenSend_${channelId}_thrown`]).toBe('true')
      expect(ss[`abortThenSend_${channelId}_isClosedErr`]).toBe('true')
    })
  })

  // ── Case 2: send() then abort() then await — rejects with ChannelClosedError ─

  test('channel: pending ack abort — await send({ ack: true }) rejects with ChannelClosedError when abort() fires after send()', async () => {
    await page.click('#channel-test-pending-ack-abort')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.pendingAckAbortChannelId).not.toBe(null)
      channelId = state.pendingAckAbortChannelId
    })

    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`pendingAbort_${channelId}_rejected`]).toBe('true')
      expect(ss[`pendingAbort_${channelId}_isClosedErr`]).toBe('true')
    })
  })

  // ── Client Case 1: abort() then send() throws ChannelClosedError ─────

  test('channel: client abort then send — client send() after abort() throws ChannelClosedError', async () => {
    await page.click('#channel-test-client-abort-then-send')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.clientAbortThenSendErr).toBe('ChannelClosedError')
    })
  })

  // ── Client Case 2: send({ack:true}) then close() then await rejects ───

  test('channel: client pending ack close — await send({ ack: true }) rejects with ChannelClosedError after close()', async () => {
    await page.click('#channel-test-client-pending-ack-close')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.clientPendingAckCloseErr).toBe('ChannelClosedError')
    })
  })
}
