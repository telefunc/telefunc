export { testChannel }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Request, Response, WebSocket } from 'playwright-chromium'
import { navigate, getResult, getCleanupState, restartProxy, stopProxy, startProxy } from '../../e2e-utils'

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
  clientPendingAckCloseReconnectChannelId: string | null
  clientPendingAckCloseReconnectOnOpenFired: boolean | null
  clientPendingAckCloseReconnectErr: string | null
  serverPendingAckCloseReconnectChannelId: string | null
  serverPendingAckCloseReconnectOnOpenFired: boolean | null
  serverPendingAckCloseReconnectClientOnCloseClean: boolean | null
  upstreamReconnectChannelId: string | null
  noListenerAckServerChannelId: string | null
  noListenerAckServerErr: string | null
  noListenerAckClientErr: string | null
  shieldClientSendNoAckReceived: string[] | null
  shieldClientSendNoAckInvalidReceived: string[] | null
  shieldClientSendNoAckInvalidThrew: boolean | null
  shieldClientSendAckValid: number | null
  shieldClientSendAckInvalidError: string | null
  shieldServerAckValid: { ok: boolean; value?: string; error?: string } | null
  shieldServerAckInvalid: { ok: boolean; value?: string; error?: string } | null
  selfRemovingListener: { pageSeen: string[]; serverSeen: string[] } | null
  unserializableAnswer: { channelId: string; pageSend: string; pageFollowUp: string; isClosed: boolean } | null
  oversize: { channelId: string; thrown: string[]; followUp: string; isClosed: boolean } | null
  heldBinary: number[][] | null
  unattachedFlood: Record<'text' | 'binary', Record<string, number>> | null
  durationLimits: { withContext: string; close: string; ack: string } | null
  endedValuesReady: boolean
  endedValuesCollected: boolean[] | null
  slowLinkReceived: number | null
  unawaitedBurst: { rejected: Record<string, number>; received: number; isClosed: boolean } | null
  smallPushReceived: number[] | null
  manyListenersMs: { page: number; server: number | null } | null
}

function testChannel(isDev: boolean, inDocker = false) {
  const channelTransports = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS)
  const channelTransport = channelTransports[channelTransports.length - 1]!

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
    await navigate(`${getServerUrl()}/channel`)

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

  test('channel: send({ ack: false }) opts out of ack on ack-default channel — resolves to void', async () => {
    await page.click('#channel-send-no-ack')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      // send() with { ack: false } returns Promise<void> — no ack value
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
      // true only when channel.isClosed === true AND err instanceof TelefuncAbort
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
      // channel.isClosed was true immediately after channel.close() was called
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
      await navigate(`${getServerUrl()}/channel`)
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

      const reconnectSignalPromise = waitForTransportReconnectSignal()
      if (inDocker) {
        // Docker setup uses streamRequest; browser offline mode doesn't sever in-flight
        // chunked POST bodies, so the only reliable disconnect is killing Caddy at the
        // TCP layer. `grace_period 1s` in the Caddyfile bounds the shutdown window.
        restartProxy()
      } else {
        // dev/preview: single-instance, no proxy to restart — setOffline still works
        // because batch-only SSE without streamRequest gets blocked by Chromium's offline
        // mode, and WS also gets severed.
        await page.context().setOffline(true)
        await page.waitForTimeout(3000)
        await page.context().setOffline(false)
      }

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
      await navigate(`${getServerUrl()}/channel`)

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
      if (inDocker) stopProxy()
      else await page.context().setOffline(true)
      await page.waitForTimeout(3000)

      // Send 3 more messages while offline — they buffer in the client replay buffer
      await page.click('#channel-test-upstream-send')
      await page.click('#channel-test-upstream-send')
      await page.click('#channel-test-upstream-send')

      // Arm reconnect watcher, then come back online.
      const reconnectSignalPromise = waitForTransportReconnectSignal()
      if (inDocker) startProxy()
      else await page.context().setOffline(false)
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

  // ── Client abort(value) → server onClose (Abort) ──────────────────

  test('channel: client abort(value) — server onClose receives Abort with the value when client aborts while still connected', async () => {
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
      expect(ss[`clientAbort_${channelId}_serverOnCloseErr`]).toBe('abort:{"reason":"client-abort"}')
    })
  })

  // Skipped in docker: the trigger endpoint needs Caddy alive (which we stop to sever the
  // browser), and the server-side close-store is per-instance globalThis — won't reach the
  // owning playground container in a cluster regardless. Leave for the single-instance paths.
  if (!isDev && !inDocker) {
    test('channel: reconnect + close — client onClose stays clean when server closes with pending ack while client is offline', async () => {
      await navigate(`${getServerUrl()}/channel`)
      await page.click('#channel-connect')
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.connected).toBe(true)
      })

      await page.click('#channel-server-pending-ack-close-reconnect-open')

      let channelId: string | null = null
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.serverPendingAckCloseReconnectChannelId).not.toBe(null)
        expect(state.serverPendingAckCloseReconnectOnOpenFired).toBe(true)
        channelId = state.serverPendingAckCloseReconnectChannelId
      })

      await page.context().setOffline(true)
      await page.waitForTimeout(3000)

      // Trigger from the test-runner process directly — browser is offline so it can't do this
      const triggerRes = await fetch(`${getServerUrl()}/api/server-close-trigger?channelId=${channelId}`, {
        method: 'POST',
      })
      expect(triggerRes.ok).toBe(true)

      const reconnectSignalPromise = waitForTransportReconnectSignal()
      await page.context().setOffline(false)
      await reconnectSignalPromise

      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.serverPendingAckCloseReconnectClientOnCloseClean).toBe(true)
        const ss = await getCleanupState()
        expect(ss[`serverClose_${channelId}_ackResult`]).toBe('ack:offline-close')
        expect(ss[`serverClose_${channelId}_closeResult`]).toBe('0')
        expect(ss[`serverClose_${channelId}_serverOnCloseErr`]).toBe('none')
      })
    })
  }

  if (!isDev) {
    test('channel: reconnect + close — server onClose stays clean when client closes with pending ack while offline', async () => {
      await navigate(`${getServerUrl()}/channel`)
      await page.click('#channel-connect')
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.connected).toBe(true)
      })

      await page.click('#channel-test-client-pending-ack-close-reconnect-open')

      let channelId: string | null = null
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.clientPendingAckCloseReconnectChannelId).not.toBe(null)
        expect(state.clientPendingAckCloseReconnectOnOpenFired).toBe(true)
        channelId = state.clientPendingAckCloseReconnectChannelId
      })

      if (inDocker) stopProxy()
      else await page.context().setOffline(true)
      await page.waitForTimeout(3000)

      await page.click('#channel-test-client-pending-ack-close-reconnect')

      const reconnectSignalPromise = waitForTransportReconnectSignal()
      if (inDocker) startProxy()
      else await page.context().setOffline(false)
      await reconnectSignalPromise

      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.clientPendingAckCloseReconnectErr).toBe('none')
        const ss = await getCleanupState()
        expect(ss[`clientClose_${channelId}_serverOnClose`]).toBe('true')
        expect(ss[`clientClose_${channelId}_serverOnCloseErr`]).toBe('none')
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

  // ── Case 2: send() then abort() then await — rejects with abort semantics ─

  test('channel: pending ack abort — await send({ ack: true }) rejects with abort semantics when abort() fires after send()', async () => {
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
      expect(ss[`pendingAbort_${channelId}_isAbortErr`]).toBe('true')
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

  // ── Client Case 2: send({ack:true}) then close() then await rejects on timeout ───

  test('channel: client pending ack close — await send({ ack: true }) rejects with ChannelClosedError after close timeout', async () => {
    await page.click('#channel-test-client-pending-ack-close')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.clientPendingAckCloseErr).toBe('ChannelClosedError')
    })
  })

  // ── No listener ack rejection ───────────────────────────────────────

  test('channel: no listener ack (server→client) — server send({ ack: true }) rejects when client has no listener', async () => {
    await page.click('#channel-test-no-listener-ack-server')

    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.noListenerAckServerChannelId).not.toBe(null)
      channelId = state.noListenerAckServerChannelId
    })

    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`noListenerAck_server_${channelId}_rejected`]).toBe('true')
      expect(ss[`noListenerAck_server_${channelId}_errMsg`]).toContain('No listener')
    })
  })

  test('channel: no listener ack (client→server) — client send({ ack: true }) rejects when server has no listener', async () => {
    await page.click('#channel-test-no-listener-ack-client')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.noListenerAckClientErr).toContain('No listener')
    })
  })

  // ── Shield validation ─────────────────────────────────────────────────

  test('channel: shield covers all client→server and server→client validation paths', async () => {
    await page.click('#channel-test-shield')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')

      // A1: valid client sends without ack reach the server listener.
      expect(state.shieldClientSendNoAckReceived).to.deep.equal(['hello', 'world'])

      // A2: invalid client sends without ack are silently dropped — listener set doesn't grow,
      // and the client's send does not throw.
      expect(state.shieldClientSendNoAckInvalidReceived).to.deep.equal(['hello', 'world'])
      expect(state.shieldClientSendNoAckInvalidThrew).toBe(false)

      // B1: valid client send with ack resolves with the listener's return (length of 'hi!' = 3).
      expect(state.shieldClientSendAckValid).toBe(3)

      // B2: invalid client send with ack rejects with a shield validation error.
      expect(state.shieldClientSendAckInvalidError).not.toBe('NO_ERROR')
      expect(state.shieldClientSendAckInvalidError).toBeTruthy()

      // C1: server-sent message acked by client's string return → server's send resolves.
      expect(state.shieldServerAckValid?.ok).toBe(true)
      expect(state.shieldServerAckValid?.value).toBe('got-100')

      // C2: server-sent message acked by client's wrong-typed return → server's send rejects.
      expect(state.shieldServerAckInvalid?.ok).toBe(false)
      expect(state.shieldServerAckInvalid?.error).toBeTruthy()
    })
  })

  test("channel: a listener that stops listening itself doesn't make the next one miss a message, on either end", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-self-removing-listener')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.selfRemovingListener?.pageSeen).deep.equal(['once:one', 'other:one', 'other:two'])
      expect(state.selfRemovingListener?.serverSeen).deep.equal(['once:one', 'other:one', 'other:two'])
    })
  })

  test("channel: an ack answer that can't be serialized rejects the other end's send, and the channel stays open", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unserializable-answer')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      const result = state.unserializableAnswer!
      expect(result.pageSend).match(/^error:/)
      expect(result.pageFollowUp).toBe('ok:ack:ok')
      expect(result.isClosed).toBe(false)
      const ss = await getCleanupState()
      expect(ss[`unserializable_${result.channelId}_serverSend`]).match(/^error:/)
      expect(ss[`unserializable_${result.channelId}_serverFollowUp`]).toBe('ok:client-ack:ok')
    })
  })

  test('channel: a send over 64 MiB throws at once and an answer that large rejects the send it answers, the channel staying open', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-oversize')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      const result = state.oversize!
      expect(result.thrown).to.have.length(2)
      expect(result.thrown).not.to.include('none')
      expect(result.followUp).toBe('ack:ok')
      expect(result.isClosed).toBe(false)
      const ss = await getCleanupState()
      expect(ss[`oversize_${result.channelId}_serverSend`]).match(/^error:/)
      expect(ss[`oversize_${result.channelId}_serverFollowUp`]).toBe('ok:client-ack:ok')
    })
  })

  test('channel: binary the server sends before the page attaches arrives as sent, though the server reuses the buffer', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-held-binary')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.heldBinary).deep.equal([
        [1, 1, 1],
        [2, 2, 2],
        [3, 3, 3],
        [4, 4, 4],
      ])
    })
  })

  test('channel: a flood of empty sends to a channel no page attached to rejects the oldest with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unattached-flood')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.unattachedFlood?.text.ChannelOverflowError).greaterThan(0)
      expect(state.unattachedFlood?.binary.ChannelOverflowError).greaterThan(0)
    })
  })

  test('channel: a per-call idleTimeout or a close() timeout longer than a timer waits is refused, and the channel stays open', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-duration-limits')

    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.durationLimits?.withContext).not.toBe('none')
      expect(state.durationLimits?.close).not.toBe('none')
      expect(state.durationLimits?.ack).toBe('ack:still-open')
    })
  })

  test('channel: the page lets go of a channel and a broadcast it closed and a generator it read to the end', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-ended-values')
    await autoRetry(async () => {
      expect((await getResult<ChannelState>('#channel-state')).endedValuesReady).toBe(true)
    })
    const cdp = await newCDPSession()
    await autoRetry(async () => {
      await cdp.send('HeapProfiler.collectGarbage')
      await page.click('#channel-test-ended-values-check')
      expect((await getResult<ChannelState>('#channel-state')).endedValuesCollected).deep.equal([true, true, true])
    })
    await cdp.detach()
  })

  test('channel: a 1 MB message to the page, 8 s at 1 Mbit/s, arrives over the connection it left on', async () => {
    await navigate(`${getServerUrl()}/channel`)
    const connections = countConnections()
    const link = await throttleLink(125_000)
    try {
      await page.click('#channel-test-slow-link')
      await autoRetry(
        async () => {
          expect((await getResult<ChannelState>('#channel-state')).slowLinkReceived).toBe(1_000_000)
        },
        { timeout: 40_000 },
      )
    } finally {
      await link.restore()
    }
    expect(connections.stop()).toBe(1)
  })

  test('channel: a burst of 32 MiB the server sends without awaiting all reaches the page', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unawaited-burst-32')
    await autoRetry(async () => {
      expect((await getResult<ChannelState>('#channel-state')).unawaitedBurst).deep.equal({
        rejected: {},
        received: 32,
        isClosed: false,
      })
    })
  })

  test('channel: a burst of 100 MiB the server sends without awaiting rejects what is past what the page may be behind with ChannelOverflowError, and the page gets the rest', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unawaited-burst-100')
    await autoRetry(async () => {
      const burst = (await getResult<ChannelState>('#channel-state')).unawaitedBurst!
      expect(Object.keys(burst.rejected)).deep.equal(['ChannelOverflowError'])
      expect(burst.received + burst.rejected.ChannelOverflowError!).toBe(100)
      expect(burst.isClosed).toBe(false)
    })
  })

  test('channel: 100,000 listeners added to a channel and removed take under a second, on the page and on the server', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-many-listeners')
    const waitFor = async (side: 'page' | 'server') => {
      let ms: number | null | undefined
      await autoRetry(
        async () => {
          ms = (await getResult<ChannelState>('#channel-state')).manyListenersMs?.[side]
          expect(ms).not.toBe(null)
          expect(ms).not.toBe(undefined)
        },
        { timeout: 50_000 },
      )
      return ms!
    }
    expect(await waitFor('page')).lessThan(1000)
    expect(await waitFor('server')).lessThan(1000)
  })

  if (channelTransport === 'ws') {
    test('channel: a push of 60 small messages the server sends without awaiting reaches the page in a few WebSocket messages', async () => {
      let messages = 0
      const onWebSocket = (ws: WebSocket) => {
        if (ws.url().includes('/_telefunc')) ws.on('framereceived', () => messages++)
      }
      page.on('websocket', onWebSocket)
      try {
        await navigate(`${getServerUrl()}/channel`)
        await page.click('#channel-test-small-push')
        await autoRetry(async () => {
          expect((await getResult<ChannelState>('#channel-state')).smallPushReceived).deep.equal(
            Array.from({ length: 60 }, (_, seq) => seq),
          )
        })
      } finally {
        page.off('websocket', onWebSocket)
      }
      expect(messages).lessThan(10)
    })
  }

  if (channelTransport === 'sse') {
    test("channel: Chrome reports none of the page's SSE POSTs canceled", async () => {
      await navigate(`${getServerUrl()}/channel`)
      const canceled: string[] = []
      const onRequestFailed = (request: Request) => {
        const isSsePost = request.method() === 'POST' && request.url().includes('_telefunc=sse')
        if (isSsePost && request.failure()?.errorText === 'net::ERR_ABORTED') canceled.push(request.url())
      }
      page.on('requestfailed', onRequestFailed)
      await page.click('#channel-connect')
      await page.fill('#channel-echo-input', 'sse-post')
      await page.click('#channel-echo-send')
      await autoRetry(async () => {
        const state = await getResult<ChannelState>('#channel-state')
        expect(state.pingAck).toBe('server-ack:ping')
        expect(state.lastEchoAck).toBe('server-ack:echo')
      })
      page.off('requestfailed', onRequestFailed)
      expect(canceled).deep.equal([])
    })
  }
}

/** Counts the WebSockets and SSE streams the page opens to Telefunc from now until `stop()`. */
function countConnections() {
  let opened = 0
  const onWebSocket = (ws: WebSocket) => {
    if (ws.url().includes('/_telefunc')) opened++
  }
  const onResponse = (response: Response) => {
    if (response.headers()['content-type']?.startsWith('text/event-stream')) opened++
  }
  page.on('websocket', onWebSocket)
  page.on('response', onResponse)
  return {
    stop() {
      page.off('websocket', onWebSocket)
      page.off('response', onResponse)
      return opened
    },
  }
}

function newCDPSession() {
  // test-e2e's `page` is a proxy, and a CDP session needs the page itself.
  return page.context().newCDPSession(page.mainFrame().page())
}

/** Slows the page's link to `bytesPerSecond` each way, as Chrome's network throttling does, WebSockets included. */
async function throttleLink(bytesPerSecond: number) {
  const cdp = await newCDPSession()
  await cdp.send('Network.enable')
  const emulate = (throughput: number) =>
    cdp.send('Network.emulateNetworkConditions', {
      offline: false,
      latency: 0,
      downloadThroughput: throughput,
      uploadThroughput: throughput,
    })
  await emulate(bytesPerSecond)
  return {
    async restore() {
      await emulate(-1)
      await cdp.detach()
    },
  }
}

function parseChannelTransports(value: string | undefined): Array<'sse' | 'ws'> {
  const parsed: unknown = JSON.parse(value ?? '["sse"]')
  if (
    !Array.isArray(parsed) ||
    parsed.length === 0 ||
    !parsed.every((transport) => transport === 'sse' || transport === 'ws')
  ) {
    throw new Error(`Invalid PUBLIC_ENV__CHANNEL_TRANSPORTS: ${value ?? '(unset)'}`)
  }
  return parsed
}
