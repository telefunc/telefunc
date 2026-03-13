export { testClose }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { resetCleanupState, getCleanupState, waitForHydration, getResult } from '../../e2e-utils'

function testClose() {
  // ── Targeted: generator ──────────────────────────────────────────────

  test('close: generator — close(gen) terminates cleanly; done=true, no error, finally block runs', async () => {
    await page.goto(`${getServerUrl()}/close`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-close-gen')

    await autoRetry(async () => {
      const result = await getResult('#close-result')
      expect(result.method).toBe('close(gen)')
      expect(result.values).deep.equal(['token-0'])
      expect(result.nextDone).toBe(true)
      expect(result.error).toBe(null)
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.closeGenFinallyRan).toBe('true')
    })
  })

  // ── Targeted: stream ─────────────────────────────────────────────────

  test('close: stream — close(stream) terminates cleanly; cancel callback fires on server', async () => {
    await page.goto(`${getServerUrl()}/close`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-close-stream')

    await autoRetry(async () => {
      const result = await getResult('#close-result')
      expect(result.method).toBe('close(stream)')
      expect(result.chunks).deep.equal(['chunk-0'])
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.closeStreamCancelled).toBe('true')
    })
  })

  // ── Targeted: channel ────────────────────────────────────────────────

  test('close: channel — close(channel) fires clean onClose on client; server sees no-peer TTL timeout', async () => {
    await page.goto(`${getServerUrl()}/close`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-close-channel')

    await autoRetry(async () => {
      const result = await getResult('#close-result')
      expect(result.method).toBe('close(channel)')
      expect(result.channelCloseClean).toBe(true)
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      // Server channel never got a peer (WS not yet connected when close() was called) → TTL timeout
      expect(state.closeChannel_onCloseErr).toBe(
        'Channel timed out: no peer connected within TTL after response was sent',
      )
    })
  })

  // ── Targeted: fn ─────────────────────────────────────────────────────

  test('close: fn — close(fn) closes backing channel; call after close throws ChannelClosedError', async () => {
    await page.goto(`${getServerUrl()}/close`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-close-fn')

    await autoRetry(async () => {
      const result = await getResult('#close-result')
      expect(result.method).toBe('close(fn)')
      expect(result.errorAfterClose).toBe('Channel is closed')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      // retFn() was called before close
      expect(state.closeFn_retFnCalled).toBe('true')
    })
  })

  // ── Mixed close: { generator, stream, channel, fn } ──────────────────

  test('close: mixed { generator, stream, channel, fn } — close(result) terminates all value types cleanly; passed and returned functions work', async () => {
    await page.goto(`${getServerUrl()}/close`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-close-mixed')

    // ── Client-side: all values closed cleanly ──
    await autoRetry(async () => {
      const result = await getResult('#close-result')
      expect(result.method).toBe('close(mixed)')

      // Passed callback was invoked by the server
      expect(result.passedFnMessages).deep.equal(['hello-from-server'])

      // Generator: first token received, then clean termination (done=true, no error)
      expect(result.genValues).deep.equal(['token-0'])
      expect(result.genNextDone).toBe(true)
      expect(result.genError).toBe(null)

      // Stream: first chunk received, then clean termination (done=true)
      expect(result.chunks).deep.equal(['chunk-0'])
      expect(result.streamReadDone).toBe(true)

      // Channel: closed cleanly with no error
      expect(result.channelCloseClean).toBe(true)

      // Calling the returned fn after close must throw
      expect(result.retFnAfterCloseError).toBe('Channel is closed')
    })

    // ── Server-side: cleanup state reflects clean close of all values ──
    await autoRetry(async () => {
      const state = await getCleanupState()

      // Passed callback was called on server
      expect(state.closeMixed_cbCalled).toBe('true')

      // Generator finally block ran (gen.return() propagated to server)
      expect(state.closeMixed_genFinallyRan).toBe('true')

      // Stream cancel callback ran (stream.cancel() propagated to server)
      expect(state.closeMixed_streamCancelled).toBe('true')

      // Channel onClose: closed cleanly (no error)
      expect(state.closeMixed_channel_onCloseErr).toBe('none')

      // Returned fn was called by client before close
      expect(state.closeMixed_retFnCalled).toBe('true')
    })
  })
}
