export { testRxjs }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { waitForHydration, getResult, resetCleanupState, getCleanupState } from '../../e2e-utils'

function testRxjs(inDocker = false) {
  // ── Observable: server → client ──────────────────────────────────────

  test('rxjs: observable — server emits 5 ticks then completes', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-obs-ticks')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.done).toBe(true)
      expect(result.values).deep.equal(['tick-0', 'tick-1', 'tick-2', 'tick-3', 'tick-4'])
    })
  })

  test('rxjs: observable — synchronous emit + complete', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-obs-complete')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.done).toBe(true)
      expect(result.values).deep.equal(['a', 'b'])
    })
  })

  test('rxjs: observable — error propagates to client subscriber', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-obs-error')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.values).deep.equal(['ok'])
      // Real error is stripped at the wire — client sees a generic message.
      expect(result.error).toContain('Internal error')
    })
  })

  test('rxjs: observable — close() stops after 2 values', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-obs-close')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.closedAfter).toBe(2)
      expect(result.values.length).toBe(2)
    })
  })

  // ── Subject: bidirectional ───────────────────────────────────────────

  test('rxjs: subject — receives server pushes and client can send', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-subject-bidir')

    // Should receive server pushes and the client-sent value
    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.received.some((v: string) => v.startsWith('server-'))).toBe(true)
      expect(result.received).toContain('client-hello')
    })
  })

  test('rxjs: subject — close() from client triggers server cleanup', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-subject-close')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.closed).toBe(true)
      expect(result.received.length).greaterThanOrEqual(1)
    })

    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.subjectCleanedUp).toBe('true')
    })
  })

  test('rxjs: subject echo — server subscribes and echoes back', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-subject-echo')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.received).toContain('ping')
      expect(result.received).toContain('echo:ping')
    })
  })

  test('rxjs: subject — server-initiated complete propagates to client', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-subject-server-complete')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.completed).toBe(true)
      expect(result.received).toContain('before-complete')
    })
  })

  // ── Shared subject: multicast ────────────────────────────────────────
  // Skipped in docker: the server-side `Subject` is a plain JS object whose subscriber
  // list lives in one process. Round-robin puts client A and B on different instances,
  // so A's `subscribe` and B's `next` operate on different Subject instances. Cross-
  // instance multicast would require Redis pub/sub bridging the Subject — out of scope.
  if (!inDocker) {
    test('rxjs: shared subject — client A sends, client B receives', async () => {
      await page.goto(`${getServerUrl()}/rxjs`)
      await waitForHydration()

      await page.click('#test-shared-subject')

      await autoRetry(async () => {
        const result = await getResult('#rxjs-result')
        // s1 sent 'from-s1' — s2 should have received it
        expect(result.received2).toContain('from-s1')
        // s2 sent 'from-s2' — s1 should have received it
        expect(result.received1).toContain('from-s2')
      })
    })
  }

  // ── Observable: client → server ──────────────────────────────────────

  test('rxjs: observable client→server — server receives all values', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-obs-from-client')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.serverReceived).deep.equal(['a', 'b', 'c'])
    })

    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.clientObsCompleted).toBe('true')
    })
  })

  // ── Subject: multiple local subscriptions ────────────────────────────

  test('rxjs: subject — multiple local subscribers both receive all values', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-subject-multi-sub')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.sub1).deep.equal([1, 2, 3])
      expect(result.sub2).deep.equal([1, 2, 3])
      expect(result.completed1).toBe(true)
      expect(result.completed2).toBe(true)
    })
  })

  // ── Server-initiated error propagation ─────────────────────────────
  test('rxjs: observable — server calls subscriber.error(), client subscription errors', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-obs-server-error')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.values).deep.equal(['before-error'])
      // Real error is stripped over the wire and replaced with a generic
      // message — actual error is logged server-side only
      expect(result.error).toContain('Internal error')
    })
  })

  test('rxjs: subject — server calls subject.error(), client subscription errors', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-subject-server-error')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.received).toContain('before-error')
      expect(result.error).toContain('Internal error')
    })
  })

  test('rxjs: subject arg — server without error handler survives client-errored Subject', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-subject-arg-no-handler')

    // Server received the value before the client errored the Subject
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.subjectArgReceived).toBe('hello')
    })

    // Server didn't crash — navigate (closes channel → onClose fires) and
    // confirm a follow-up telefunction call succeeds.
    await page.reload()
    await waitForHydration()
    await page.click('#test-obs-complete')
    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.done).toBe(true)
    })

    // After navigation, onClose fired on the old channel
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.subjectArgClosed).toBe('true')
    })
  })

  // Skipped in docker — same shared-Subject cross-instance limitation.
  if (!inDocker) {
    test('rxjs: shared subject — one client errors, shared Subject dies for all', async () => {
      await page.goto(`${getServerUrl()}/rxjs`)
      await waitForHydration()

      await page.click('#test-shared-error-one')

      await autoRetry(async () => {
        const result = await getResult('#rxjs-result')
        // A errored — transparent: kills the shared server Subject
        expect(result.a_errored).toBe(true)
        // B should also error — shared Subject is dead
        expect(result.b_errored).toBe(true)
      })
    })
  }

  // ── Shield data-flow validation ───────────────────────────────────────
  //
  // Shield generation walks `[TELEFUNC_SHIELDS]` on `Subject<T>` / `Observable<T>`,
  // producing a `next` validator that fires on the server side wherever the wire
  // receives `msg.v` from the client. Invalid values are silently dropped — the
  // server's subscription never sees them.
  test('rxjs: shield — Subject arg / Subject return / Observable arg reject invalid next values', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()

    await page.click('#test-shield-all')

    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      // A: Subject as arg — client sent 'hello', 12345 (invalid), 'world'.
      expect(result.subjectArgReceived).deep.equal(['hello', 'world'])
      // B: Subject as return — client sent 'foo', 999 (invalid), 'bar'.
      expect(result.subjectReturnReceived).deep.equal(['foo', 'bar'])
      // C: Observable as arg — client emitted 1, 'bad' (invalid), 2.
      expect(result.observableArgReceived).deep.equal([1, 2])
    })
  })

  test('rxjs: subject — server subscribes without error handler, no crash on error', async () => {
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-subject-no-handler')

    // Client should receive the server-sent error gracefully
    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.error).toContain('Internal error')
    })

    // Server should have received the client-sent value (via live subscription)
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.noHandlerReceived).toBe('hello-from-client')
    })

    // Confirm server is still alive — navigation closes the channel (triggering
    // onClose cleanup) and a follow-up telefunction call confirms the process
    // didn't crash.
    await page.goto(`${getServerUrl()}/rxjs`)
    await waitForHydration()
    await page.click('#test-obs-complete')
    await autoRetry(async () => {
      const result = await getResult('#rxjs-result')
      expect(result.done).toBe(true)
    })

    // After navigation, onClose should have fired on the old channel
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.noHandlerClosed).toBe('true')
    })
  })
}
