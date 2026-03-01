export { testAbort }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { resetCleanupState, getCleanupState, waitForHydration, getResult } from '../../e2e-utils'

function testAbort() {
  // ── Generator abort mechanisms ──────────────────────────────────────

  test('abort: generator via abort(gen)', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-generator-abort-fn')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('abort(gen)')
      expect(result.values).deep.equal(['token-0'])
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      expect(Number(state.slowAIAbortedAt) - t0).to.be.below(600)
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  test('abort: generator via gen.return()', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-generator-return')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('gen.return()')
      expect(result.values).deep.equal(['token-0'])
      expect(result.nextDone).toBe(true)
      expect(result.error).toBe(null)
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      expect(Number(state.slowAIAbortedAt) - t0).to.be.below(600)
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  test('abort: generator via withContext(gen, signal)', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-generator-withContext')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('withContext(gen, signal)')
      expect(result.values).deep.equal(['token-0'])
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      expect(Number(state.slowAIAbortedAt) - t0).to.be.below(600)
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  // ── Stream abort mechanisms ─────────────────────────────────────────

  test('abort: stream via reader.cancel()', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-stream-reader-cancel')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('reader.cancel()')
      expect(result.chunks).deep.equal(['chunk-0'])
      expect(result.readDone).toBe(true)
      expect(result.error).toBe(null)
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowStream).toBe('cleaned-up')
      expect(Number(state.slowStreamAbortedAt) - t0).to.be.below(600)
      expect(state.slowStreamCancelled).toBe('true')
    })
  })

  test('abort: stream via withContext(stream, signal)', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-stream-withContext')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('withContext(stream, signal)')
      expect(result.chunks).deep.equal(['chunk-0'])
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowStream).toBe('cleaned-up')
      expect(Number(state.slowStreamAbortedAt) - t0).to.be.below(600)
      expect(state.slowStreamCancelled).toBe('true')
    })
  })

  // ── Non-streaming abort ─────────────────────────────────────────────

  test('abort: non-streaming telefunc — client gets cancel error', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-slow-normal-telefunc')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowNormal).toBe('cleaned-up')
      const steps = Number(state.slowNormalSteps)
      expect(steps).to.be.above(0)
      expect(steps).to.be.below(15)
    })
  })

  // ── Upload abort ────────────────────────────────────────────────────

  // 1MB file with sleep(100) between reads — client aborts at 300ms
  test('abort: single file upload — client cancel, server disconnect error', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-upload-abort-single')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortSingleError).toContain('disconnected')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortSingle).toBe('cleaned-up')
    })
  })

  // Three 50MB files — file1 consumed fully, client aborts at 3s during
  // post-file1 sleep, file2+file3 error on disconnect
  test('abort: multiple file upload — file1 received, file2+file3 error on disconnect', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await waitForHydration()
    await resetCleanupState()

    await page.click('#test-upload-abort-multiple')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortMulti).toBe('cleaned-up')
      expect(state.uploadAbortMultiConnectionAbort).toBe('fired')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      const results = JSON.parse(state.uploadAbortMultiResults!)
      expect(results).to.have.length(3)
      expect(results[0].name).toBe('file1.txt')
      expect(results[0].bytesRead).toBe(50_000_000)
      expect(results[0].error).toBe(null)
      expect(results[1].name).toBe('file2.txt')
      expect(results[1].error).toContain('disconnected')
      expect(results[2].name).toBe('file3.txt')
      expect(results[2].error).toContain('disconnected')
    })
  })
}
