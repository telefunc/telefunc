export { testAbort }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'

async function resetCleanupState() {
  await fetch(`${getServerUrl()}/api/cleanup-state/reset`, { method: 'POST' })
}
async function getCleanupState(): Promise<Record<string, string>> {
  const resp = await fetch(`${getServerUrl()}/api/cleanup-state`)
  return resp.json()
}

function testAbort() {
  test('abort: slow AI generator — onConnectionAbort fires immediately on disconnect', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const disconnectTime = Date.now()
    await page.click('#test-slow-ai-generator')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.aiDisconnected).toBe(true)
      expect(result.values).deep.equal(['token-0'])
    })
    // onConnectionAbort should fire quickly (well under 10s), not after waiting for next token
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      const abortedAt = Number(state.slowAIAbortedAt)
      expect(abortedAt).to.be.above(0)
      expect(abortedAt - disconnectTime).to.be.below(5000)
    })
  })

  test('abort: slow ReadableStream — onConnectionAbort fires immediately on cancel', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const disconnectTime = Date.now()
    await page.click('#test-slow-stream')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.streamCancelled).toBe(true)
      expect(result.chunks).deep.equal(['chunk-0'])
    })
    // onConnectionAbort should fire quickly (well under 5s), not after waiting for next chunk
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowStream).toBe('cleaned-up')
      const abortedAt = Number(state.slowStreamAbortedAt)
      expect(abortedAt).to.be.above(0)
      expect(abortedAt - disconnectTime).to.be.below(5000)
    })
  })

  test('abort: non-streaming telefunc exits early via onConnectionAbort', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    await page.click('#test-slow-normal-telefunc')
    // Wait for abort() to fire after 1.5s
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.normalStarted).toBe(true)
    })
    // Server should detect abort and the telefunc should exit early
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowNormal).toBe('cleaned-up')
      // Should have completed only a few steps (not all 20)
      const steps = Number(state.slowNormalSteps)
      expect(steps).to.be.above(0)
      expect(steps).to.be.below(15)
    })
  })
}
