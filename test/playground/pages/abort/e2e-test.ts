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
  // ── Generator abort mechanisms ──────────────────────────────────────

  test('abort: generator via abort(gen)', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-generator-abort-fn')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.method).toBe('abort(gen)')
      expect(result.values).deep.equal(['token-0'])
      // abort(gen) aborts the fetch — pending .next() rejects with cancel error
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    // Server-side onConnectionAbort fires promptly
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      expect(Number(state.slowAIAbortedAt) - t0).to.be.below(600)
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  test('abort: generator via gen.return()', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-generator-return')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.method).toBe('gen.return()')
      expect(result.values).deep.equal(['token-0'])
      // gen.return() resolves with { done: true }
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
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-generator-withContext')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.method).toBe('withContext(gen, signal)')
      expect(result.values).deep.equal(['token-0'])
      // withContext signal abort — pending .next() rejects with cancel error
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
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-stream-reader-cancel')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.method).toBe('reader.cancel()')
      expect(result.chunks).deep.equal(['chunk-0'])
      // reader.cancel() makes the pending read() resolve { done: true } cleanly
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
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    const t0 = Date.now()
    await page.click('#test-stream-withContext')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.method).toBe('withContext(stream, signal)')
      expect(result.chunks).deep.equal(['chunk-0'])
      // withContext signal abort — pending read() rejects with cancel error
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
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    await page.click('#test-slow-normal-telefunc')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      // Client should get a cancel error
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    // Server exits early
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowNormal).toBe('cleaned-up')
      const steps = Number(state.slowNormalSteps)
      expect(steps).to.be.above(0)
      expect(steps).to.be.below(15)
    })
  })

  // ── Upload abort ────────────────────────────────────────────────────

  // Single 1MB file with sleep(100) between reads on the server.
  // The sleep stretches consumption to ~1.6s; client aborts at 300ms,
  // so StreamReader.#pullChunk() returns null mid-read → disconnect error.
  test('abort: single file upload — client cancel, server disconnect error', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    await page.click('#test-upload-abort-single')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      // Client gets cancel error
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    // Server caught the disconnect error while reading the file
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortSingleError).toContain('disconnected')
    })
    // onConnectionAbort fired
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortSingle).toBe('cleaned-up')
    })
  })

  // Three 50MB files, no sleep between reads within a file.
  // File1 is consumed fully (50MB < 3s on localhost), then the server sleeps 5s.
  // Client aborts at 3s (during the post-file1 sleep). File2+file3 error because
  // StreamReader.#pullChunk() hits the closed connection. Note: file2.bytesRead
  // may be > 0 because StreamReader's internal buffer can hold leftover bytes
  // from a chunk that straddled the file1/file2 boundary.
  test('abort: multiple file upload — file1 received, file2+file3 error on disconnect', async () => {
    await page.goto(`${getServerUrl()}/abort`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await resetCleanupState()

    await page.click('#test-upload-abort-multiple')
    // Client gets cancel error
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#abort-result'))!)
      expect(result.isCancel).toBe(true)
      expect(result.error).toContain('Telefunc call cancelled')
    })
    // onConnectionAbort fired
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortMulti).toBe('cleaned-up')
      expect(state.uploadAbortMultiConnectionAbort).toBe('fired')
    })
    // Server-side: file1 received fully, file2+file3 errored
    await autoRetry(async () => {
      const state = await getCleanupState()
      const results = JSON.parse(state.uploadAbortMultiResults!)
      expect(results).to.have.length(3)
      // file1 fully received (50MB)
      expect(results[0].name).toBe('file1.txt')
      expect(results[0].bytesRead).toBe(50_000_000)
      expect(results[0].error).toBe(null)
      // file2 errored — client disconnected during sleep after file1
      expect(results[1].name).toBe('file2.txt')
      expect(results[1].error).toContain('disconnected')
      // file3 errored — client disconnected
      expect(results[2].name).toBe('file3.txt')
      expect(results[2].error).toContain('disconnected')
    })
  })
}
