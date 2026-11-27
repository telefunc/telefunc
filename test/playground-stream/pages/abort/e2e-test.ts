export { testAbort }

import { connect } from 'node:net'
import { once } from 'node:events'
import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import {
  resetCleanupState,
  getCleanupState,
  navigate,
  getResult,
  serverRuntime,
  sleep,
  scriptsRequestedDuring,
} from '../../e2e-utils'

function testAbort(isDev: boolean) {
  // ── Generator abort mechanisms ──────────────────────────────────────

  test('abort: generator via abort(gen)', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

    await page.click('#test-generator-abort-fn')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('abort(gen)')
      expect(result.values).deep.equal(['token-0'])
      expect(result.isAbort).toBe(true)
      expect(result.error).toContain('Aborted telefunction call')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      expect(state.slowAIClosedWaiting).toBe('true')
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  test('abort: generator via gen.return()', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

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
      expect(state.slowAIClosedWaiting).toBe('true')
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  test('abort: generator via withContext(gen, signal)', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

    await page.click('#test-generator-withContext')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('withContext(gen, signal)')
      expect(result.values).deep.equal(['token-0'])
      expect(result.isAbort).toBe(true)
      expect(result.error).toContain('Aborted telefunction call')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowAI).toBe('cleaned-up')
      expect(state.slowAIClosedWaiting).toBe('true')
      expect(state.slowAIFinallyRan).toBe('true')
    })
  })

  // ── Stream abort mechanisms ─────────────────────────────────────────

  test('abort: stream via reader.cancel()', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

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
      expect(state.slowStreamClosedWaiting).toBe('true')
      expect(state.slowStreamCancelled).toBe('true')
    })
  })

  test('abort: stream via withContext(stream, signal)', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

    await page.click('#test-stream-withContext')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.method).toBe('withContext(stream, signal)')
      expect(result.chunks).deep.equal(['chunk-0'])
      expect(result.isAbort).toBe(true)
      expect(result.error).toContain('Aborted telefunction call')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowStream).toBe('cleaned-up')
      expect(state.slowStreamClosedWaiting).toBe('true')
      expect(state.slowStreamCancelled).toBe('true')
    })
  })

  // ── Non-streaming abort ─────────────────────────────────────────────

  test('abort: non-streaming telefunc — client call throws Abort', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

    const scripts = await scriptsRequestedDuring(async () => {
      await page.click('#test-slow-normal-telefunc')
      // Abort once the server runs the telefunc.
      await autoRetry(async () => {
        const state = await getCleanupState()
        expect(state.slowNormal).toBe('running')
      })
      await page.click('#test-slow-normal-abort')
      await autoRetry(async () => {
        const result = await getResult('#abort-result')
        expect(result.isAbort).toBe(true)
        expect(result.error).toContain('Aborted telefunction call')
      })
    })
    // The built page has the code of a call that carries plain values only.
    if (!isDev) expect(scripts).deep.equal([])
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.slowNormal).toBe('cleaned-up')
      const steps = Number(state.slowNormalSteps)
      expect(steps).to.be.above(0)
      expect(steps).to.be.below(15)
    })
  })

  // ── Upload abort ────────────────────────────────────────────────────

  // Through Telefunc's adapter for Bun.serve or Deno.serve, which says how its server shows the client left. Node's
  // playground route hands Telefunc srvx's `Request`, which carries no evidence of its transport: a client that leaves
  // there is printed (serve/node.requestBody.spec.ts holds the rows of Node's `serve({ req, res })`).
  if (serverRuntime() !== 'node')
    test('abort: a text call whose client leaves in the middle of its body prints nothing on the server', async () => {
      await resetCleanupState()
      const { hostname, port } = new URL(getServerUrl())
      const socket = connect(Number(port), hostname)
      await once(socket, 'connect')
      socket.write(
        'POST /_telefunc HTTP/1.1\r\nHost: localhost\r\nx-telefunc-request: txt\r\nContent-Type: text/plain\r\nContent-Length: 1000\r\n\r\n{"file":',
      )
      // The server has the request and waits for the rest of its body.
      await sleep(500)
      socket.destroy()
      // An error printed fails the test (test-e2e reads the server's stderr); `onBug()` hears the call that failed.
      await autoRetry(async () => {
        const state = await getCleanupState()
        expect(state['bug:The client disconnected before the end of the request body']).toBe('reported')
      })
    })

  // 1MB file with sleep(100) between reads — the client aborts while the server reads it
  // Node only: Bun and Deno hand the server the whole 1 MB the browser sent before it aborted, and only then report the disconnect.
  if (serverRuntime() === 'node')
    test('abort: single file upload — client cancel, server disconnect error', async () => {
      await navigate(`${getServerUrl()}/abort`)
      await resetCleanupState()

      await page.click('#test-upload-abort-single')
      // Abort once the server reads the file.
      await autoRetry(async () => {
        const state = await getCleanupState()
        expect(state.uploadAbortSingle).toBe('running')
      })
      await page.click('#test-upload-abort-single-abort')
      await autoRetry(async () => {
        const result = await getResult('#abort-result')
        expect(result.isAbort).toBe(true)
        expect(result.error).toContain('Aborted telefunction call')
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

  // Three 50MB files — file1 consumed fully, the client aborts during the
  // post-file1 sleep, file2+file3 error on disconnect
  test('abort: multiple file upload — file1 received, file2+file3 error on disconnect', async () => {
    await navigate(`${getServerUrl()}/abort`)
    await resetCleanupState()

    await page.click('#test-upload-abort-multiple')
    // Abort once the server has read file1.
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortMultiFilesRead).toBe('1')
    })
    await page.click('#test-upload-abort-multiple-abort')
    await autoRetry(async () => {
      const result = await getResult('#abort-result')
      expect(result.isAbort).toBe(true)
      expect(result.error).toContain('Aborted telefunction call')
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.uploadAbortMulti).toBe('cleaned-up')
      expect(state.uploadAbortMultiConnectionClose).toBe('fired')
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
