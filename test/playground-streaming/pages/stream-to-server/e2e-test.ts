export { testStreamToServer }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { waitForHydration, getResult } from '../../e2e-utils'

function testStreamToServer() {
  test('stream-to-server: echo', async () => {
    await page.goto(`${getServerUrl()}/stream-to-server`)
    await waitForHydration()
    await page.click('#test-echo')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.chunks).deep.equal(['hello', ' ', 'world'])
    })
  })

  test('stream-to-server: collect bytes/chunks', async () => {
    await page.click('#test-collect')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.chunkCount).toBe(4)
      expect(result.totalBytes).toBe(10) // 'aaa' + 'bb' + 'cccc' + 'd' = 3+2+4+1
    })
  })

  test('stream-to-server: relay via async generator', async () => {
    await page.click('#test-relay')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.values).deep.equal(['alpha', 'beta', 'gamma'])
    })
  })

  test('stream-to-server: passthrough (checksum)', async () => {
    await page.click('#test-passthrough')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.match).toBe(true)
      expect(result.sent).deep.equal(['one', 'two', 'three'])
      expect(result.received).deep.equal(['one', 'two', 'three'])
    })
  })

  test('stream-to-server: slow consumer', async () => {
    await page.click('#test-slow-consumer')
    await autoRetry(
      async () => {
        const result = await getResult('#stream-result')
        expect(result.chunks).deep.equal(['fast-0', 'fast-1', 'fast-2', 'fast-3', 'fast-4'])
      },
      { timeout: 5000 },
    )
  })

  // 50 MB pull-based stream. Server stalls 3s after first chunk.
  // If backpressure works, the client should not push remaining data during the stall.
  test('stream-to-server: 50 MB transfer with backpressure', async () => {
    await page.click('#test-backpressure')
    await autoRetry(
      async () => {
        const result = await getResult('#stream-result')
        expect(result.done).toBe(true)
        expect(result.totalMB).toBe(50)
        expect(result.chunkCount).toBe(50)
      },
      { timeout: 60_000 },
    )
  })

  // Client sends ping-0..ping-5 at 500ms intervals, server echoes each back.
  // UI updates progressively — each echo should arrive ~500ms apart.
  test('stream-to-server: live loopback (progressive)', async () => {
    await page.click('#test-live-loopback')
    await autoRetry(
      async () => {
        const result = await getResult('#stream-result')
        expect(result.done).toBe(true)
        expect(result.log.length).toBe(6)
        expect(result.log[0].value).toBe('ping-0')
        expect(result.log[5].value).toBe('ping-5')
        // Last echo should arrive ~3s after first — progressive, not batched.
        const totalTime = result.log[5].at - result.log[0].at
        expect(totalTime).greaterThan(2000)
      },
      { timeout: 10000 },
    )
  })

  test('stream-to-server: server abort mid-stream', async () => {
    await page.click('#test-abort-mid-stream')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.error).toBe(true)
      expect(result.isAbort).toBe(true)
      expect(result.abortValue).deep.equal({ reason: 'enough', chunksRead: 3 })
    })
  })

  test('stream-to-server: server abort mid-relay (stream→generator)', async () => {
    await page.click('#test-abort-mid-relay')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.error).toBe(true)
      expect(result.isAbort).toBe(true)
      expect(result.abortValue).deep.equal({ reason: 'mid-relay-abort', chunksRelayed: 2 })
      expect(result.values).deep.equal(['x', 'y'])
    })
  })

  test('stream-to-server: client abort(res)', async () => {
    await page.click('#test-client-abort')
    await autoRetry(async () => {
      const result = await getResult('#stream-result')
      expect(result.aborted || result.cancelled).toBe(true)
    })
  })
}
