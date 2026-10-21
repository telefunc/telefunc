export { testStreamToServer }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState } from '../../e2e-utils'

function testStreamToServer(isDev: boolean, inDocker = false) {
  test('stream-to-server: echo', async () => {
    await navigate(`${getServerUrl()}/stream-to-server`)
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

  test('stream-to-server: a chunk larger than a channel message arrives whole', async () => {
    await navigate(`${getServerUrl()}/stream-to-server`)
    await page.click('#test-one-large-chunk')
    await autoRetry(async () => {
      expect(await getResult('#stream-result')).deep.equal({ totalBytes: 65 * 1024 * 1024 })
    })
  })

  test('stream-to-server: an upload whose source fails after a chunk errors on the server instead of completing', async () => {
    await navigate(`${getServerUrl()}/stream-to-server`)
    await page.click('#test-ingest-failing-source')
    const { key } = await getResult('#stream-result')
    await autoRetry(async () => {
      expect((await getCleanupState())[`ingest_${key}`]).match(/^errored:/)
    })
  })

  const channelTransports: string[] = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!)
  if (process.env.PUBLIC_ENV__STREAM_TRANSPORT === 'channel' && channelTransports.at(-1) === 'ws') {
    test('stream-to-server: an upload whose page closes mid-upload errors on the server instead of completing short', async () => {
      const otherContext = await page.context().browser()!.newContext()
      const other = await otherContext.newPage()
      await navigate(`${getServerUrl()}/stream-to-server`, undefined, other)
      await other.click('#test-ingest-endless')
      const { key } = JSON.parse((await other.textContent('#stream-result'))!)
      await autoRetry(async () => {
        expect((await getCleanupState())[`ingest_${key}`]).match(/^reading:[1-9]/)
      })
      await other.close()
      await autoRetry(async () => {
        expect((await getCleanupState())[`ingest_${key}`]).match(/^errored:/)
      })
      await otherContext.close()
    })
  }

  // Preview only: Vite HMR reloads an offline page.
  if (!isDev && !inDocker) {
    test('stream-to-server: an upload that ends while the page is offline longer than a close waits arrives whole once the page is back', async () => {
      await navigate(`${getServerUrl()}/stream-to-server`)
      await page.click('#test-ingest-until-finish')
      const { key } = await getResult('#stream-result')
      await autoRetry(async () => {
        expect((await getCleanupState())[`ingest_${key}`]).toBe('reading:1')
      })

      await page.context().setOffline(true)
      await page.click('#test-ingest-finish')
      // Away longer than close()'s 5 s default.
      await page.waitForTimeout(7000)
      await page.context().setOffline(false)

      await autoRetry(async () => {
        expect((await getCleanupState())[`ingest_${key}`]).toBe('completed:2')
      })
    })
  }
}
