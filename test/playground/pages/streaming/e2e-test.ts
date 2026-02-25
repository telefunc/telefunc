export { testStreaming }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'

function testStreaming() {
  test('streaming: ReadableStream return', async () => {
    await page.goto(`${getServerUrl()}/streaming`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })

    await page.click('#test-readable-stream')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.content).toBe('hello stream')
      expect(result.chunkCount).greaterThanOrEqual(1)
    })
  })

  test('streaming: AsyncGenerator<number> return', async () => {
    await page.click('#test-async-generator')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result).deep.equal({ values: [1, 2, 3, 4, 5], count: 5 })
    })
  })

  test('streaming: generator with metadata', async () => {
    await page.click('#test-generator-with-meta')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.messages).deep.equal(['hello', 'world'])
      expect(result.timestamp).toBe(1234567890)
      expect(result.tags).deep.equal(['a', 'b'])
    })
  })

  test('streaming: empty generator', async () => {
    await page.click('#test-empty-generator')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result).deep.equal({ values: [], count: 0 })
    })
  })

  test('streaming: delayed ReadableStream arrives over time', async () => {
    await page.click('#test-delayed-stream')
    // Should see intermediate state (not done yet)
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.chunks.length).greaterThanOrEqual(1)
    })
    // Wait for completion
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.done).toBe(true)
      expect(result.chunks).deep.equal(['chunk1', 'chunk2', 'chunk3'])
    })
  })

  test('streaming: delayed AsyncGenerator yields values over time', async () => {
    await page.click('#test-delayed-generator')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.done).toBe(true)
      expect(result.values).deep.equal(['alpha', 'beta', 'gamma', 'delta'])
    })
  })

  test('streaming: delayed generator with metadata', async () => {
    await page.click('#test-delayed-generator-meta')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.done).toBe(true)
      expect(result.label).toBe('countdown')
      expect(result.values).deep.equal([3, 2, 1, 0])
    })
  })

  test('streaming: ReadableStream with metadata in object', async () => {
    await page.click('#test-stream-with-meta')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.chunks).deep.equal(['foo', 'bar', 'baz'])
      expect(result.count).toBe(3)
    })
  })

  test('streaming: two generators should error', async () => {
    await page.click('#test-two-generators')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.error).toBe(true)
      expect(result.message).toContain('Internal Server Error')
    })
  })

  test('streaming: stream + generator should error', async () => {
    await page.click('#test-stream-and-generator')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.error).toBe(true)
      expect(result.message).toContain('Internal Server Error')
    })
  })

  test('streaming: generator Abort() mid-stream', async () => {
    await page.goto(`${getServerUrl()}/streaming`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })

    await page.click('#test-generator-abort-midstream')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.error).toBe(true)
      expect(result.isAbort).toBe(true)
      expect(result.values).deep.equal(['before-abort'])
    })
  })

  test('streaming: generator Abort() with value mid-stream', async () => {
    await page.click('#test-generator-abort-with-value')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.error).toBe(true)
      expect(result.isAbort).toBe(true)
      expect(result.abortValue).deep.equal({ reason: 'not-allowed', code: 403 })
      expect(result.values).deep.equal(['before-abort'])
    })
  })

  test('streaming: generator bug mid-stream', async () => {
    await page.click('#test-generator-bug-midstream')
    await autoRetry(async () => {
      const result = JSON.parse((await page.textContent('#streaming-result'))!)
      expect(result.error).toBe(true)
      expect(result.isBug).toBe(true)
      expect(result.message).toContain('Internal Server Error')
      expect(result.values).deep.equal(['before-bug'])
    })
  })
}
