export { testStreaming }

import { page, test, expect, autoRetry, getServerUrl, skip } from '@brillout/test-e2e'
import { resetCleanupState, getCleanupState, waitForHydration, getResult } from '../../e2e-utils'

function testStreaming() {
  test('streaming: ReadableStream return', async () => {
    await page.goto(`${getServerUrl()}/streaming`)
    await waitForHydration()

    await page.click('#test-readable-stream')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.content).toBe('hello stream')
      expect(result.chunkCount).greaterThanOrEqual(1)
    })
  })

  test('streaming: AsyncGenerator<number> return', async () => {
    await page.click('#test-async-generator')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result).deep.equal({ values: [1, 2, 3, 4, 5], count: 5 })
    })
  })

  test('streaming: generator with metadata', async () => {
    await page.click('#test-generator-with-meta')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.messages).deep.equal(['hello', 'world'])
      expect(result.timestamp).toBe(1234567890)
      expect(result.tags).deep.equal(['a', 'b'])
    })
  })

  test('streaming: empty generator', async () => {
    await page.click('#test-empty-generator')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result).deep.equal({ values: [], count: 0 })
    })
  })

  test('streaming: delayed ReadableStream arrives over time', async () => {
    await page.click('#test-delayed-stream')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.chunks.length).greaterThanOrEqual(1)
    })
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.done).toBe(true)
      expect(result.chunks).deep.equal(['chunk1', 'chunk2', 'chunk3'])
    })
  })

  test('streaming: delayed AsyncGenerator yields values over time', async () => {
    await page.click('#test-delayed-generator')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.done).toBe(true)
      expect(result.values).deep.equal(['alpha', 'beta', 'gamma', 'delta'])
    })
  })

  test('streaming: delayed generator with metadata', async () => {
    await page.click('#test-delayed-generator-meta')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.done).toBe(true)
      expect(result.label).toBe('countdown')
      expect(result.values).deep.equal([3, 2, 1, 0])
    })
  })

  test('streaming: ReadableStream with metadata in object', async () => {
    await page.click('#test-stream-with-meta')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.chunks).deep.equal(['foo', 'bar', 'baz'])
      expect(result.count).toBe(3)
    })
  })

  test('streaming: two generators multiplexed', async () => {
    await resetCleanupState()
    await page.click('#test-two-generators')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.first).deep.equal([1, 2, 3])
      expect(result.second).deep.equal([10, 20, 30])
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.twoGeneratorsClosed).not.toBe('')
    })
  })

  test('streaming: stream + generator multiplexed', async () => {
    await resetCleanupState()
    await page.click('#test-stream-and-generator')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.chunks).deep.equal(['hi'])
      expect(result.values).deep.equal([1])
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.streamAndGeneratorClosed).not.toBe('')
    })
  })

  test('streaming: multiple promises multiplexed', async () => {
    await resetCleanupState()
    await page.click('#test-multiple-promises')
    // Fast promise should resolve first
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.fast).toBe('quick')
      expect(result.updates).deep.equal(['fast'])
      expect(result.slow).toBe(undefined)
    })
    // Then slow resolves
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.fast).toBe('quick')
      expect(result.slow).toBe('delayed')
      expect(result.label).toBe('promises')
      expect(result.updates).deep.equal(['fast', 'slow'])
    })
    // onConnectionClose fires after all streams complete
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.multiplePromisesClosed).not.toBe('')
    })
  })

  test('streaming: stream + promise deadlock — promise unblocks when stream is consumed', async () => {
    // Start: makes the call but does NOT consume the stream yet
    await page.click('#test-stream-promise-deadlock-start')

    // Promise must stay pending while the stream is unconsumed
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.promisePending).toBe(true)
      expect(result.streamDone).toBe(false)
    })

    // Consume: drains the 2 MB stream, unblocking the promise frame
    await page.click('#test-stream-promise-deadlock-consume')

    // After the stream is consumed the pipeline uncorks and the promise resolves
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.promiseResolved).toBe(true)
      expect(result.streamDone).toBe(true)
      expect(result.byteCount).toBe(32 * 64 * 1024) // 2 MB
    })
  })

  test('streaming: cancel generator + promise — chunks arrive incrementally', async () => {
    await resetCleanupState()

    await page.click('#test-mixed-endless-cancel')

    // First gen value should render before all 3 are received
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.genValues.length).greaterThanOrEqual(1)
      expect(result.steps).toContain('gen-0')
    })

    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.steps).toContain('gen-cancelled')
    })

    const result = await getResult('#streaming-result')
    expect(result.genValues).lengthOf(3)
    expect(result.steps).toContain('gen-0')
    expect(result.steps).toContain('gen-1')
    expect(result.steps).toContain('gen-2')
    expect(result.steps).toContain('promise-resolved')
    expect(result.steps).toContain('gen-cancelled')

    // After all consumers done, onConnectionClose should fire
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.mixedEndless).toBe('cleaned-up')
      expect(state.mixedEndlessClosed).not.toBe('')
    })
  })

  test('streaming: asymmetric generators — fast done while slow still running', async () => {
    await page.click('#test-asymmetric-generators')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.fastValues).deep.equal(['fast-done'])
      expect(result.slowValues).deep.equal(['slow-0', 'slow-1', 'slow-2'])
      expect(result.fastDoneBeforeSlowFinished).toBe(true)
    })
  })

  test('streaming: abort(res) on multiplexed return object', async () => {
    await resetCleanupState()
    await page.click('#test-abort-multiplexed')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.isAbort).toBe(true)
      expect(result.error).toContain('Aborted telefunction call')
      expect(result.slowErr?.isAbort).toBe(true)
      expect(result.slowErr?.error).toContain('Aborted telefunction call')
      expect(result.genValues).lengthOf(2)
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.mixedEndless).toBe('cleaned-up')
      expect(state.mixedEndlessClosed).not.toBe('')
    })
  })

  test('streaming: generator Abort() mid-stream', async () => {
    await page.click('#test-generator-abort-midstream')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.error).toBe(true)
      expect(result.isAbort).toBe(true)
      expect(result.values).deep.equal(['before-abort'])
    })
  })

  test('streaming: generator Abort() with value mid-stream', async () => {
    await page.click('#test-generator-abort-with-value')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.error).toBe(true)
      expect(result.isAbort).toBe(true)
      expect(result.abortValue).deep.equal({ reason: 'not-allowed', code: 403 })
      expect(result.values).deep.equal(['before-abort'])
    })
  })

  test('streaming: generator bug mid-stream', async () => {
    await page.click('#test-generator-bug-midstream')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.error).toBe(true)
      expect(result.isBug).toBe(true)
      expect(result.message).toContain('Internal Server Error')
      expect(result.values).deep.equal(['before-bug'])
    })
  })

  test('streaming: Abort in one producer aborts all multiplexed streaming values', async () => {
    await page.click('#test-abort-one-of-many-streaming-values')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.abortingValues).deep.equal(['abort-0'])
      expect(result.otherValues.length).greaterThan(0)
      expect(result.streamChunks.length).greaterThan(0)
      expect(result.abortingErr?.isAbort).toBe(true)
      expect(result.otherErr?.isAbort).toBe(true)
      expect(result.streamErr?.isAbort).toBe(true)
      expect(result.promiseErr?.isAbort).toBe(true)
      expect(result.abortingErr?.abortValue).deep.equal({ reason: 'stream-abort', code: 101 })
      expect(result.otherErr?.abortValue).deep.equal({ reason: 'stream-abort', code: 101 })
      expect(result.streamErr?.abortValue).deep.equal({ reason: 'stream-abort', code: 101 })
      expect(result.promiseErr?.abortValue).deep.equal({ reason: 'stream-abort', code: 101 })
    })
  })

  test('streaming: channel listener Abort aborts sibling streaming values too', async () => {
    await page.click('#test-channel-abort-does-not-abort-streaming-values')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.firstErr?.isAbort).toBe(true)
      expect(result.secondErr?.isAbort).toBe(true)
      expect(result.streamErr?.isAbort).toBe(true)
      expect(result.firstErr?.abortValue).deep.equal({ reason: 'channel-listener-abort', code: 7 })
      expect(result.secondErr?.abortValue).deep.equal({ reason: 'channel-listener-abort', code: 7 })
      expect(result.streamErr?.abortValue).deep.equal({ reason: 'channel-listener-abort', code: 7 })
      expect(result.channelSendErr?.isAbort).toBe(true)
      expect(result.channelSendErr?.abortValue).deep.equal({ reason: 'channel-listener-abort', code: 7 })
      expect(result.channelCloseErr?.isAbort).toBe(true)
      expect(result.channelCloseErr?.abortValue).deep.equal({ reason: 'channel-listener-abort', code: 7 })
    })
  })

  // Half-duplex: browser withholds the response until upload completes,
  // so progress updates arrive simultaneously on the client side.
  // When full duplex is supported, client duration will exceed server duration
  // and the assertion below should be inverted.
  test('streaming: upload with progress (half duplex)', async () => {
    if (process.env.PUBLIC_ENV__TRANSPORT === 'ws') {
      return skip(
        'WS transport returns the HTTP response before the request body is fully consumed, breaking lazy file streaming (ERR_CONNECTION_RESET)',
      )
    }

    await page.click('#test-upload-progress')
    await autoRetry(async () => {
      const result = await getResult('#streaming-result')
      expect(result.done).toBe(true)
      expect(result.updates.length).greaterThanOrEqual(1)
      const last = result.updates[result.updates.length - 1]
      expect(last.bytesRead).toBe(10_000_000)
      expect(last.totalSize).toBe(10_000_000)
      expect(result.updates.length).greaterThan(1)
      for (let i = 1; i < result.updates.length; i++) {
        expect(result.updates[i].bytesRead).greaterThan(result.updates[i - 1].bytesRead)
      }
      const firstMs: number = result.updates[0].clientMs
      const lastMs: number = result.updates[result.updates.length - 1].clientMs
      // Half-duplex: browser buffers the full response before delivering any chunk,
      // so both updates arrive nearly simultaneously on the client.
      // Client duration must be less than server duration to prove no real-time delivery.
      expect(lastMs - firstMs).lessThan(last.duration)
    })
  })
}
