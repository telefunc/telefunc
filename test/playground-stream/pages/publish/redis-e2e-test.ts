export { testRedisReconnect }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, REDIS_TOOK_REFUSED_KEY } from '../../e2e-utils'

function testRedisReconnect() {
  test("broadcast: while Redis refuses one key's subscription, a publish on another key from the same server reaches its subscribers", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-beside-refused')
    let refuses = true
    await autoRetry(
      async () => {
        const result = await getResult('#redis-beside-refused-result')
        refuses = result.refusesChannels !== false
        if (refuses) expect(result).deep.equal({ refusedReady: 'rejected', published: 'sent', received: ['beside'] })
      },
      { timeout: 20_000 },
    )
    if (!refuses) throw new Error(REDIS_TOOK_REFUSED_KEY)
  })

  test("broadcast: a failed Redis subscription rejects the publishes of its key and kind, not the key's other kind, and a new subscription restores delivery", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-readiness')
    await autoRetry(async () => {
      expect(await getResult('#redis-readiness-result')).deep.equal({
        rejected: true,
        binary: 'fulfilled',
        received: ['recovered'],
      })
    })
  })

  test("broadcast: publishes across a key's 2^32nd sequence number in Redis reach its subscriber with the seq each publish returned", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-seq-boundary')
    await autoRetry(async () => {
      expect(await getResult('#redis-seq-boundary-result')).deep.equal({
        published: [2 ** 32 - 1, 2 ** 32, 2 ** 32 + 1],
        received: [
          { text: 'a', seq: 2 ** 32 - 1 },
          { text: 'b', seq: 2 ** 32 },
          { text: 'c', seq: 2 ** 32 + 1 },
        ],
      })
    })
  })

  test('broadcast: binary frames published from one reused buffer while Redis reconnects arrive as published', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-reconnect')
    await autoRetry(
      async () => {
        expect(await getResult<number[]>('#redis-reconnect-result')).deep.equal([1, 2, 3])
      },
      { timeout: 10_000 },
    )
  })
}
