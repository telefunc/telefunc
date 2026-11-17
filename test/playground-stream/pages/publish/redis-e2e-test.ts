export { testRedisReconnect }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

function testRedisReconnect() {
  test("broadcast: while Redis refuses one key's subscription, a publish on another key from the same server reaches its subscribers", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-beside-refused')
    await autoRetry(
      async () => {
        expect(await getResult('#redis-beside-refused-result')).deep.equal({
          refusedReady: 'rejected',
          published: 'sent',
          received: ['beside'],
        })
      },
      { timeout: 20_000 },
    )
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
