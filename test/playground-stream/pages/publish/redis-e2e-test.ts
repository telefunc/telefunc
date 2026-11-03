export { testRedisReconnect }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

function testRedisReconnect() {
  test('broadcast: failed Redis subscriptions reject publishes and a new subscription restores delivery', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-readiness')
    await autoRetry(async () => {
      expect(await getResult('#redis-readiness-result')).deep.equal({ rejected: true, received: ['recovered'] })
    })
    // The subscriber connection reports the outage the refusal starts, once.
    expectLog('NOPERM', { filter: (log) => log.logSource === 'stderr' || log.logSource === 'stdout' })
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
