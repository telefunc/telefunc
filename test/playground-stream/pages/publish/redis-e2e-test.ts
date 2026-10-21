export { testRedisReconnect }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

function testRedisReconnect() {
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
