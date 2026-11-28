export { testSlowPost }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

// Run by the lane whose server sets `sseFlushThrottle` to 1000 ms: the page confirms once a second.
function testSlowPost() {
  // 60/s of 4 KiB for 5 s, on a link that keeps up.
  test('channel: a lossy broadcast on SSE whose POSTs go once a second delivers every message on a link that keeps up', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-lossy-ticker')
    await autoRetry(
      async () => {
        const { lossyTicker } = await getResult<{
          lossyTicker: { received: number; last: number | undefined; worstMs: number } | null
        }>('#channel-state')
        expect(lossyTicker).deep.equal({ ...lossyTicker, received: 300, last: 299 })
      },
      { timeout: 40_000 },
    )
  })
}
