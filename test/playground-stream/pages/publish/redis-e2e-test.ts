export { testRedisReconnect, testBroadcastRedisSubscriberDropped, testBroadcastRedisPublishLimit }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

function testRedisReconnect() {
  test('broadcast: failed Redis subscriptions reject publishes and a new subscription restores delivery', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-readiness')
    await autoRetry(async () => {
      expect(await getResult('#redis-readiness-result')).deep.equal({ rejected: true, received: ['recovered'] })
    })
  })

  test('broadcast: Redis denies one channel: its listener and publish get the reason, the channels next to it keep delivering', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-channel-denied')
    await autoRetry(async () => {
      expect(await getResult('#redis-channel-denied-result')).deep.equal({
        deniedListenerTold: true,
        deniedPublishTold: true,
        upChanges: [],
        received: ['allowed-fresh: next to it', 'allowed-up: after', 'allowed-up: first'],
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

function testBroadcastRedisPublishLimit() {
  test('broadcast: a publish larger than Redis delivers rejects, by its bytes, and the largest one and the next arrive', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-over-limit')
    await autoRetry(
      async () => {
        expect(await getResult('#redis-over-limit-result')).deep.equal({
          refused: ['ChannelOverflowError', 'ChannelOverflowError'],
          received: [28 * 1024 * 1024, 1],
        })
      },
      { timeout: 10_000 },
    )
  })
}

function testBroadcastRedisSubscriberDropped() {
  test("broadcast: Redis drops a server's subscriber connection during publishes of both kinds: what didn't arrive is reported, count for count, per kind", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-subscriber-dropped')
    await autoRetry(
      async () => {
        const result = await getResult<Record<'text' | 'binary', Record<string, number>> | null>(
          '#redis-subscriber-dropped-result',
        )
        expect(result).not.toBe(null)
        for (const { published, received, reported } of Object.values(result!)) {
          expect(published).toBe(300)
          expect(reported! > 0).toBe(true)
          expect(received! + reported!).toBe(300)
        }
      },
      { timeout: 15_000 },
    )
    expectLog('Redis subscriber connection closed')
  })

  test('broadcast: a publish lands while a reconnected subscription reads where its key starts: it arrives, and nothing is reported', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-during-resume')
    await autoRetry(
      async () => {
        expect(await getResult('#redis-during-resume-result')).deep.equal({
          received: ['before', 'during the read', 'after'],
          reported: [],
        })
      },
      { timeout: 10_000 },
    )
    expectLog('Redis subscriber connection closed')
  })
}
