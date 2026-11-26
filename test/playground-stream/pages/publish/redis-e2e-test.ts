export { testRedisReconnect, testBroadcastRedisSubscriberDropped, testBroadcastRedisPublishLimit }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
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
        if (refuses) expect(result).deep.equal({ refusedOpened: 'rejected', published: 'sent', received: ['beside'] })
      },
      { timeout: 20_000 },
    )
    if (!refuses) throw new Error(REDIS_TOOK_REFUSED_KEY)
  })

  test('broadcast: a subscription in effect whose renewal Redis refuses ends its subscriber, reported once, and a fresh subscriber hears the next publish once', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-renewal-refused')
    let refuses = true
    await autoRetry(
      async () => {
        const result = await getResult('#redis-renewal-refused-result')
        refuses = result.refusesChannels !== false
        if (refuses) expect(result).deep.equal({ opened: 'resolved', reports: 1, heard: ['before'], fresh: ['after'] })
      },
      { timeout: 30_000 },
    )
    if (!refuses) throw new Error(REDIS_TOOK_REFUSED_KEY)
    expectLog('NOPERM')
    expectLog('Redis subscriber connection closed')
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

function testBroadcastRedisPublishLimit() {
  test("broadcast: a publish over the Redis backend's ceiling rejects, by its bytes, and the largest one and the next arrive", async () => {
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
        const result = await getResult<Record<
          'text' | 'binary',
          { published: number; publishedWhileDown: number; received: number; reported: number }
        > | null>('#redis-subscriber-dropped-result')
        if (result === null) throw new Error('No result yet')
        for (const [kind, { published, publishedWhileDown, received, reported }] of Object.entries(result)) {
          expect({ kind, published, publishedWhileDown }).deep.equal({ kind, published: 300, publishedWhileDown: 100 })
          // What was published while the connection was down can't have arrived: the row can't pass with nothing lost.
          expect({ kind, reportedWhileDown: reported >= publishedWhileDown }).deep.equal({
            kind,
            reportedWhileDown: true,
          })
          expect({ kind, accounted: received + reported }).deep.equal({ kind, accounted: published })
        }
      },
      { timeout: 15_000 },
    )
    expectLog('Redis subscriber connection closed')
  })

  test("broadcast: a subscriber connection goes silent, its socket still open: it's taken for lost, what it missed is reported, and later publishes arrive", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-silent')
    await autoRetry(
      async () => {
        expect(await getResult('#redis-silent-result')).deep.equal({
          received: ['before', 'after'],
          reported: ["1 text message never reached this server's Redis subscription"],
          connections: 2,
        })
      },
      { timeout: 50_000 },
    )
    expectLog('Redis subscriber connection went silent')
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

  test("broadcast: reading where a new subscription starts fails during a publish of the key's other kind: the publish arrives, and that subscription reads again until it is ready", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-redis-start-read-failed')
    await autoRetry(
      async () => {
        expect(await getResult('#redis-start-read-failed-result')).deep.equal({
          received: ['before', 'beside the failed read'],
          reported: [],
          binary: 'ready',
          startReads: 2,
        })
      },
      { timeout: 10_000 },
    )
    expectLog("Telefunc's Redis backend can't reach Redis at")
  })
}
