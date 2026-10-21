export { testBacklog }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState, withNetworkConditions } from '../../e2e-utils'

type BacklogState = { channelId: string | null; received: number; outOfOrder: number }
type Run = { sent: number; refused: string[] }

async function openBacklog() {
  await navigate(`${getServerUrl()}/backlog`)
  await page.click('#backlog-open')
  let channelId = ''
  await autoRetry(async () => {
    const state = await getResult<BacklogState>('#backlog-state')
    expect(state.channelId).not.toBe(null)
    channelId = state.channelId!
  })
  return channelId
}

async function getRun(channelId: string): Promise<Run> {
  let run: Run | undefined
  await autoRetry(async () => {
    const raw = (await getCleanupState())[`backlog_${channelId}`]
    expect(raw).not.toBe(undefined)
    run = JSON.parse(raw!)
  })
  return run!
}

function testBacklog() {
  test('backlog: 64 MiB the server sends awaited reach the page whole and in order', async () => {
    const channelId = await openBacklog()
    await page.click('#backlog-run-awaited')
    expect((await getRun(channelId)).refused).deep.equal([])
    await autoRetry(
      async () => {
        expect(await getResult<BacklogState>('#backlog-state')).deep.equal({ channelId, received: 4000, outOfOrder: 0 })
      },
      { timeout: 30_000 },
    )
  })

  test('backlog: sends nobody awaits to a page that stops reading are refused with ChannelOverflowError, and what was taken arrives in order once it reads', async () => {
    const channelId = await openBacklog()
    // A page that reads nothing: what the server sends waits on the way.
    const { sent, refused } = await withNetworkConditions({ downloadThroughput: 1, uploadThroughput: -1 }, async () => {
      await page.click('#backlog-run-unawaited')
      return getRun(channelId)
    })
    expect(refused.length).greaterThan(0)
    expect(new Set(refused)).deep.equal(new Set(['ChannelOverflowError']))
    await autoRetry(
      async () => {
        expect(await getResult<BacklogState>('#backlog-state')).deep.equal({
          channelId,
          received: sent - refused.length,
          outOfOrder: 0,
        })
      },
      { timeout: 30_000 },
    )
  })
}
