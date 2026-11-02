export { testBacklog }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState } from '../../e2e-utils'
import { sendsAheadWarning, withNetworkConditions } from '../../../utils'

type BacklogState = { channelId: string | null; received: number; outOfOrder: number; closeErr: string | null }
type Run = { sent: number; refused: string | null }

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
    expect((await getRun(channelId)).refused).toBe(null)
    await autoRetry(
      async () => {
        expect(await getResult<BacklogState>('#backlog-state')).deep.equal({
          channelId,
          received: 4000,
          outOfOrder: 0,
          closeErr: null,
        })
      },
      { timeout: 30_000 },
    )
  })

  test('backlog: 96 MiB the server sends nobody awaiting to a page that stops reading all go out, with a warning, and arrive whole and in order once it reads, the channel open', async () => {
    const channelId = await openBacklog()
    // A page that reads nothing: what the server sends waits on the way.
    const { sent, refused } = await withNetworkConditions({ downloadThroughput: 1, uploadThroughput: -1 }, async () => {
      await page.click('#backlog-run-unawaited')
      return getRun(channelId)
    })
    expect({ sent, refused }).deep.equal({ sent: 6000, refused: null })
    await autoRetry(
      async () => {
        expect(await getResult<BacklogState>('#backlog-state')).deep.equal({
          channelId,
          received: 6000,
          outOfOrder: 0,
          closeErr: null,
        })
        expectLog(sendsAheadWarning)
      },
      { timeout: 30_000 },
    )
  })
}
