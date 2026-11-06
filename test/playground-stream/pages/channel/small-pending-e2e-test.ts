export { testSmallPending }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState } from '../../e2e-utils'
import { sendRefusedWarning } from '../../../utils'

// Run by the lanes whose server has the smallest `maxMemory`, 2 MiB.
function testSmallPending() {
  test('channel: a flood of empty sends to a channel no page attached to refuses those past maxMemory with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unattached-flood')

    await autoRetry(async () => {
      const state = await getResult<any>('#channel-state')
      expect(state.unattachedFlood?.text.ChannelOverflowError).greaterThan(0)
      expect(state.unattachedFlood?.binary.ChannelOverflowError).greaterThan(0)
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: the byte cap refuses the send with the named error', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-budgets')
    await autoRetry(async () => {
      const ids = JSON.parse((await page.textContent('#channel-budgets-result'))!)
      const state = await getCleanupState()
      expect(state[`budget_${ids.bytes}_credit`]).toBe('accepted')
      expect(state[`budget_${ids.bytes}`]).toContain('maxMemory')
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: multi-byte text that fits maxMemory to the byte is accepted, and one byte more is refused', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-at-limit')
    await autoRetry(async () => {
      const state = await getCleanupState()
      for (const kind of ['twoBytes', 'threeBytes', 'fourBytes']) {
        expect(state[`atLimit_${kind}_fits`]).toBe('accepted')
        expect(state[`atLimit_${kind}_over`]).toContain('maxMemory')
      }
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: acknowledged sends made before the page attaches are refused once they pass maxMemory, and the rest are answered', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-offline-budgets')
    await autoRetry(async () => {
      const { id } = JSON.parse((await page.textContent('#channel-offline-budgets-result'))!)
      const sends: string[] = JSON.parse((await getCleanupState())[`offline_budget_${id}`] ?? '[]')
      expect(sends.slice(0, 2)).deep.equal(['answered', 'answered'])
      expect(sends[2]).toContain(
        'ChannelOverflowError: Send refused: the most for one channel, or config.channel.maxMemory',
      )
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: an awaited message larger than maxMemory is refused, the channel stays open, and the next message reaches the page', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-awaited-from-offline')
    await autoRetry(
      async () => {
        const result = (await getResult<any>('#channel-state')).awaitedFromOffline!
        expect(result.closeErr).toBe(null)
        expect(result.received).toBe(2)
        const sends: string[] = JSON.parse((await getCleanupState())[`awaitedFromOffline_${result.channelId}`] ?? '[]')
        expect(sends[0]).toBe('sent')
        expect(sends[1]).toContain('config.channel.maxMemory')
        expect(sends[2]).toBe('sent')
      },
      { timeout: 30_000 },
    )
    expectLog(sendRefusedWarning)
  })

  // Three publishes of 4 MiB in one go: the wire has the first, and neither of the others fits beside it.
  test("channel: a broadcast the server can't hold even the newest message for closes with ChannelOverflowError", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-broadcast-overflow')
    await autoRetry(
      async () => {
        const { received, closeErr } = (await getResult<any>('#channel-state')).broadcastOverflow
        expect(closeErr).toBe('ChannelOverflowError')
        expect(received).lessThan(3)
      },
      { timeout: 30_000 },
    )
    expectLog('fell behind what the server holds for its client')
  })
}
