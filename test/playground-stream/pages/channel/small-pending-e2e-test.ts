export { testSmallPending }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState } from '../../e2e-utils'
import { sendRefusedWarning } from '../../../utils'

// Run by the lanes whose server has the smallest `maxPendingBytes`, 2 MiB.
function testSmallPending() {
  test('channel: a flood of empty sends to a channel no page attached to refuses those past maxPendingBytes with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unattached-flood')

    await autoRetry(async () => {
      const state = await getResult<any>('#channel-state')
      expect(state.unattachedFlood?.text.ChannelOverflowError).greaterThan(0)
      expect(state.unattachedFlood?.binary.ChannelOverflowError).greaterThan(0)
    })
  })

  test('channel: the byte cap refuses the send with the named error', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-budgets')
    await autoRetry(async () => {
      const ids = JSON.parse((await page.textContent('#channel-budgets-result'))!)
      const state = await getCleanupState()
      expect(state[`budget_${ids.bytes}_credit`]).toBe('accepted')
      expect(state[`budget_${ids.bytes}`]).toContain('maxPendingBytes')
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: multi-byte text that fits maxPendingBytes to the byte is accepted, and one byte more is refused', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-at-limit')
    await autoRetry(async () => {
      const state = await getCleanupState()
      for (const kind of ['twoBytes', 'threeBytes', 'fourBytes']) {
        expect(state[`atLimit_${kind}_fits`]).toBe('accepted')
        expect(state[`atLimit_${kind}_over`]).toContain('maxPendingBytes')
      }
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: acknowledged sends made before the page attaches are refused once they pass maxPendingBytes, and the rest are answered', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-offline-budgets')
    await autoRetry(async () => {
      const { id } = JSON.parse((await page.textContent('#channel-offline-budgets-result'))!)
      const sends: string[] = JSON.parse((await getCleanupState())[`offline_budget_${id}`] ?? '[]')
      expect(sends.slice(0, 2)).deep.equal(['answered', 'answered'])
      expect(sends[2]).toContain('ChannelOverflowError: Send refused: config.channel.maxPendingBytes')
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: an awaited message larger than maxPendingBytes is refused, the channel stays open, and the next message reaches the page', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-awaited-from-offline')
    await autoRetry(
      async () => {
        const result = (await getResult<any>('#channel-state')).awaitedFromOffline!
        expect(result.closeErr).toBe(null)
        expect(result.received).toBe(2)
        const sends: string[] = JSON.parse((await getCleanupState())[`awaitedFromOffline_${result.channelId}`] ?? '[]')
        expect(sends[0]).toBe('sent')
        expect(sends[1]).toContain('config.channel.maxPendingBytes')
        expect(sends[2]).toBe('sent')
      },
      { timeout: 30_000 },
    )
    expectLog(sendRefusedWarning)
  })
}
