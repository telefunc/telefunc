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

  test("channel: acknowledged sends made before the page attaches are refused once they pass maxMemory, and the rest are answered; a page's acknowledged send the server can't hold is refused with ChannelOverflowError and the channel stays open", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-offline-budgets')
    await autoRetry(async () => {
      const { id, refused, next, isClosed } = JSON.parse((await page.textContent('#channel-offline-budgets-result'))!)
      expect(refused).toBe('ChannelOverflowError')
      expect(next).toBe('answered next')
      expect(isClosed).toBe(false)
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

  // 16 publishes of 256 KiB in one go: past 7/8 of maxMemory, the oldest the wire hasn't taken go. Then the page's
  // publish (17) is outstanding as its connection drops, with 17 more published behind its receipt while it is away.
  test('channel: a client that falls behind a broadcast misses its oldest messages, gets the newest with a jump in info.seq, and stays open', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-broadcast-behind')
    await autoRetry(
      async () => {
        const { seqsAhead, isClosed, receiptAhead } = (await getResult<any>('#channel-state')).broadcastBehind
        const [burst, away] = [seqsAhead.filter((a: number) => a <= 16), seqsAhead.filter((a: number) => a > 17)]
        expect(burst.at(-1)).toBe(16)
        expect(burst.length).lessThan(16)
        expect(receiptAhead).toBe(17)
        expect(away.at(-1)).toBe(34)
        expect(away.length).lessThan(17)
        expect(isClosed).toBe(false)
      },
      { timeout: 30_000 },
    )
    expectLog('fell behind what the server holds for its client')
    expectLog('missed messages')
  })

  // The same two with `whenBehind: 'close'`, the second on a broadcast of its own: where its page would miss the oldest,
  // it closes instead, and the receipt behind the gap doesn't come either.
  test("channel: a client that falls behind a broadcast with whenBehind 'close' gets every message up to the gap, then closes with ChannelOverflowError", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-broadcast-behind-closes')
    await autoRetry(
      async () => {
        const { seqsAhead, closeErr, across } = (await getResult<any>('#channel-state')).broadcastBehindCloses
        expect(closeErr).toBe('ChannelOverflowError')
        expect(seqsAhead.length).lessThan(16)
        expect(seqsAhead).deep.equal(seqsAhead.map((_: number, i: number) => i + 1))
        expect(across).deep.equal({ publish: 'ChannelOverflowError', received: 0, closeErr: 'ChannelOverflowError' })
      },
      { timeout: 30_000 },
    )
  })
}
