export { testSmallPending }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import {
  navigate,
  getResult,
  getCleanupState,
  reconnectAtFullDelay,
  defined,
  runServerAction,
  sleep,
} from '../../e2e-utils'
import { request, type IncomingMessage } from 'node:http'
import { startBlackHoleProxy } from '../../black-hole-proxy'
import { parseChannelTransports } from '../../channelTransports'
import {
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  WIRE_VERSION,
} from '../../../../packages/telefunc/wire-protocol/constants'
import { encodeSseRequest } from '../../../../packages/telefunc/wire-protocol/wire/envelope'
import { encode } from '../../../../packages/telefunc/wire-protocol/wire/frame'
import { pingDeadline } from '../../../../packages/telefunc/wire-protocol/wire/pingDeadline'
import type { ChannelMemoryPeak } from '../../channel-memory'
import { sendRefusedWarning } from '../../../utils'
import { burstOf, burstWhileOffline, memoryPeakOf } from './burst-e2e'

/** What the channel page shows of the rows here. */
type SmallPendingState = {
  channelsQueued: { queued: string[]; received: string[][]; closed: boolean[] } | null
  acksWaiting: {
    error?: string
    ended: string
    started: number
    settled: Record<string, number>
    next: string | null
  } | null
  unattachedFlood: Record<'text' | 'binary', Record<string, number>> | null
  awaitedFromOffline: { channelId: string; received: number; closeErr: string | null } | null
  broadcastOverflow: { received: number; closeErr: string } | null
  broadcastBehind: {
    seqsAhead: number[]
    isClosed: boolean
    receiptAhead: number
    opening: { receipts: number; last: string }
    attached: { receipts: number; last: string }
    reconnected: { receipts: number; last: string }
  } | null
  broadcastAwayShared: { received: number[][]; closed: string[] } | null
  broadcastBehindCloses: {
    seqsAhead: number[]
    closeErr: string | null
    pastMemory: { seqsAhead: number[]; closeErr: string }
    across: { publish: string; received: number; closeErr: string }
  } | null
  broadcastKeys: { publish: string; firstClosed: string; secondReceived: string[]; secondClosed: boolean } | null
  unreadSends: { opened: true } | null
  keptUpload: { next: string; answer: string; second: string; third: string } | null
}

/** The channel page's state. */
const stateOf = () => getResult<SmallPendingState>('#channel-state')

/** The server's `config.channel.pingInterval`. */
const PING_INTERVAL = 1000

/** POSTs `body` as a page's SSE request: its status, and its answer, which it reads no further than its first chunk. */
async function postSse(body: Blob): Promise<{ status: number; res: IncomingMessage }> {
  const bytes = Buffer.from(await body.arrayBuffer())
  return new Promise((resolve, reject) => {
    const req = request(
      `${getServerUrl()}/_telefunc?wire=${WIRE_VERSION}`,
      { method: 'POST', headers: { 'x-telefunc-request': 'sse' } },
      (res) => {
        res.once('data', () => {
          res.pause()
          resolve({ status: res.statusCode ?? 0, res })
        })
        res.once('end', () => resolve({ status: res.statusCode ?? 0, res }))
      },
    )
    req.on('error', reject)
    req.end(bytes)
  })
}

/** PINGs that list as many ended channels as a page may have: each PONG answers each. */
const ended = Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, i) => ({ ix: i + 1, lastSeq: 0 }))
const widePings = (count: number) => Array.from({ length: count }, () => encode.ping(ended))

/** A page of the test's own, whose session holds one channel of `onUnreadEventStream`, and whose event stream it stops
 *  reading at its first chunk. */
async function openUnreadEventStream() {
  const call = await fetch(`${getServerUrl()}/_telefunc`, {
    method: 'POST',
    headers: { 'x-telefunc-request': 'txt' },
    body: JSON.stringify({
      file: '/pages/channel/SmallPendingExtra.telefunc.ts',
      name: 'onUnreadEventStream',
      args: [],
    }),
  })
  const { channelId } = JSON.parse(await call.text()).ret as { channelId: string }
  const connId = crypto.randomUUID()
  const reconcile = encode.reconcile({
    sessionId: crypto.randomUUID(),
    attempt: 1,
    open: [{ id: channelId, ix: 0, lastSeq: 0, initial: true }],
  })
  const { res } = await postSse(encodeSseRequest({ connId, streamResponse: true }, [reconcile]))
  return {
    channelId,
    /** How the server ended the channel, or undefined while it is open. */
    ended: async () => (await getCleanupState())[`unreadEventStream_${channelId}`],
    /** The page reads its event stream again, to its end. */
    catchUp: () => void res.on('data', () => {}).resume(),
    pings: (count = 10) => postSse(encodeSseRequest({ connId }, widePings(count))),
  }
}

/** What the server's channel memory holds, and the most it held since the last `openUnreadEventStream()`. */
async function channelMemory(): Promise<ChannelMemoryPeak & { total: number }> {
  await runServerAction('channelMemory')
  return JSON.parse(defined((await getCleanupState()).channelMemory, 'channelMemory'))
}

// Run by the lanes whose server has the smallest `maxMemory`, 2 MiB.
function testSmallPending() {
  // 1,200 KiB held, 700 KiB waiting for the page to confirm it, and 200 KiB that would carry both past 2 MiB.
  for (const kind of ['text', 'binary'] as const) {
    test(`channel: a ${kind} send nobody awaits that would carry what the server holds and what waits for room past maxMemory is refused, the one waiting arrives, and the channel stays open`, async () => {
      const channelId = await burstWhileOffline(`${kind}-past-memory`)
      await autoRetry(
        async () => {
          expect(await burstOf()).deep.include({ received: [0, 1], onClose: null })
        },
        { timeout: 30_000 },
      )
      expect((await getCleanupState())[`wireBurst_${channelId}`]).toContain('config.channel.maxMemory')
      expectLog(sendRefusedWarning)
      const { held, heldWithWaiting, maxMemory } = await memoryPeakOf(channelId)
      expect(held).greaterThanOrEqual(1200 * 1024)
      expect(heldWithWaiting).greaterThanOrEqual(1900 * 1024)
      expect(heldWithWaiting).lessThanOrEqual(maxMemory)
    })
  }

  // 1 MiB queued on each of eight channels before their telefunction returns: on 2 MiB, one fits and a second doesn't.
  test('channel: sends queued before a telefunction returns count against maxMemory together: those past it are refused with ChannelOverflowError, and every channel stays open', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-channels-queued')
    await autoRetry(
      async () => {
        const { queued, received, closed } = defined((await stateOf()).channelsQueued, 'channelsQueued')
        expect(received).deep.equal([['queued'], ...Array(7).fill([])])
        expect(queued).deep.equal(['delivered', ...Array(7).fill('ChannelOverflowError')])
        expect(closed).deep.equal(Array(8).fill(false))
      },
      { timeout: 30_000 },
    )
    expectLog(sendRefusedWarning)
  })

  // 500 tiny acknowledged sends wait on the server, then their 4 KiB answers come while the page is away: those past
  // maxMemory are refused, and the channel stays open.
  test("channel: acknowledged sends waiting for their answers each settle, those whose answers the server can't hold with ChannelOverflowError, and the channel stays open", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-acks-waiting')
    await autoRetry(
      async () => {
        const { error, ended, started, settled, next } = defined((await stateOf()).acksWaiting, 'acksWaiting')
        expect(error).toBe(undefined)
        expect(ended).toBe('open')
        expect(started).toBe(500)
        expect(settled.answered).greaterThan(0)
        expect(settled.ChannelOverflowError).greaterThan(0)
        expect((settled.answered ?? 0) + (settled.ChannelOverflowError ?? 0)).toBe(500)
        expect(next).toBe('answered next')
      },
      { timeout: 30_000 },
    )
  })

  // 10,000 of them: the server can't hold room for all their answers, so it closes the channel before running the rest.
  test('channel: more acknowledged sends waiting for their answers than the server can hold even refusals for close the channel with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-acks-flood')
    await autoRetry(
      async () => {
        const { error, ended, started, settled } = defined((await stateOf()).acksWaiting, 'acksWaiting')
        expect(error).toBe(undefined)
        expect(ended).toBe('ChannelOverflowError')
        expect(started).lessThan(10_000)
        expect(settled).deep.equal({ ChannelOverflowError: 10_000 })
      },
      { timeout: 30_000 },
    )
  })

  test('channel: a flood of empty sends to a channel no page attached to refuses those past maxMemory with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-unattached-flood')

    await autoRetry(async () => {
      const state = await stateOf()
      expect(state.unattachedFlood?.text.ChannelOverflowError).greaterThan(0)
      expect(state.unattachedFlood?.binary.ChannelOverflowError).greaterThan(0)
    })
    expectLog(sendRefusedWarning)
  })

  test('channel: the byte cap refuses the send with the named error', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-budgets')
    await autoRetry(async () => {
      const ids = JSON.parse(defined(await page.textContent('#channel-budgets-result'), '#channel-budgets-result'))
      const state = await getCleanupState()
      expect(state[`budget_${ids.bytes}_credit`]).toBe('accepted')
      expect(state[`budget_${ids.bytes}`]).toContain('maxMemory')
    })
    expectLog(sendRefusedWarning)
  })

  test("channel: multi-byte text that fits a channel's limit to the byte is accepted, and one byte more is refused", async () => {
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
      const { id, refused, next, isClosed } = JSON.parse(
        defined(await page.textContent('#channel-offline-budgets-result'), '#channel-offline-budgets-result'),
      )
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
        const result = defined((await stateOf()).awaitedFromOffline, 'awaitedFromOffline')
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
        const { received, closeErr } = defined((await stateOf()).broadcastOverflow, 'broadcastOverflow')
        expect(closeErr).toBe('ChannelOverflowError')
        expect(received).lessThan(3)
      },
      { timeout: 30_000 },
    )
  })

  // The page's own 20 publishes at once before its first attach, each its ordinal at the same size: only the newest
  // waits, so all 20 settle with one receipt and the server gets the last. Then the same 20 attached, on a link that
  // keeps up: each goes with a receipt of its own. Then 16 publishes of 256 KiB in one go to a lossy broadcast: each
  // one the wire hasn't taken goes as a newer one comes. Then the page's publish (17) is outstanding as its connection
  // drops, with 17 more published behind its receipt while it is away: only the newest of them is held. Then the page's
  // own 20 again, while it reconnects.
  test('channel: a client that falls behind a lossy broadcast skips to the newest, with a jump in info.seq, and stays open, and so does its own uplink', async () => {
    await navigate(`${getServerUrl()}/channel`)
    // The page is away while the server publishes: its reconnect waits its whole first delay.
    await reconnectAtFullDelay()
    await page.click('#channel-test-broadcast-behind')
    await autoRetry(
      async () => {
        const { seqsAhead, isClosed, receiptAhead, opening, attached, reconnected } = defined(
          (await stateOf()).broadcastBehind,
          'broadcastBehind',
        )
        const [burst, away] = [seqsAhead.filter((a: number) => a <= 16), seqsAhead.filter((a: number) => a > 17)]
        expect(burst.at(-1)).toBe(16)
        expect(burst.length).lessThan(16)
        expect(receiptAhead).toBe(17)
        expect(away).deep.equal([34])
        expect(opening).deep.equal({ receipts: 1, last: '19' })
        expect(attached).deep.equal({ receipts: 20, last: '219' })
        expect(reconnected.receipts).lessThan(20)
        expect(reconnected.last).toBe('119')
        expect(isClosed).toBe(false)
      },
      { timeout: 30_000 },
    )
  })

  // Two lossy broadcasts of one key whose page is away, with bodies of 1200 KiB: one fits the 2 MiB budget, two don't.
  // Each newer body replaces the one waiting before the server decides what it can hold, so neither closes.
  test("channel: a lossy broadcast whose client is away keeps its newest where it and the one before don't both fit the server's memory, for two subscribers sharing them", async () => {
    await navigate(`${getServerUrl()}/channel`)
    // The page is away while the server publishes: its reconnect waits its whole first delay.
    await reconnectAtFullDelay()
    await page.click('#channel-test-broadcast-away-shared')
    await autoRetry(
      async () => {
        const { received, closed } = defined((await stateOf()).broadcastAwayShared, 'broadcastAwayShared')
        expect(closed).deep.equal(['open', 'open'])
        expect(received).deep.equal([[18], [18]])
      },
      { timeout: 30_000 },
    )
  })

  // The same two without `lossy`, the second on a broadcast of its own: where its page would miss a message, it closes
  // instead, and the receipt behind the gap doesn't come either. Between them, three publishes and one past the
  // server's memory in the same turn: the gap is the fourth, so the page gets all three first.
  test('channel: a client that falls behind a broadcast gets every message up to the gap, then closes with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/channel`)
    // The page is away while the server publishes: its reconnect waits its whole first delay.
    await reconnectAtFullDelay()
    await page.click('#channel-test-broadcast-behind-closes')
    await autoRetry(
      async () => {
        const { seqsAhead, closeErr, pastMemory, across } = defined(
          (await stateOf()).broadcastBehindCloses,
          'broadcastBehindCloses',
        )
        expect(closeErr).toBe('ChannelOverflowError')
        expect(seqsAhead.length).lessThan(16)
        expect(seqsAhead).deep.equal(seqsAhead.map((_: number, i: number) => i + 1))
        expect(pastMemory).deep.equal({ seqsAhead: [1, 2, 3], closeErr: 'ChannelOverflowError' })
        expect(across).deep.equal({ publish: 'ChannelOverflowError', received: 0, closeErr: 'ChannelOverflowError' })
      },
      { timeout: 30_000 },
    )
  })

  test("channel: a broadcast publish larger than the server may hold closes its own key's subscriber with ChannelOverflowError, and another key's gets its next publish", async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-broadcast-keys')
    await autoRetry(
      async () => {
        expect(defined((await stateOf()).broadcastKeys, 'broadcastKeys')).deep.equal({
          publish: 'published',
          firstClosed: 'ChannelOverflowError',
          secondReceived: ['after the oversized publish'],
          secondClosed: false,
        })
      },
      { timeout: 30_000 },
    )
  })

  test('channel: a retained upload drops the cancelled first file and the refused tail, keeping the second file and room for an answer', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-test-kept-upload')
    await autoRetry(
      async () => {
        const result = defined((await stateOf()).keptUpload, 'keptUpload')
        expect({
          ...result,
          third: result.third.includes('the rest was dropped.') ? 'refused' : result.third,
        }).deep.equal({
          next: 'hello',
          answer: `${512 * 1024} bytes`,
          second: 'bytes match',
          third: 'refused',
        })
      },
      { timeout: 30_000 },
    )
  })

  // Over SSE what the server sends a page waits in its event stream's response once the kernel's buffers are full.
  if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).join() === 'sse') {
    for (const count of [1, 4])
      test(`channel: ${count} pages that flood PINGs and read nothing of their event streams hold no more than maxMemory together, their channels end with ChannelOverflowError, and the server lets go of what they held a ping deadline later`, async () => {
        const pages = await Promise.all(Array.from({ length: count }, openUnreadEventStream))
        const before = (await channelMemory()).total
        const answered = pages.map(() => true)
        for (let round = 0; round < 300 && answered.some(Boolean); round++) {
          for (const [i, each] of pages.entries()) if (answered[i]) answered[i] = (await each.pings()).status === 200
        }
        expect(answered).deep.equal(pages.map(() => false))
        await autoRetry(async () => {
          expect(await Promise.all(pages.map((each) => each.ended()))).deep.equal(
            pages.map(() => 'ChannelOverflowError'),
          )
        })
        const { held, maxMemory } = await channelMemory()
        expect(held).greaterThan(maxMemory / 2)
        expect(held).lessThanOrEqual(maxMemory)
        await sleep(pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE) + 500)
        await autoRetry(async () => {
          expect((await channelMemory()).total).lessThanOrEqual(before)
        })
      })

    test('channel: a page that stops reading its event stream and catches up: the server lets go of the PONGs its event stream held for it', async () => {
      const each = await openUnreadEventStream()
      const before = (await channelMemory()).total
      let held = before
      // One PING at a time, so that the PONGs fill the kernel's buffers and then hold past 1 MiB, short of maxMemory.
      for (let round = 0; round < 200 && held - before <= 2 ** 20; round++) {
        expect((await each.pings(1)).status).toBe(200)
        held = (await channelMemory()).total
      }
      expect(held - before).greaterThan(2 ** 20)
      each.catchUp()
      await autoRetry(async () => {
        expect((await channelMemory()).total - before).lessThan((held - before) / 4)
      })
    })
  }

  // Each page's link carries 4 KB/s, so of 340 sends of 15 KiB to it next to nothing reaches the page: what the server
  // sends waits in its memory, or is refused.
  test('channel: four pages that read nothing of what the server sends them hold no more than maxMemory together on the server', async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    const browser = defined(page.context().browser(), 'the browser')
    const contexts = await Promise.all(Array.from({ length: 3 }, () => browser.newContext()))
    const pages = [page, ...(await Promise.all(contexts.map((context) => context.newPage())))]
    try {
      for (const each of pages) await each.goto(`${proxy.url}/channel`)
      for (const each of pages) await each.waitForSelector('#hydrated', { state: 'attached' })
      proxy.pace({ downBytesPerSecond: 4096, upBytesPerSecond: 1_000_000, queueBytes: 16 * 1024 })
      for (const each of pages) {
        await each.click('#channel-test-unread-sends')
        await autoRetry(
          async () => {
            const state = JSON.parse(defined(await each.textContent('#channel-state'), '#channel-state'))
            expect(state.unreadSends).deep.equal({ opened: true })
          },
          { timeout: 30_000 },
        )
      }
      await runServerAction('unreadSends')
      await sleep(3000)
      await runServerAction('unreadSendsPeak')
      const { held, maxMemory, outcomes } = JSON.parse(
        defined((await getCleanupState()).unreadSendsPeak, 'the peak'),
      ) as ChannelMemoryPeak & { outcomes: Record<string, number>[] }
      expect(outcomes.length).toBe(4)
      expect(held).greaterThan(maxMemory / 2)
      expect(held).lessThanOrEqual(maxMemory)
      expectLog(sendRefusedWarning)
    } finally {
      for (const context of contexts) await context.close()
      await page.goto('about:blank')
      await proxy.close()
    }
  })
}
