export { testPublish, testPublishHold, testPublishFailingBackend }

import type { ConsoleMessage, WebSocket } from 'playwright-chromium'
import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import {
  navigate,
  getResult,
  failBackend,
  getCleanupState,
  resetCleanupState,
  cutWire,
  canCutWire,
  runServerAction,
  forceServerGc,
  serverHeapUsed,
  serverRuntime,
} from '../../e2e-utils'

type ShieldState = {
  validReceiptKey: string | null
  validReceived: Array<{ text: string }> | null
  invalidThrew: boolean | null
  invalidErrorMessage: string | null
  receivedAfterInvalid: Array<{ text: string }> | null
}

type TextBroadcastResult = {
  acks: Array<{ seq: number; key: string }>
  received: Array<{ text: string; from: string; seq: number }>
}

type BinaryPairResult = {
  acks: Array<{ seq: number; key: string }>
  received: Array<{ size: number; firstByte: number; seq: number }>
}

type ServerBroadcastResult = {
  received: Array<{ size: number; firstByte: number }>
  done: boolean
}

/** A fresh key's first seq depends on the backend (Redis seeds it from its clock): assert three in a row. */
function consecutive(first: number): number[] {
  return [first, first + 1, first + 2]
}

function testPublish(isDev: boolean, inDocker = false) {
  const transports: string[] = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS ?? '["sse"]')
  const transport = transports.at(-1)

  test('broadcast: a messages() loop that falls 2 MiB behind a busy broadcast ends with ChannelOverflowError, on the page and on the server, and the page is sent nothing published after', async () => {
    let wsBytes = 0
    const onWebSocket = (ws: WebSocket) => {
      if (ws.url().includes('/_telefunc')) ws.on('framereceived', ({ payload }) => (wsBytes += payload.length))
    }
    page.on('websocket', onWebSocket)
    try {
      await navigate(`${getServerUrl()}/publish`)
      await page.click('#test-broadcast-slow-loop')
      await autoRetry(
        async () => {
          const result = await getResult<any>('#publish-result')
          expect(result.error).toBe('ChannelOverflowError')
          expect(result.received).lessThan(1000)
          expect(result.server.error).toBe('ChannelOverflowError')
          expect(result.server.received).lessThan(1000)
        },
        { timeout: 30_000 },
      )
      const before = wsBytes
      await page.click('#test-broadcast-after-overflow')
      await autoRetry(async () => expect(await page.textContent('#publish-result')).toBe('flooded'))
      // 800 KiB was published after the loop ended; Playwright sees an SSE stream's bytes only as it ends.
      if (transport === 'ws') expect(wsBytes - before).lessThan(64 * 1024)
    } finally {
      page.off('websocket', onWebSocket)
    }
  })

  test('broadcast: a messages() loop that fell behind still ends with ChannelOverflowError after the server closes the broadcast, on the page and on the server', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-overflow-then-close')
    await autoRetry(
      async () => {
        expect(await getResult('#publish-result')).deep.equal({
          error: 'ChannelOverflowError',
          received: 1,
          closed: 'clean',
          server: { error: 'ChannelOverflowError', received: 1 },
        })
      },
      { timeout: 30_000 },
    )
  })

  test('broadcast: a slow server messages() loop over a burst published and closed in one task ends with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-burst-then-close')
    await autoRetry(async () => {
      const result = await getResult<any>('#publish-result')
      expect(result.error).toBe('ChannelOverflowError')
      expect(result.received).lessThan(1000)
    })
  })

  test('broadcast: a messages() loop that keeps up gets every one of 600 awaited publishes of 2 KiB, in order', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-keeps-up')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({ received: 600, inOrder: true })
    })
  })

  // The in-memory adapter: these lanes publish through Redis.
  if (!inDocker && !process.env.REDIS_URL) {
    test('broadcast: in memory, a loop of publishes a listener answers delivers in it, a listener publishing 125 MiB delivers it all, one publishing 375 MiB holds at most 320 MiB and rejects the rest, one 8 MiB message alone goes through, four producers awaiting each publish deliver all of theirs, a timer publishing 4,096 a tick leaves none waiting at the next, a listener answering itself at once lets a timer fire, and a timer ends a loop awaiting publishes and a listener answering itself after an `await`', async () => {
      await navigate(`${getServerUrl()}/publish`)
      await page.click('#test-in-memory-publishes')
      await autoRetry(
        async () => {
          const {
            longLoopsInLoop,
            behindAnAnswer,
            loop,
            listenerBatch,
            pastTheBound,
            aloneBytes,
            producers,
            timerProducer,
            timerFiredMidChain,
            awaitedLoopEnded,
            answeringListenerEnded,
          } = await getResult<any>('#publish-result')
          expect(longLoopsInLoop).deep.equal([20_000, 20_000])
          expect(behindAnAnswer.inLoop).greaterThan(4900)
          expect(behindAnAnswer.inLoop).lessThan(5121)
          expect(behindAnAnswer.delivered).toBe(6000)
          expect(loop).deep.equal({ deliveredInLoop: 3000, inOrder: true })
          expect(listenerBatch).deep.equal({ delivered: 2000, rejected: 0, errors: [], admittedInOrder: true })
          // 320 MiB of 64 KiB messages.
          expect(pastTheBound.delivered).greaterThan(4900)
          expect(pastTheBound.delivered).lessThan(5121)
          expect(pastTheBound.delivered + pastTheBound.rejected).toBe(6000)
          expect(pastTheBound.errors).deep.equal(['ChannelOverflowError'])
          expect(pastTheBound.admittedInOrder).toBe(true)
          expect(aloneBytes).toBe(8 * 1024 * 1024)
          expect(producers).deep.equal({ received: 8000, rejected: 0 })
          expect(timerProducer).deep.equal({ sent: 30 * 4096, peakWaiting: 0, delivered: 30 * 4096 })
          expect(timerFiredMidChain).toBe(true)
          expect(awaitedLoopEnded).toBe(true)
          expect(answeringListenerEnded).toBe(true)
        },
        { timeout: 30_000 },
      )
    })
  }

  test('broadcast: publish rejections stay silent whether caught or ignored', async () => {
    const reports: string[] = []
    const onConsole = (message: ConsoleMessage) => {
      if (message.text().includes('[telefunc:channel-error]')) reports.push(message.text())
    }
    page.on('console', onConsole)
    try {
      await navigate(`${getServerUrl()}/publish`)
      await page.click('#test-broadcast-publish-failures')
      await autoRetry(async () => {
        expect(await getResult('#publish-result')).deep.equal(['ignored', 'stop', 'ignored', 'stop'])
      })
      expect(reports).deep.equal([])
    } finally {
      page.off('console', onConsole)
    }
  })

  test('broadcast: iteration preserves publish info and signals remove subscriptions on both ends', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-api')
    await autoRetry(async () => {
      const result = await getResult<any>('#publish-result')
      const info = result.receipts.map(({ key, seq, timestamp }: any) => ({ key, seq, timestamp }))
      expect(info.map(({ seq }: any) => seq)).deep.equal([info[0].seq, info[0].seq + 1])
      expect(info.every(({ key, timestamp }: any) => /^room:api:/.test(key) && timestamp > 0)).toBe(true)
      const messages = [
        { type: 'text', data: 'first', info: info[0] },
        { type: 'binary', data: [2], info: info[1] },
      ]
      const expected = { messages, callbacks: messages, cancelled: 'stop', preAborted: 'skipped', skippedCalls: 0 }
      expect(result.client).deep.equal(expected)
      expect(result.server).deep.equal(expected)
      expect(result.snapshot).deep.equal({ callbacks: messages, staticCalls: 2 })
      expect(result.open).toBe(true)
      expect(result.ended).toBe(true)
    })
  })

  test('broadcast: paired text publish/subscribe roundtrip', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-text-broadcast')

    await autoRetry(async () => {
      const result = await getResult<TextBroadcastResult>('#publish-result')

      // 3 publishes → 3 acks with consecutive seqs, all keyed to the shared topic.
      expect(result.acks.length).toBe(3)
      const seqs = consecutive(result.acks[0]!.seq)
      expect(result.acks.map((a) => a.seq)).deep.equal(seqs)
      for (const ack of result.acks) expect(ack.key).match(/^room:text-test:/)

      // Subscriber on the same key received all 3 messages in publish order.
      expect(result.received.map((r) => r.text)).deep.equal(['msg-0', 'msg-1', 'msg-2'])
      for (const r of result.received) expect(r.from).toBe('client')
      expect(result.received.map((r) => r.seq)).deep.equal(seqs)
    })
  })

  test('broadcast: paired binary publish/subscribe roundtrip', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-binary-broadcast-pair')

    await autoRetry(async () => {
      const result = await getResult<BinaryPairResult>('#publish-result')

      expect(result.acks.length).toBe(3)
      const seqs = consecutive(result.acks[0]!.seq)
      expect(result.acks.map((a) => a.seq)).deep.equal(seqs)
      for (const ack of result.acks) expect(ack.key).match(/^room:binary-test:/)

      // Each frame is 128 bytes filled with i + 10 (10, 11, 12).
      expect(result.received.length).toBe(3)
      expect(result.received.map((r) => r.size)).deep.equal([128, 128, 128])
      expect(result.received.map((r) => r.firstByte)).deep.equal([10, 11, 12])
      expect(result.received.map((r) => r.seq)).deep.equal(seqs)
    })
  })

  test('broadcast: server publishes binary frames, client subscribes', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-binary-broadcast')

    await autoRetry(async () => {
      const result = await getResult<ServerBroadcastResult>('#publish-result')

      // Server publishes 5 frames, each 64 bytes filled with i + 1 (1..5).
      expect(result.done).toBe(true)
      expect(result.received.length).toBe(5)
      expect(result.received.map((r) => r.size)).deep.equal([64, 64, 64, 64, 64])
      expect(result.received.map((r) => r.firstByte)).deep.equal([1, 2, 3, 4, 5])
    })
  })

  test('broadcast: binary publishes go out as they were at the call, though the server refills one buffer for each while a subscription establishes', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-binary-broadcast-reused-buffer')

    await autoRetry(async () => {
      const result = await getResult<{ received: number[][]; seen: number[][] }>('#publish-result')
      const frames = [
        [1, 1, 1, 1],
        [2, 2, 2, 2],
        [3, 3, 3, 3],
      ]
      expect(result.received).deep.equal(frames)
      expect(result.seen).deep.equal(frames)
    })
  })

  test('broadcast: publishes right after a subscribe reach it, text and binary in call order', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-right-after')

    await autoRetry(async () => {
      const result = await getResult<{ seen: unknown[] }>('#publish-result')
      expect(result.seen).deep.equal(['t1', [1], 't2'])
    })
  })

  test("broadcast: a listener's answer arrives after the message it answers; a listener answering every message lets timers fire; an awaited publish is delivered when it resolves on the in-memory backend, and every one is delivered on Redis", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-answers')

    await autoRetry(async () => {
      const result = await getResult<{
        received: string[]
        timerFired: boolean
        deliveredAtLastReceipt: number
        delivered: number
      }>('#publish-result')
      expect(result.received[0]).toBe('ping')
      expect([...result.received].sort()).deep.equal(['async-pong', 'ping', 'pong'])
      expect(result.timerFired).toBe(true)
      if (inDocker) expect(result.delivered).toBe(3000)
      else expect(result.deliveredAtLastReceipt).toBe(3000)
    })
  })

  test('broadcast: shield validates client→server publishes', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-shield')

    await autoRetry(async () => {
      const state = await getResult<ShieldState>('#publish-shield-state')

      // Valid publish: server-side shield accepts → subscribers see it, publish receipt has the topic key.
      expect(state.validReceiptKey).match(/^shield-test:/)
      expect(state.validReceived).deep.equal([{ text: 'hi' }])

      // Invalid publish: server-side shield rejects → publish() rejects, subscribers do NOT see the bad message.
      expect(state.invalidThrew).toBe(true)
      expect(state.invalidErrorMessage).not.toBe(null)
      expect(state.receivedAfterInvalid).deep.equal([{ text: 'hi' }])
    })
  })

  test('broadcast: what a channel publishes from its onClose reaches its key, as the chat pattern does', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-chat-leave-on-close')
    await autoRetry(async () => {
      expect(await getResult<{ received: string[] }>('#publish-result')).deep.equal({ received: ['left'] })
    })
  })

  test('broadcast: a subscribe after abort opens nothing, and an unsubscribe stops deliveries', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-server-broadcast-checks')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({
        afterAbort: [],
        received: ['leaving', 'joining'],
        statically: ['first'],
      })
    })
  })

  test("broadcast: each binary listener gets bytes of its own, so one that transfers their buffer leaves the next one's and the page's intact", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-own-bytes')

    await autoRetry(async () => {
      const result = await getResult<{ page: unknown[]; server: unknown[] }>('#publish-result')
      expect(result).deep.equal({
        page: [[1, 2, 3]],
        server: [
          [1, 2, 3],
          [1, 2, 3],
        ],
      })
    })
  })

  test("broadcast: the server's own listeners keep getting a key's messages after the page unsubscribes", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-server-listener')

    await autoRetry(async () => {
      const result = await getResult<{ received: Array<string | number> }>('#publish-result')
      expect(result.received).deep.equal(['after', 3])
    })
  })

  test('broadcast: what onOpen publishes reaches the page that opened the broadcast', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-on-open-publish')

    await autoRetry(async () => {
      const result = await getResult<{ received: string[] }>('#publish-result')
      expect(result.received).deep.equal(['from-onOpen'])
    })
  })

  test('broadcast: a subscriber that aborts its channel in its callback leaves the publish to reach the rest', async () => {
    await page.click('#test-broadcast-subscriber-aborts')
    await autoRetry(async () => {
      const result = await getResult<{ seq: number; received: string[] }>('#publish-result')
      expect(result.seq).greaterThan(0)
      expect(result.received).deep.equal(['abort'])
    })
  })

  test("broadcast: what a loop that stopped reading queued isn't held once its channel closed", async () => {
    await page.click('#test-broadcast-loop-left-behind')
    await autoRetry(async () => {
      const result = await getResult<{ held: number; heldOnceClosed: number }>('#publish-result')
      expect(result.held).greaterThan(0)
      expect(result.heldOnceClosed).toBe(0)
    })
  })

  test("broadcast: a listener's answer reaches every subscriber after the message it answers", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-answer-order')

    await autoRetry(async () => {
      const result = await getResult<{ seen: string[] }>('#publish-result')
      expect(result.seen).deep.equal(['first:1', 'second:1', 'first:2', 'second:2'])
    })
  })

  test('broadcast: the server answers the next call while listeners answer every message on their own key', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-self-answer')

    await autoRetry(async () => {
      const result = await getResult<{ answers: number }>('#publish-result')
      expect(result.answers).greaterThan(0)
    })
  })

  test('broadcast: a static Broadcast.publish() resolves to a receipt with its key', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-static-receipts')

    await autoRetry(async () => {
      const result = await getResult<{ key: string; receipts: Array<{ key: string; seq: number }> }>('#publish-result')
      const [first, second] = result.receipts
      expect(result.receipts.map((receipt) => receipt.key)).deep.equal([result.key, result.key])
      // `seq` is monotonic per key.
      expect(second!.seq).greaterThan(first!.seq)
    })
  })

  test("broadcast: a Broadcast.subscribe() listener that throws or rejects is reported, one that throws Abort isn't, and the key's other subscribers still get the message", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-throwing-listener')

    await autoRetry(async () => {
      const result = await getResult<{ received: string[]; receiptKey: boolean }>('#publish-result')
      expect(result).deep.equal({ received: ['static:hello', 'channel:hello'], receiptKey: true })
    })
    for (const error of ['broadcast-listener-throws', 'broadcast-listener-rejects']) {
      expectLog(error, {
        // docker compose prints a server's stderr on its stdout, each line prefixed with the container's name.
        filter: (log) =>
          inDocker
            ? log.logSource === 'stdout' &&
              log.logText.split('\n').some((line) => line.includes(error) && /^playground-[a-z]-\d+ +\| /.test(line))
            : log.logSource === 'stderr',
      })
    }
  })

  test('broadcast: in a chat whose saving, history, avatar or mark-as-read fails, the error is reported and the next message still arrives', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await resetCleanupState()
    await page.click('#test-chat')

    await autoRetry(async () => {
      expect(await getResult<{ messages: string[] }>('#publish-result')).deep.equal({ messages: ['how are you?'] })
    })
    await autoRetry(async () => {
      const state = await getCleanupState()
      expect(state.chatSaved).toBe(JSON.stringify(['how are you?']))
      expect(state['bug:Database unavailable']).toBe('reported')
      expect(state['bug:Could not load the chat history']).toBe('reported')
    })
    for (const error of ['Database unavailable', 'Could not load the chat history']) {
      expectLog(error, {
        // docker compose prints a server's stderr on its stdout, each line prefixed with the container's name.
        filter: (log) =>
          inDocker
            ? log.logSource === 'stdout' &&
              log.logText.split('\n').some((line) => line.includes(error) && /^playground-[a-z]-\d+ +\| /.test(line))
            : log.logSource === 'stderr',
      })
    }
    for (const error of ['Could not load the avatar', 'Could not mark the chat as read']) {
      expectLog(`[telefunc:channel-error] Error: ${error}`, { filter: (log) => log.logSource === 'Browser Error' })
    }
  })

  test("broadcast: a key's seq goes on growing after its subscribers leave and another key publishes, and runs on by one while it has a subscriber", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-seq-across-subscribers')

    await autoRetry(async () => {
      const { seqs } = await getResult<{ seqs: number[] }>('#publish-result')
      expect(seqs.length).toBe(5)
      expect(seqs[1]).toBe(seqs[0]! + 1)
      for (let i = 1; i < seqs.length; i++) expect(seqs[i]).greaterThan(seqs[i - 1]!)
    })
  })

  // The in-memory backend's: Redis delivers what reaches a key's subscription once it is there.
  if (!inDocker && !process.env.REDIS_URL) {
    test('broadcast: a subscriber that comes while publishes queue gets only what is published after it, text and binary alike', async () => {
      await navigate(`${getServerUrl()}/publish`)
      await page.click('#test-broadcast-join-while-queued')

      await autoRetry(async () => {
        expect((await getResult<{ received: Array<string | number> }>('#publish-result')).received).deep.equal(['t4'])
      })
    })
  }

  // Preview on Node only: the margin is measured there. A leak of 100,000 keys' seq counters grows the heap by 7 MB or more.
  if (!isDev && !inDocker && !process.env.REDIS_URL && serverRuntime() === 'node') {
    for (const subscribed of [false, true]) {
      test(`broadcast: publishes to 100,000 distinct keys, ${subscribed ? 'each while a subscriber comes and goes' : 'none subscribed'}, leave the server's heap where it was`, async () => {
        await navigate(`${getServerUrl()}/publish`)
        const run = async () => {
          await page.click(`#test-broadcast-many-keys${subscribed ? '-subscribed' : ''}`)
          await autoRetry(async () =>
            expect(await getResult<{ done: boolean }>('#publish-result')).deep.equal({ done: true }),
          )
        }
        // A first run warms up what the code allocates once.
        await run()
        await forceServerGc()
        const before = await serverHeapUsed()
        await run()
        await forceServerGc()
        expect((await serverHeapUsed()) - before).lessThan(3_000_000)
      })
    }
  }

  test("broadcast: a subscriber that unsubscribes itself doesn't make the next one miss a message", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-self-unsubscribe')

    await autoRetry(async () => {
      const result = await getResult<{ pageSeen: string[]; serverSeen: string[] }>('#publish-result')
      expect(result.pageSeen).deep.equal(['once:one', 'other:one', 'other:two'])
      expect(result.serverSeen).deep.equal(['once:one', 'other:one', 'other:two'])
    })
  })

  // Preview only: Vite HMR reloads an offline page.
  if (!isDev && !inDocker) {
    test('broadcast: a page that subscribes while its connection is silently dead gets what is published after it reconnects', async () => {
      await navigate(`${getServerUrl()}/publish`)
      await page.click('#test-broadcast-reconnect-open')
      await autoRetry(async () => {
        expect((await getResult<{ open: boolean }>('#publish-result')).open).toBe(true)
      })

      await page.context().setOffline(true)
      await page.click('#test-broadcast-reconnect-subscribe')
      await page.waitForTimeout(3000)
      await page.context().setOffline(false)
      await page.click('#test-broadcast-reconnect-publish')

      await autoRetry(async () => {
        const result = await getResult<{ received: string[] }>('#publish-result')
        expect(result.received).deep.equal(['after-reconnect'])
      })
    })

    for (const count of [2, 32]) {
      if (canCutWire())
        test(`broadcast: a page offline while ${count * 128} KiB is published gets all of it once back`, async () => {
          await navigate(`${getServerUrl()}/publish`)
          await page.click(`#test-broadcast-while-offline-${count}`)
          let key: string | null = null
          await autoRetry(async () => {
            const result = await getResult<{ key: string; received: string[] }>('#publish-result')
            expect(result.received).deep.equal(['before'])
            key = result.key
          })

          await page.context().setOffline(true)
          await cutWire()
          await runServerAction(`publish_${key}`)
          await page.context().setOffline(false)

          await autoRetry(async () => {
            const result = await getResult<{ received: string[]; closed: string | null }>('#publish-result')
            expect(result.received).deep.equal(['before', ...Array.from({ length: count }, (_, n) => String(n))])
            expect(result.closed).toBe(null)
          })
        })
    }
  }
}

/** Tests that need the failing backend (failing-backend.ts), which a server without Redis installs. */
function testPublishFailingBackend() {
  test('broadcast: a subscribe the store refuses is reported once, and the channel subscribes again', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await failBackend({ op: 'subscribe', key: 'room:subscribe-refused:' })
    await page.click('#test-subscribe-refused')

    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({ seen: ['first:1'] })
    })
    expectLog('Backend subscription closed: text:room%3Asubscribe-refused%3A', {
      filter: (log) => log.logSource === 'stderr',
    })
  })

  test('broadcast: a message the store delivers before it acknowledges the subscription reaches the listener, before the next', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await failBackend({ op: 'early', key: 'room:delivered-before-ready:' })
    await page.click('#test-delivered-before-ready')

    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({ seen: [1, 2] })
    })
  })

  test('broadcast: a message the store delivers before it acknowledges the subscription reaches only the listeners subscribed when it was delivered', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-joined-before-ready')

    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({ first: [1, 2], second: [2] })
    })
  })

  test('broadcast: a message the store delivers while the subscription is lost reaches the listener as it arrives', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await failBackend({ op: 'lost', key: 'room:delivered-while-lost:' })
    await page.click('#test-delivered-while-lost')

    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({ seen: [1, 2] })
    })
  })

  test('broadcast: a subscriber that throws or rejects is reported as a bug and the others get every message, and an un-awaited publish the store fails leaves no unhandled rejection', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-errors')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({
        seen: ['a', 'b'],
        reported: [
          'broadcast-listener-bug: rejected',
          'broadcast-listener-bug: rejected',
          'broadcast-listener-bug: thrown',
          'broadcast-listener-bug: thrown',
        ],
        unhandled: 0,
        storeFailedBoth: true,
      })
    })
  })
}

/** Runs on Redis. */
function testPublishHold() {
  test('broadcast: publishes held while a subscription establishes fit 2 MiB by their bytes, and the one past it rejects with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-hold-overflow')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({
        outcomes: ['sent', 'sent', 'ChannelOverflowError'],
        seen: ['1', '2'],
      })
    })
  })
  test('broadcast: a publish held alone goes through past 2 MiB, and a second one rejects with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-hold-alone')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({ outcomes: ['sent', 'ChannelOverflowError'], seen: ['1'] })
    })
  })
}
