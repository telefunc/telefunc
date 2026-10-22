export { testPublish, testPublishSmallBuffer, testPublishFailingBackend }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import {
  navigate,
  getResult,
  failBackend,
  getCleanupState,
  cutWire,
  canCutWire,
  runServerAction,
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

  test("broadcast: each binary listener gets bytes of its own, so one that writes into them or transfers them leaves the next one's and the page's intact", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-binary-broadcast-own-bytes')

    await autoRetry(async () => {
      const result = await getResult<{ received: number[][]; seen: number[][]; keySeen: number[][] }>('#publish-result')
      expect(result.received).deep.equal([[1, 2, 3, 4]])
      expect(result.seen).deep.equal([[1, 2, 3, 4]])
      expect(result.keySeen).deep.equal([[1, 2, 3, 4]])
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

  test('broadcast: a subscribe after abort opens nothing, receivers counts the subscribers a publish reached, and an unsubscribe stops deliveries', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-server-broadcast-checks')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({
        afterAbort: [0, 0],
        received: ['leaving', 'joining'],
        duringDelivery: [1, 1],
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

    for (const { count, received, closed } of [
      { count: 2, received: ['before', '0', '1'], closed: null },
      { count: 8, received: ['before'], closed: 'ChannelOverflowError' },
    ]) {
      if (canCutWire())
        test(`broadcast: a page offline while ${count * 128} KiB is published ${closed ? 'leaves with ChannelOverflowError on both ends' : 'gets all of it once back'}`, async () => {
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
            expect(result.received).deep.equal(received)
            expect(result.closed).toBe(closed)
            if (closed) expect((await getCleanupState())[`whileOffline_${key}_serverClose`]).toBe(closed)
          })
        })
    }
  }
}

/** Tests that need the failing backend (failing-backend.ts), which a server without Redis installs. */
function testPublishFailingBackend() {
  test('broadcast: a subscribe the store refuses throws and leaves no listener behind', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await failBackend({ op: 'bind', key: 'room:subscribe-throws:' })
    await page.click('#test-subscribe-throws')

    await autoRetry(async () => {
      const result = await getResult<{ thrown: string | null; seen: string[] }>('#publish-result')
      expect(result.thrown).toBe('failing-backend: bind refused')
      expect(result.seen).deep.equal(['second:1'])
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

/** Runs in a lane whose config.channel.bufferLimit is 4,096 bytes. */
function testPublishSmallBuffer() {
  test('broadcast: publishes held while a subscription establishes fit bufferLimit by their bytes, and the one past it rejects with ChannelOverflowError', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-hold-overflow')
    await autoRetry(async () => {
      expect(await getResult('#publish-result')).deep.equal({
        outcomes: ['sent', 'sent', 'ChannelOverflowError'],
        seen: ['1', '2'],
      })
    })
  })
}
