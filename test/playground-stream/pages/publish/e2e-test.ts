export { testPublish, testPublishFailingBackend }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, failBackend } from '../../e2e-utils'

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

function testPublish() {
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
