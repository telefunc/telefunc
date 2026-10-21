export { testPublish }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

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

  test("broadcast: a listener's answer arrives after the message it answers; a listener answering every message lets timers fire; an awaited publish is delivered when it resolves", async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-answers')

    await autoRetry(async () => {
      const result = await getResult<{ received: string[]; timerFired: boolean; deliveredAtLastReceipt: number }>(
        '#publish-result',
      )
      expect(result.received[0]).toBe('ping')
      expect([...result.received].sort()).deep.equal(['async-pong', 'ping', 'pong'])
      expect(result.timerFired).toBe(true)
      expect(result.deliveredAtLastReceipt).toBe(3000)
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
}
