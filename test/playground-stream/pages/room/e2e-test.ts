export { testRoom }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

type ChatResult = {
  events: string[]
  received: Array<{ text: string; from: string }>
  updates: Array<[number, number | undefined]>
  countAfterJoin: number
  count: number
  ack: { key: string; seq: number }
}

type ParticipantResult = {
  received: Array<{ text: string; from: string }>
  count: number
  remoteMetaName: string | null
  localMetaName: string | null
}

type BinaryResult = {
  frames: Array<{ size: number; firstByte: number; fromSelf: boolean }>
}

type AdminResult = {
  kicked: boolean
  closed: boolean
  isClosed: boolean
  count: number
}

function testRoom() {
  test('room: join, publish with sender identity, setMeta, leave', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-chat')

    await autoRetry(async () => {
      const result = await getResult<ChatResult>('#room-result')

      // Presence: own join and leave announced, count tracked live.
      expect(result.events).deep.equal(['join:Alice', 'leave:Alice'])
      expect(result.countAfterJoin).toBe(1)
      expect(result.count).toBe(0)

      // Data: received with sender identity; the receipt is keyed to the room ID.
      expect(result.received).deep.equal([{ text: 'hello', from: 'Alice' }])
      expect(result.ack.key).match(/^e2e-chat:/)
      expect(result.ack.seq).greaterThan(0)

      // Metadata: the remote view saw the score change (undefined -> 42).
      expect(result.updates).deep.equal([[42, null]])
    })
  })

  test('room: server-joined participant publishes and updates metadata', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-participant')

    await autoRetry(async () => {
      const result = await getResult<ParticipantResult>('#room-result')

      expect(result.received).deep.equal([{ text: 'from-bob', from: 'Bob' }])
      expect(result.count).toBe(1)
      // setMeta propagated both to the room's remote view and back to the participant stub.
      expect(result.remoteMetaName).toBe('Bobby')
      expect(result.localMetaName).toBe('Bobby')
    })
  })

  test('room: binary frames round-trip in isolated mode', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-binary')

    await autoRetry(async () => {
      const result = await getResult<BinaryResult>('#room-result')

      // 3 frames, 64 bytes each, filled with 1..3 — all attributed to the publisher.
      expect(result.frames.length).toBe(3)
      expect(result.frames.map((f) => f.size)).deep.equal([64, 64, 64])
      expect(result.frames.map((f) => f.firstByte)).deep.equal([1, 2, 3])
      for (const frame of result.frames) expect(frame.fromSelf).toBe(true)
    })
  })

  test('room: admin kick and close reach the client', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-admin')

    await autoRetry(async () => {
      const result = await getResult<AdminResult>('#room-result')

      expect(result.kicked).toBe(true) // LocalParticipant.onLeave fired on removeParticipant()
      expect(result.closed).toBe(true) // Room.onClose fired on close()
      expect(result.isClosed).toBe(true)
      expect(result.count).toBe(0)
    })
  })
}
