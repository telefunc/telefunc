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
  snapshotChanged: boolean
  snapshotStable: boolean
  changes: number
}

type ParticipantResult = {
  received: Array<{ text: string; from: string }>
  count: number
  remoteMetaName: string | null
  localMetaName: string | null
  localIdentity: string | null
  remoteIdentity: string | null
  dms: Array<{ data: string; fromAlly: boolean }>
}

type BinaryResult = {
  frames: Array<{ size: number; firstByte: number; fromSelf: boolean; track: string | null; keyFrame: boolean }>
  cameraOnly: number[]
  defaultOnly: number[]
  camReceivers: number
}

type GuardResult = {
  joinError: string | null
  publishError: string | null
  sendError: string | null
  received: string[]
  inbox: string[]
}

type AdminResult = {
  announcements: string[]
  system: Array<{ data: string; fromRoom: boolean }>
  kicked: boolean
  kickCause: { type: string; reason?: string } | null
  closed: boolean
  isClosed: boolean
  count: number
}

type MemberResult = {
  hasMember: boolean
  memberName: string | null
  sameObject: boolean
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

      // snapshot()/onChange: reference-stable until change; every change signaled.
      expect(result.snapshotChanged).toBe(true)
      expect(result.snapshotStable).toBe(true)
      expect(result.changes).greaterThan(0)
    })
  })

  test('room: server-joined participant publishes, updates metadata, receives a DM', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-participant')

    await autoRetry(async () => {
      const result = await getResult<ParticipantResult>('#room-result')

      expect(result.received).deep.equal([{ text: 'from-bob', from: 'Bob' }]) // the DM never hit the room stream
      expect(result.count).toBe(2) // Bob + Ally
      // Identity: stamped at the server-side join, visible locally and on the remote view.
      expect(result.localIdentity).toBe('user:Bob')
      expect(result.remoteIdentity).toBe('user:Bob')
      // setMeta propagated both to the room's remote view and back to the participant stub.
      expect(result.remoteMetaName).toBe('Bobby')
      expect(result.localMetaName).toBe('Bobby')
      // The DM reached the standalone participant's inbox, with sender identity.
      expect(result.dms).deep.equal([{ data: 'psst', fromAlly: true }])
    })
  })

  test('room: binary frames round-trip in isolated mode', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-binary')

    await autoRetry(async () => {
      const result = await getResult<BinaryResult>('#room-result')

      // 3 default-track frames + 1 named camera keyframe — all attributed to the publisher.
      expect(result.frames.length).toBe(4)
      expect(result.frames.map((f) => f.size)).deep.equal([64, 64, 64, 32])
      expect(result.frames.map((f) => f.firstByte)).deep.equal([1, 2, 3, 9])
      for (const frame of result.frames) expect(frame.fromSelf).toBe(true)
      expect(result.frames.map((f) => f.track)).deep.equal([null, null, null, 'camera'])
      expect(result.frames.map((f) => f.keyFrame)).deep.equal([false, false, false, true])
      // Track-filtered subscriptions saw only their stream — `{ track: null }` is the default lane.
      expect(result.cameraOnly).deep.equal([9])
      expect(result.defaultOnly).deep.equal([1, 2, 3])
      // The publish ack reports the track's live subscription count (the pause-at-0 signal).
      expect(result.camReceivers).greaterThanOrEqual(1)
    })
  })

  test('room: a returned RemoteParticipant is the same object as the room view', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-member')

    await autoRetry(async () => {
      const result = await getResult<MemberResult>('#room-result')
      expect(result.hasMember).toBe(true)
      expect(result.memberName).toBe('Viewed')
      expect(result.sameObject).toBe(true) // ref-identity binds the view to its room
    })
  })

  test('room: guards reject over the wire; allowed messages flow', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-guard')

    await autoRetry(async () => {
      const result = await getResult<GuardResult>('#room-result')

      // The rejection carries the guard's error back through the wire ack.
      expect(result.joinError).toBe('blocked join of Banned')
      expect(result.publishError).toBe('blocked publish from Mallory')
      expect(result.sendError).toBe('blocked send from Mallory')
      // Guarded-out messages never delivered; allowed ones flow.
      expect(result.received).deep.equal(['fine'])
      expect(result.inbox).deep.equal(['psst'])
    })
  })

  test('room: room-authored messages, admin kick and close reach the client', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-admin')

    await autoRetry(async () => {
      const result = await getResult<AdminResult>('#room-result')

      // Room-authored: Room.announce() landed on onAnnounce, Room.send() on listen with from null.
      expect(result.announcements).deep.equal(['maintenance'])
      expect(result.system).deep.equal([{ data: 'welcome', fromRoom: true }])

      expect(result.kicked).toBe(true) // LocalParticipant.onLeave fired on removeParticipant()
      expect(result.kickCause).deep.equal({ type: 'removed', reason: 'be nice' }) // the reason rode the removal
      expect(result.closed).toBe(true) // Room.onClose fired on close()
      expect(result.isClosed).toBe(true)
      expect(result.count).toBe(0)
    })
  })
}
