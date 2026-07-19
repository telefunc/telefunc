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
  frames: Array<{
    size: number
    firstByte: number
    fromSelf: boolean
    track: string | null
    meta: Record<string, unknown> | null
  }>
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

  test('room: binary frames round-trip', async () => {
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
      expect(result.frames.map((f) => f.meta)).deep.equal([null, null, null, { key: true }])
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

  test('room: the declared message type is shielded at runtime — a malformed publish is rejected', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-shield')

    await autoRetry(async () => {
      const r = await getResult<{ okAck: boolean; badError: string | null; received: string[] }>('#room-result')

      expect(r.okAck).toBe(true) // the well-typed payload is admitted
      // The shield auto-generated from `Room<…, ChatMsg>` rejects the malformed payload at the ingress;
      // the branded error rides home over the wire and rejects the client's `publish()` promise.
      expect(r.badError).toBe('ShieldValidationError')
      expect(r.received).deep.equal(['hi']) // only the valid payload ever reached the room
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

  test('room: coalesce conflates a same-key burst to first + latest', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-conflate')

    await autoRetry(async () => {
      const r = await getResult<{ received: number[]; acked: number; allSeqs: boolean }>('#room-result')
      expect(r.received).deep.equal([1, 5]) // 2..4 collapsed into the single pending slot
      expect(r.acked).toBe(5) // every caller's promise still resolves...
      expect(r.allSeqs).toBe(true) // ...with the winning send's ack
    })
  })

  test('room: setAttributes merges per key and deletes on undefined', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-attributes')

    await autoRetry(async () => {
      const r = await getResult<{
        name: string | null
        title: string | null
        hasScore: boolean
        localName: string | null
        localHasScore: boolean
      }>('#room-result')
      expect(r.name).toBe('Zoe') // untouched key preserved across merges
      expect(r.title).toBe('lead') // added key
      expect(r.hasScore).toBe(false) // score removed by `undefined`
      expect(r.localName).toBe('Zoe') // the local handle reflects the merge too
      expect(r.localHasScore).toBe(false)
    })
  })

  test('room: onDemand turns on when a subscriber wants a track and off when it leaves', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-demand')

    await autoRetry(async () => {
      const r = await getResult<{ cam: boolean[] }>('#room-result')
      expect(r.cam).toContain(true) // a viewer arrived (the track is wanted)
      expect(r.cam[r.cam.length - 1]).toBe(false) // ...and left again — back to unwanted
    })
  })

  test('room: Room.get({ tail }) holds a between-get-and-subscribe message', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-tail')

    await autoRetry(async () => {
      const r = await getResult<{ received: string[] }>('#room-result')
      expect(r.received).toContain('between') // relay started at serialize, buffered until subscribe
    })
  })

  test('room: onAfterJoin/Publish/Send fire with authoritative receipts', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-hooks')

    await autoRetry(async () => {
      const r = await getResult<{
        joins: string[]
        joinHasTs: boolean
        publish: { name: string; data: unknown; seqOk: boolean } | null
        send: { name: string; to: string; seqOk: boolean } | null
      }>('#room-result')
      expect(r.joins).deep.equal(['A', 'B']) // onAfterJoin fired for both grants
      expect(r.joinHasTs).toBe(true) // receipt carries joinedAt
      expect(r.publish).deep.equal({ name: 'A', data: 'hello', seqOk: true }) // onAfterPublish + seq
      expect(r.send).deep.equal({ name: 'A', to: 'B', seqOk: true }) // onAfterSend + seq
    })
  })

  test('room: a per-member subscription receives only that member', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-member-sub')

    await autoRetry(async () => {
      const r = await getResult<{ xText: string[]; xBin: number[]; all: string[] }>('#room-result')
      expect(r.all).deep.equal(['x1', 'y1']) // both delivered room-wide (so absence below is meaningful)
      expect(r.xText).deep.equal(['x1']) // the per-member text sub saw only X
      expect(r.xBin).deep.equal([7]) // per-member binary is selective too
    })
  })

  test('room: a DM sent before listen() is held and flushed on attach', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-dm-hold')

    await autoRetry(async () => {
      const r = await getResult<{ held: string[] }>('#room-result')
      expect(r.held).deep.equal(['early'])
    })
  })

  test('room: selfDelivery:false suppresses your own frames locally, not for others', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-self')

    await autoRetry(async () => {
      const r = await getResult<{ mine: string[]; theirs: string[]; selfDelivery: boolean }>('#room-result')
      expect(r.selfDelivery).toBe(false)
      expect(r.theirs).deep.equal(['hi']) // others receive it
      expect(r.mine).deep.equal([]) // you don't
    })
  })

  test('room: a co-returned server-side selfDelivery:false member is suppressed at the source, while a client join on the same stub is delivered', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-self-server')

    await autoRetry(async () => {
      const r = await getResult<{ mine: string[]; theirs: string[]; selfDelivery: boolean }>('#room-result')
      expect(r.selfDelivery).toBe(false)
      expect(r.theirs).deep.equal(['from-me', 'from-notme']) // the observer receives both
      expect(r.mine).deep.equal(['from-notme']) // own co-return echo suppressed; client join delivered
    })
  })

  test('room: Room.setMeta propagates; list and getOrCreate resolve', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-reconfig')

    await autoRetry(async () => {
      const r = await getResult<{
        updates: string[]
        topic: string | null
        listed: string[]
        sameId: boolean
        sameCount: number
      }>('#room-result')
      expect(r.updates).toContain('updated') // room.onUpdate fired
      expect(r.topic).toBe('updated') // room.meta reflects it
      expect(r.listed.length).toBe(2) // both prefixed rooms enumerated by Room.list
      expect(r.sameId).toBe(true) // getOrCreate returned the existing room
      expect(r.sameCount).greaterThanOrEqual(1) // ...with its member preserved
    })
  })

  test('room: removeParticipant({ identity }) kicks; onEmpty fires', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-identity')

    await autoRetry(async () => {
      const r = await getResult<{ cause: { type: string; reason?: unknown } | null; count: number; empty: boolean }>(
        '#room-result',
      )
      expect(r.cause).deep.equal({ type: 'removed', reason: 'multi-tab' }) // kicked by identity, reason rode along
      expect(r.count).toBe(0)
      expect(r.empty).toBe(true) // onEmpty fired when the last member went
    })
  })
}
