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
function testRoomScenario<Result>(
  id: string,
  name: string,
  validate: (result: Result) => void | Promise<void>,
  after?: () => Promise<void>,
) {
  test(name, async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click(`#test-room-${id}`)
    await autoRetry(async () => validate(await getResult<Result>('#room-result')))
    await after?.()
  })
}
function testRoom() {
  testRoomScenario<ChatResult>('chat', 'room: join, publish with sender identity, setMeta, leave', (result) => {
    expect(result.events).deep.equal(['join:Alice', 'leave:Alice'])
    expect(result.countAfterJoin).toBe(1)
    expect(result.count).toBe(0)
    expect(result.received).deep.equal([{ text: 'hello', from: 'Alice' }])
    expect(result.ack.key).match(/^e2e-chat:/)
    expect(result.ack.seq).greaterThan(0)
    expect(result.updates).deep.equal([[42, null]])
    expect(result.snapshotChanged).toBe(true)
    expect(result.snapshotStable).toBe(true)
    expect(result.changes).greaterThan(0)
  })
  testRoomScenario<{ received: string[] }>(
    'retain',
    'room: a retained message reaches a subscriber that joins after it was published',
    (r) => {
      // Retained replay begins only after subscription readiness.
      expect(r.received).deep.equal(['pinned'])
    },
  )
  testRoomScenario<ParticipantResult>(
    'participant',
    'room: server-joined participant publishes, updates metadata, receives a DM',
    (result) => {
      expect(result.received).deep.equal([{ text: 'from-bob', from: 'Bob' }]) // the DM never hit the room stream
      expect(result.count).toBe(2) // Bob + Ally
      expect(result.localIdentity).toBe('user:Bob')
      expect(result.remoteIdentity).toBe('user:Bob')
      expect(result.remoteMetaName).toBe('Bobby')
      expect(result.localMetaName).toBe('Bobby')
      expect(result.dms).deep.equal([{ data: 'psst', fromAlly: true }])
    },
  )
  testRoomScenario<BinaryResult>('binary', 'room: binary frames round-trip', (result) => {
    expect(result.frames.length).toBe(4)
    expect(result.frames.map((f) => f.size)).deep.equal([64, 64, 64, 32])
    expect(result.frames.map((f) => f.firstByte)).deep.equal([1, 2, 3, 9])
    for (const frame of result.frames) expect(frame.fromSelf).toBe(true)
    expect(result.frames.map((f) => f.track)).deep.equal([null, null, null, 'camera'])
    expect(result.frames.map((f) => f.meta)).deep.equal([null, null, null, { key: true }])
    expect(result.cameraOnly).deep.equal([9])
    expect(result.defaultOnly).deep.equal([1, 2, 3])
    expect(result.camReceivers).greaterThanOrEqual(1)
  })
  testRoomScenario<MemberResult>(
    'member',
    'room: a returned RemoteParticipant is the same object as the room view',
    (result) => {
      expect(result.hasMember).toBe(true)
      expect(result.memberName).toBe('Viewed')
      expect(result.sameObject).toBe(true) // ref-identity binds the view to its room
    },
  )
  testRoomScenario<GuardResult>('guard', 'room: guards reject over the wire; allowed messages flow', (result) => {
    expect(result.joinError).toBe('blocked join of Banned')
    expect(result.publishError).toBe('blocked publish from Mallory')
    expect(result.sendError).toBe('blocked send from Mallory')
    expect(result.received).deep.equal(['fine'])
    expect(result.inbox).deep.equal(['psst'])
  })
  testRoomScenario<{ okAck: boolean; badError: string | null; received: string[] }>(
    'shield',
    'room: the declared message type is shielded at runtime — a malformed publish is rejected',
    (r) => {
      expect(r.okAck).toBe(true) // the well-typed payload is admitted
      // The generated shield rejects malformed ingress through the publish promise.
      expect(r.badError).toBe('ShieldValidationError')
      expect(r.received).deep.equal(['hi']) // only the valid payload ever reached the room
    },
  )
  testRoomScenario<AdminResult>(
    'admin',
    'room: room-authored messages, admin kick and close reach the client',
    (result) => {
      expect(result.announcements).deep.equal(['maintenance'])
      expect(result.system).deep.equal([{ data: 'welcome', fromRoom: true }])
      expect(result.kicked).toBe(true) // LocalParticipant.onLeave fired on removeParticipant()
      expect(result.kickCause).deep.equal({ type: 'removed', reason: 'be nice' }) // the reason rode the removal
      expect(result.closed).toBe(true) // Room.onClose fired on close()
      expect(result.isClosed).toBe(true)
      expect(result.count).toBe(0)
    },
  )
  testRoomScenario<{ received: number[]; ackSeqs: number[] }>(
    'conflate',
    'room: coalesce conflates a same-key burst to first + latest',
    (r) => {
      const first = r.ackSeqs[0]!
      const latest = r.ackSeqs[4]!
      expect(r.received).deep.equal([1, 5]) // 2..4 collapsed into the single pending slot
      expect(first).greaterThan(0)
      expect(latest).toBe(first + 1) // only first and latest committed
      expect(r.ackSeqs).deep.equal([first, latest, latest, latest, latest]) // overwritten callers share the winner
    },
  )
  testRoomScenario<{
    name: string | null
    title: string | null
    hasScore: boolean
    localName: string | null
    localHasScore: boolean
  }>('attributes', 'room: setAttributes merges per key and deletes on undefined', (r) => {
    expect(r.name).toBe('Zoe') // untouched key preserved across merges
    expect(r.title).toBe('lead') // added key
    expect(r.hasScore).toBe(false) // score removed by `undefined`
    expect(r.localName).toBe('Zoe') // the local handle reflects the merge too
    expect(r.localHasScore).toBe(false)
  })
  testRoomScenario<{ cam: boolean[] }>(
    'demand',
    'room: onDemand turns on when a subscriber wants a track and off when it leaves',
    (r) => {
      expect(r.cam).toContain(true) // a viewer arrived (the track is wanted)
      expect(r.cam[r.cam.length - 1]).toBe(false) // ...and left again — back to unwanted
    },
  )
  testRoomScenario<{ received: string[] }>(
    'tail',
    'room: Room.get({ tail }) holds a between-get-and-subscribe message',
    (r) => {
      expect(r.received).toContain('between') // relay started at serialize, buffered until subscribe
    },
  )
  testRoomScenario<{
    joins: string[]
    joinHasTs: boolean
    publish: { name: string; data: unknown; seqOk: boolean } | null
    send: { name: string; to: string; seqOk: boolean } | null
  }>('hooks', 'room: onAfterJoin/Publish/Send fire with authoritative receipts', (r) => {
    expect(r.joins).deep.equal(['A', 'B']) // onAfterJoin fired for both grants
    expect(r.joinHasTs).toBe(true) // receipt carries joinedAt
    expect(r.publish).deep.equal({ name: 'A', data: 'hello', seqOk: true }) // onAfterPublish + seq
    expect(r.send).deep.equal({ name: 'A', to: 'B', seqOk: true }) // onAfterSend + seq
  })
  testRoomScenario<{ xText: string[]; xBin: number[]; all: string[] }>(
    'member-sub',
    'room: a per-member subscription receives only that member',
    (r) => {
      expect(r.all).deep.equal(['x1', 'y1']) // both delivered room-wide (so absence below is meaningful)
      expect(r.xText).deep.equal(['x1']) // the per-member text sub saw only X
      expect(r.xBin).deep.equal([7]) // per-member binary is selective too
    },
  )
  testRoomScenario<{ held: string[] }>(
    'dm-hold',
    'room: a DM sent before listen() is held and flushed on attach',
    (r) => {
      expect(r.held).deep.equal(['early'])
    },
  )
  testRoomScenario<{ mine: string[]; theirs: string[]; selfDelivery: boolean }>(
    'self',
    'room: selfDelivery:false suppresses your own frames locally, not for others',
    (r) => {
      expect(r.selfDelivery).toBe(false)
      expect(r.theirs).deep.equal(['hi']) // others receive it
      expect(r.mine).deep.equal([]) // you don't
    },
  )
  testRoomScenario<{ mine: string[]; theirs: string[]; selfDelivery: boolean }>(
    'self-server',
    'room: a co-returned server-side selfDelivery:false member sees no own publish, while a client join on the same room view is delivered',
    (r) => {
      expect(r.selfDelivery).toBe(false)
      expect(r.theirs).deep.equal(['from-me', 'from-notme']) // the observer receives both
      expect(r.mine).deep.equal(['from-notme']) // own co-return echo suppressed; client join delivered
    },
  )
  testRoomScenario<{
    updates: string[]
    topic: string | null
    expectedIds: string[]
    listed: string[]
    sameId: boolean
    sameCount: number
  }>('reconfig', 'room: Room.setMeta propagates; list and getOrCreate resolve', (r) => {
    expect(r.updates).toContain('updated') // room.onUpdate fired
    expect(r.topic).toBe('updated') // room.meta reflects it
    expect([...r.listed].sort()).deep.equal([...r.expectedIds].sort()) // both exact rooms enumerated by Room.list
    expect(r.sameId).toBe(true) // getOrCreate returned the existing room
    expect(r.sameCount).toBe(1) // ...with exactly its original member preserved
  })
  testRoomScenario<{ cause: { type: string; reason?: unknown } | null; count: number; empty: boolean }>(
    'identity',
    'room: removeParticipant({ identity }) kicks; onEmpty fires',
    (r) => {
      expect(r.cause).deep.equal({ type: 'removed', reason: 'multi-tab' }) // kicked by identity, reason rode along
      expect(r.count).toBe(0)
      expect(r.empty).toBe(true) // onEmpty fired when the last member went
    },
  )
}
