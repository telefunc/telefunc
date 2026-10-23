export { testRoom, testRoomMetaReads, testRoomLimits, testRoomAckTimeout, testRoomSmallBuffer, testRoomLongWaits }
import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Response, WebSocket } from 'playwright-chromium'
import { navigate, getResult, getCleanupState, runServerAction, cutWire, forceServerGc, sleep } from '../../e2e-utils'
import { newCDPSession } from '../../../utils'

/** On SSE the server waits out the 60 s reconnectTimeout for a closed tab. */
const CLOSED_TAB_WAIT = { timeout: 90_000 }

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

function testRoomScenario<Result>(id: string, name: string, validate: (result: Result) => void | Promise<void>) {
  test(name, async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click(`#test-room-${id}`)
    try {
      await autoRetry(async () => validate(await getResult<Result>('#room-result')))
    } finally {
      // The page leaves with its connection, so a later test that kills its server meets no page errors.
      await page.goto('about:blank')
    }
  })
}

/** Reads of a member's meta of more than a page that a write overlaps, on any backend: the Redis lanes run them too. */
function testRoomMetaReads() {
  testRoomScenario<Record<string, string>>(
    'meta-across-write',
    "room: a read of a member's meta of more than a page gets it before or after a replace, a replace with no keys or a merge that commits between its pages, a replace from generation 9 or 99 included",
    (reads) => {
      expect(Object.keys(reads)).deep.equal([
        'replace with no keys from 1',
        'merge from 1',
        'replace from 9',
        'replace from 99',
      ])
      expect(Object.entries(reads).filter(([, read]) => read !== 'before' && read !== 'after')).deep.equal([])
    },
  )
  testRoomScenario<{ read: string }>(
    'meta-across-drop',
    "room: a read of a member's meta of more than a page that lands in the drop of the meta a replace with no keys left gets it before or after the replace",
    (r) => {
      expect(['before', 'after']).toContain(r.read)
    },
  )
  testRoomScenario<{ read: string }>(
    'split-meta-across-merge',
    "room: a read of a member's meta whose keys another member's meta of more than a page split gets it before or after a merge that commits between the read's pages",
    (r) => {
      expect(['before', 'after']).toContain(r.read)
    },
  )
}

/** What one client holds through one room view: the participants it joins and the named tracks each publishes on. */
function testRoomLimits() {
  testRoomScenario<{ joined: number; errors: string[]; count: number; still: boolean }>(
    'members-cap',
    'room: one room view joins at most 4,096 participants: the join past them rejects, and the ones it joined keep working',
    (r) => {
      expect(r).deep.equal({
        joined: 4096,
        errors: ['A room view joins at most 4096 participants'],
        count: 4096,
        still: true,
      })
    },
  )
  testRoomScenario<{ accepted: number; errors: string[]; retained: number }>(
    'tracks-cap',
    'room: a participant publishes on at most 4,096 named tracks: the publish on one past them rejects, and its tracks keep working',
    (r) => {
      expect(r).deep.equal({
        accepted: 4096,
        errors: ['A participant publishes on at most 4096 named tracks'],
        retained: 4096,
      })
    },
  )
}

/** Someone with the chat open in a browser of their own: `join` as a guest, `sign-in`, or `watch` unseen. */
async function openChat(chatId: string, name: string, how: 'join' | 'sign-in' | 'watch') {
  const context = await page.context().browser()!.newContext()
  const tab = await context.newPage()
  await navigate(`${getServerUrl()}/room?chat=${encodeURIComponent(chatId)}&as=${name}`, undefined, tab)
  await tab.click(`#chat-${how}`)
  await tab.waitForSelector('#chat-status')
  const sees = async () => ({
    status: await tab.textContent('#chat-status'),
    members: await tab.locator('#chat-members li').allTextContents(),
  })
  /** The chat's activity lines about `who`: a test's cut drops every page's connection for a moment. */
  const activity = async (who: string) =>
    (await tab.locator('#chat-activity li').allTextContents()).filter((line) => line.startsWith(who))
  return { context, tab, sees, activity }
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
  testRoomScenario<{ seen: number[][]; serverSeen: number[][]; lateSeen: number[][] }>(
    'own-bytes',
    "room: each binary listener gets bytes of its own, so one that writes into them or transfers them leaves the next one's, a late subscriber's and the page's intact",
    (r) => {
      expect(r.seen).deep.equal([
        [1, 2, 3, 4],
        [5, 6, 7, 8],
      ])
      expect(r.serverSeen).deep.equal([
        [1, 2, 3, 4],
        [5, 6, 7, 8],
      ])
      expect(r.lateSeen).deep.equal([[5, 6, 7, 8]]) // the retained frame, as published
    },
  )
  testRoomScenario<{
    messages: Array<{ data: unknown; from: string }>
    frames: number[][]
    announcements: unknown[]
    dms: unknown[]
    reuserTags: string[] | null
    roomTags: string[] | null
  }>(
    'at-call',
    'room: a write goes out as it was at the call, though its caller changes the object or buffer right after',
    (r) => {
      expect(r.messages).deep.equal([
        { data: { n: 10 }, from: 'Me' },
        { data: { n: 11 }, from: 'Me' },
        { data: { n: 1 }, from: 'Reuser' },
      ])
      expect(r.frames).deep.equal([[1, 2, 3]])
      expect(r.announcements).deep.equal([{ n: 3 }])
      expect(r.dms).to.have.deep.members([{ n: 2 }, { n: 4 }])
      expect(r.reuserTags).deep.equal(['b'])
      expect(r.roomTags).deep.equal(['x'])
    },
  )
  testRoomScenario<{ seen: unknown[]; serverSeen: unknown[] }>(
    'right-after',
    'room: a publish right after subscribing, and a DM right after its recipient joined, arrive, each kind in call order',
    (r) => {
      // Text and binary travel apart: only each kind's own order is kept.
      const texts = (list: unknown[]) => list.filter((data) => typeof data === 'string')
      expect(texts(r.serverSeen)).deep.equal(['t1', 't2', 'dm'])
      expect(r.serverSeen.filter(Array.isArray)).deep.equal([[1]])
      expect(texts(r.seen)).deep.equal(['p1', 'p2'])
      expect(r.seen.filter(Array.isArray)).deep.equal([[2]])
    },
  )
  testRoomScenario<{ texts: number[]; frames: number[] }>(
    'call-order',
    "room: a member's publishes arrive in the order it called them, though the guard takes longer for the earlier ones",
    (r) => {
      const numbers = Array.from({ length: 20 }, (_, i) => i)
      expect(r.texts).deep.equal(numbers)
      expect(r.frames).deep.equal(numbers)
    },
  )
  testRoomScenario<{
    stored: { afterAttributes: string[]; roomMeta: [string, unknown][]; memberMeta: [string, unknown][] }
    roomMeta: [string, unknown][]
    otherRoomMeta: [string, unknown][]
    memberMeta: [string, unknown][]
    otherMemberMeta: [string, unknown][]
  }>(
    'meta-race',
    'room: concurrent meta writes from many callers each apply, and every view ends on the stored meta in its key order',
    (r) => {
      const { afterAttributes, roomMeta, memberMeta } = r.stored
      expect(afterAttributes.length).toBe(201) // the created `topic`, then all 200 keys
      expect(afterAttributes[0]).toBe('topic')
      // One Room.setMeta() won whole: its own two keys, in its order.
      const winner = roomMeta[0]![1] as number
      expect(roomMeta).deep.equal([
        ['winner', winner],
        [`w${winner}`, true],
      ])
      // A member's writes apply in call order: its last setMeta(), then the setAttributes() calls after it.
      expect(memberMeta).deep.equal([
        ['name', 'Writer'],
        ['round', 90],
        ...Array.from({ length: 9 }, (_, i) => [`a${91 + i}`, 91 + i]),
      ])
      expect(r.roomMeta).deep.equal(roomMeta)
      expect(r.otherRoomMeta).deep.equal(roomMeta)
      expect(r.memberMeta).deep.equal(memberMeta)
      expect(r.otherMemberMeta).deep.equal(memberMeta)
    },
  )
  testRoomScenario<{ reads: string[] }>(
    'meta-replace',
    "room: a read of a room's or member's meta of more than a page, while it is replaced, gets the meta before or after the replace, never a mix or none",
    (r) => {
      expect(r.reads.length).toBe(12)
      for (const read of r.reads) expect(['a', 'b']).toContain(read)
    },
  )
  testRoomMetaReads()
  testRoomLimits()
  testRoomScenario<{ names: unknown[]; otherNames: unknown[]; viewName: unknown; otherViewName: unknown }>(
    'sender',
    'room: a meta write made while a publish waits on its guard applies on every view, and the publish arrives once',
    (r) => {
      expect(r.names.length).toBe(1)
      expect(r.otherNames.length).toBe(1)
      expect(r.viewName).toBe('B')
      expect(r.otherViewName).toBe('B')
    },
  )
  testRoomScenario<{
    binaryError: string | null
    afterHookAck: boolean | string
    calls: Array<{ hook: string; name: unknown; frozen: boolean }>
  }>(
    'guard-calls',
    "room: guards get a frozen sender and never a stand-in for a member that left; a binary publish a guard refuses rejects with the guard's Abort; an after-hook that throws keeps the publish",
    (r) => {
      expect(r.binaryError).toBe('no frames')
      expect(r.afterHookAck).toBe(true)
      expect(r.calls.filter((call) => call.hook === 'join').map((call) => call.name)).deep.equal(['G', 'P'])
      expect(r.calls.filter((call) => call.hook === 'send').map((call) => call.name)).deep.equal(['G'])
      for (const call of r.calls) expect(call).deep.include({ frozen: true })
      for (const call of r.calls.filter((call) => call.hook === 'publish')) expect(call.name).toBe('G')
    },
  )
  testRoomScenario<{ mine: [string, unknown][]; remote: [string, unknown][] }>(
    'own-meta-race',
    "room: a page's own member ends on the meta its unawaited writes stored, in call order, as other views do",
    (r) => {
      const stored = [['name', 'Mine'], ['round', 15], ...Array.from({ length: 4 }, (_, i) => [`m${16 + i}`, 16 + i])]
      expect(r.mine).deep.equal(stored)
      expect(r.remote).deep.equal(stored)
    },
  )
  testRoomScenario<{
    leave: string
    kick: string
    close: string
    atLeave: { cause: string; isClosed: boolean; count: number } | null
    unsubscribeInOnClose: string | null
    lateOnClose: boolean
    isClosed: boolean[]
  }>(
    'close-handles',
    'room: a leave or kick that empties a room onEmpty closes resolves, and Room.close() ends every handle: the leave callback sees the room closed and empty, an unsubscribe in onClose returns, and an onClose added after fires',
    (r) => {
      expect(r.leave).toBe('resolved')
      expect(r.kick).toBe('resolved')
      expect(r.close).toBe('resolved')
      expect(r.isClosed).deep.equal([true, true])
      expect(r.atLeave).deep.equal({ cause: 'closed', isClosed: true, count: 0 })
      expect(r.unsubscribeInOnClose).toBe('returned')
      expect(r.lateOnClose).toBe(true)
    },
  )
  testRoomScenario<{
    order: string[]
    whileClosing: { listed: string[] | string; after: string[] }
    recreated: { meta: unknown; count: number }
    recreatedOpen: boolean
  }>(
    'list-races',
    'room: Room.list() lists in code point order and skips rooms that close during it; a room created again after its close starts fresh and still closes by itself',
    (r) => {
      expect(r.order).deep.equal(['B', 'Z', 'a', 'b', 'é'])
      expect(Array.isArray(r.whileClosing.listed)).toBe(true) // a list, not an error
      expect(r.whileClosing.after).deep.equal([])
      expect(r.recreated).deep.equal({ meta: { incarnation: 2 }, count: 0 })
      expect(r.recreatedOpen).toBe(false)
    },
  )
  testRoomScenario<{ fromPage: unknown; fromServer: unknown; refused: unknown }>(
    'ack-dm',
    "room: an acknowledged DM resolves with its recipient's reply, from a page or a server member, and rejects with the recipient's Abort",
    (r) => {
      expect(r.fromPage).deep.equal({ echo: 'hi' })
      expect(r.fromServer).deep.equal({ echo: 'hi' })
      expect(r.refused).toBe('refused')
    },
  )
  testRoomScenario<{ listed: number | string; serverCount: number; count: number; emptied: boolean }>(
    'roster-churn',
    'room: a page opening a room of 2,000 members while members join, leave and change meta loads its roster, counts only visible members, and sees no empty room',
    (r) => {
      expect(typeof r.listed).toBe('number') // the roster loaded, not an error
      expect(r.listed).greaterThanOrEqual(2000)
      expect(r.serverCount).toBe(2001) // 2,000 members and the page's; not the hidden one
      expect(r.count).toBe(r.serverCount)
      expect(r.emptied).toBe(false)
    },
  )
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
    'room: the declared message type is shielded at runtime (a malformed publish is rejected)',
    (r) => {
      expect(r.okAck).toBe(true)
      // The generated shield rejects malformed ingress through the publish promise.
      expect(r.badError).toBe('ShieldValidationError')
      expect(r.received).deep.equal(['hi'])
    },
  )
  testRoomScenario<AdminResult>(
    'admin',
    'room: room-authored messages, admin kick and close reach the client',
    (result) => {
      expect(result.announcements).deep.equal(['maintenance'])
      expect(result.system).deep.equal([{ data: 'welcome', fromRoom: true }])
      expect(result.kicked).toBe(true) // LocalParticipant.onLeave fired on removeParticipant()
      expect(result.kickCause).deep.equal({ type: 'removed', reason: 'be nice' })
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
    expect(r.name).toBe('Zoe')
    expect(r.title).toBe('lead')
    expect(r.hasScore).toBe(false) // score removed by `undefined`
    expect(r.localName).toBe('Zoe')
    expect(r.localHasScore).toBe(false)
  })
  testRoomScenario<{ cam: boolean[] }>(
    'demand',
    'room: onDemand turns on when a subscriber wants a track and off when it leaves',
    (r) => {
      expect(r.cam).toContain(true) // a viewer arrived (the track is wanted)
      expect(r.cam[r.cam.length - 1]).toBe(false) // ...and left again, back to unwanted
    },
  )
  test('room: a member the page keeps works on after the page lets go of the room it joined through and a GC pass runs', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-member-keeps-room')
    await autoRetry(async () => {
      expect((await getResult<{ roomId: string }>('#room-result')).roomId).match(/^e2e-member-keeps-room:/)
    })
    const cdp = await newCDPSession()
    await cdp.send('HeapProfiler.collectGarbage')
    await cdp.detach()
    // A room the pass collected closes right after it, so it would have closed by now.
    await sleep(1000)
    await page.click('#test-room-member-keeps-room-check')
    try {
      await autoRetry(async () => {
        expect(await getResult('#room-result')).deep.equal({ published: 'ok', received: ['after-gc'], left: null })
      })
    } finally {
      await page.goto('about:blank')
    }
  })
  test('room: a GC pass after the page lets go of a member and its room makes the member leave and closes the room, a member the page keeps after it left or after Room.close() holds no room open, and a join a guard refuses rejects without an unhandled rejection', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-members-let-go')
    await autoRetry(async () => {
      expect((await getResult<{ ready: boolean }>('#room-result')).ready).toBe(true)
    })
    const cdp = await newCDPSession()
    await cdp.send('HeapProfiler.collectGarbage')
    await cdp.detach()
    await sleep(1000)
    await page.click('#test-room-members-let-go-check')
    try {
      await autoRetry(
        async () => {
          expect(await getResult('#room-result')).deep.equal({
            droppedLeaves: ['disconnected'],
            droppedOpen: false,
            leftOpen: false,
            leaverPublish: 'Participant left the room',
            closedLeaves: ['closed'],
            rejected: 'blocked join of Banned',
            unhandled: 0,
          })
        },
        { timeout: 20_000 },
      )
    } finally {
      await page.goto('about:blank')
    }
  })
  testRoomScenario<{ received: string[]; existing: string[]; created: string[]; plain: string[] }>(
    'tail',
    'room: Room.get({ tail }) and Room.getOrCreate({ tail }) hold a between-get-and-subscribe message',
    (r) => {
      expect(r.received).toContain('between') // relay started at serialize, buffered until subscribe
      expect(r.existing).toContain('between') // getOrCreate of an existing room
      expect(r.created).toContain('between') // getOrCreate that created the room
      expect(r.plain).deep.equal(['after']) // without a tail, the view gets only what came after its subscribe
    },
  )
  testRoomScenario<{
    joins: string[]
    joinHasTs: boolean
    publish: { name: string; data: unknown; seqOk: boolean } | null
    send: { name: string; to: string; seqOk: boolean } | null
  }>('hooks', 'room: onAfterJoin/Publish/Send fire with authoritative receipts', (r) => {
    expect(r.joins).deep.equal(['A', 'B'])
    expect(r.joinHasTs).toBe(true)
    expect(r.publish).deep.equal({ name: 'A', data: 'hello', seqOk: true })
    expect(r.send).deep.equal({ name: 'A', to: 'B', seqOk: true })
  })
  testRoomScenario<{ xText: string[]; xBin: number[]; all: string[] }>(
    'member-sub',
    'room: a per-member subscription receives only that member',
    (r) => {
      expect(r.all).deep.equal(['x1', 'y1']) // both delivered room-wide (so absence below is meaningful)
      expect(r.xText).deep.equal(['x1'])
      expect(r.xBin).deep.equal([7])
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
      expect(r.theirs).deep.equal(['hi'])
      expect(r.mine).deep.equal([])
    },
  )
  testRoomScenario<{ mine: string[]; theirs: string[]; selfDelivery: boolean }>(
    'self-server',
    'room: a co-returned server-side selfDelivery:false member sees no own publish, while a client join on the same room view is delivered',
    (r) => {
      expect(r.selfDelivery).toBe(false)
      expect(r.theirs).deep.equal(['from-me', 'from-notme'])
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
    expect(r.topic).toBe('updated')
    expect([...r.listed].sort()).deep.equal([...r.expectedIds].sort())
    expect(r.sameId).toBe(true)
    expect(r.sameCount).toBe(1) // ...with exactly its original member preserved
  })
  testRoomScenario<{ cause: { type: string; reason?: unknown } | null; count: number; empty: boolean }>(
    'identity',
    'room: removeParticipant({ identity }) kicks; onEmpty fires',
    (r) => {
      expect(r.cause).deep.equal({ type: 'removed', reason: 'multi-tab' })
      expect(r.count).toBe(0)
      expect(r.empty).toBe(true)
    },
  )
  test('room: a member who leaves, or whose tab closes, leaves once with its cause, and an acknowledged DM to or from it fails at the leave', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-leave-causes')
    const { roomId } = await autoRetry(() => getResult<{ roomId: string }>('#room-result'))
    const context = await page.context().browser()!.newContext()
    const other = await context.newPage()
    await navigate(`${getServerUrl()}/room?roomId=${encodeURIComponent(roomId)}`, undefined, other)
    await other.click('#test-room-join-only')
    await autoRetry(async () => expect(await other.textContent('#room-result')).toBe('"joined"'))
    await context.close()
    await autoRetry(async () => {
      const r = await getResult<{
        recipientLeft: string
        senderLeft: string
        seconds: number
        causes: Array<{ name: unknown; type: string }>
      }>('#room-result')
      expect(r.recipientLeft).toBe('rejected')
      expect(r.senderLeft).toBe('rejected')
      expect(r.seconds).lessThan(10) // not the 60 s acknowledgement timeout
      expect(r.causes).deep.equal([
        { name: 'A', type: 'left' },
        { name: 'B', type: 'left' },
        { name: 'Closer', type: 'disconnected' },
      ])
    }, CLOSED_TAB_WAIT)
  })
  test("room: another page's member shows when its page drops and comes back, and leaves with 'disconnected' once its page never returns", async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-connection')
    const { roomId } = await autoRetry(() => getResult<{ roomId: string }>('#room-result'))
    const context = await page.context().browser()!.newContext()
    const other = await context.newPage()
    await navigate(`${getServerUrl()}/room?roomId=${encodeURIComponent(roomId)}`, undefined, other)
    await other.click('#test-room-join-only')
    await autoRetry(async () => expect(await other.textContent('#room-result')).toBe('"joined"'))
    const seen = async () => {
      const { events, closerConnected } = await getResult<{ events: string[]; closerConnected: boolean | null }>(
        '#room-result',
      )
      return { events, closerConnected }
    }
    await autoRetry(async () => expect(await seen()).deep.equal({ events: [], closerConnected: true }))
    await context.setOffline(true)
    await cutWire()
    await autoRetry(async () =>
      expect(await seen()).deep.equal({ events: ['disconnect:false'], closerConnected: false }),
    )
    await context.setOffline(false)
    await autoRetry(async () =>
      expect(await seen()).deep.equal({ events: ['disconnect:false', 'reconnect:true'], closerConnected: true }),
    )
    await context.setOffline(true)
    await cutWire()
    await context.close()
    await autoRetry(async () => {
      expect(await seen()).deep.equal({
        events: ['disconnect:false', 'reconnect:true', 'disconnect:false', 'leave:disconnected'],
        closerConnected: null,
      })
    }, CLOSED_TAB_WAIT)
  })
  test("room: in a chat, everyone sees a member whose tab went offline as reconnecting, in the list, the activity, Who's online and that member's profile, and as back once it is; the member sees itself reconnecting", async () => {
    const chatId = `e2e-chat:${crypto.randomUUID()}`
    const alice = await openChat(chatId, 'Alice', 'join')
    const bob = await openChat(chatId, 'Bob', 'join')
    try {
      await autoRetry(async () =>
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob'] }),
      )
      await bob.context.setOffline(true)
      await cutWire()
      await autoRetry(async () => {
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob (reconnecting…)'] })
        expect(await alice.activity('Bob')).deep.equal(['Bob lost connection'])
        expect((await bob.sees()).status).toBe('Reconnecting…')
      })
      await alice.tab.click('#chat-members button:has-text("Bob")')
      await alice.tab.click('#chat-who')
      await autoRetry(async () => {
        expect(await alice.tab.textContent('#chat-profile')).toBe('Bob: reconnecting…')
        expect(await alice.tab.textContent('#chat-online')).toBe('Alice, Bob (away)')
      })
      await bob.context.setOffline(false)
      await autoRetry(async () => {
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob'] })
        expect(await alice.activity('Bob')).deep.equal(['Bob lost connection', 'Bob is connected again'])
        expect(await alice.tab.textContent('#chat-profile')).toBe('Bob: online')
        expect((await bob.sees()).status).toBe('Connected')
      })
      await alice.tab.click('#chat-who')
      await autoRetry(async () => expect(await alice.tab.textContent('#chat-online')).toBe('Alice, Bob'))
    } finally {
      await alice.context.close()
      await bob.context.close()
    }
  })
  test("room: in a chat, the server announces when a signed-in member's tab goes offline and comes back, and someone watching unseen drops without anyone seeing it", async () => {
    const chatId = `e2e-chat:${crypto.randomUUID()}`
    const alice = await openChat(chatId, 'Alice', 'join')
    const carol = await openChat(chatId, 'Carol', 'sign-in')
    const viewer = await openChat(chatId, 'Dan', 'watch')
    try {
      const aliceSees = async () => ({
        count: await alice.tab.textContent('#chat-count'),
        members: (await alice.sees()).members,
        carol: (await alice.activity('Carol')).sort(),
        viewer: [...(await alice.activity('Viewer')), ...(await alice.activity('Dan'))],
      })
      await autoRetry(async () =>
        expect(await aliceSees()).deep.equal({
          count: '2 in the chat',
          members: ['Alice (you)', 'Carol'],
          carol: [],
          viewer: [],
        }),
      )
      await carol.context.setOffline(true)
      await viewer.context.setOffline(true)
      await cutWire()
      await autoRetry(async () => {
        expect(await aliceSees()).deep.equal({
          count: '2 in the chat',
          members: ['Alice (you)', 'Carol (reconnecting…)'],
          carol: ['Carol lost connection', 'Carol went away'],
          viewer: [],
        })
        expect((await carol.sees()).status).toBe('Reconnecting…')
        expect((await viewer.sees()).status).toBe('Reconnecting…')
      })
      await carol.context.setOffline(false)
      await viewer.context.setOffline(false)
      await autoRetry(async () => {
        expect(await aliceSees()).deep.equal({
          count: '2 in the chat',
          members: ['Alice (you)', 'Carol'],
          carol: ['Carol is back', 'Carol is connected again', 'Carol lost connection', 'Carol went away'],
          viewer: [],
        })
        expect((await carol.sees()).status).toBe('Connected')
        expect((await viewer.sees()).status).toBe('Connected')
      })
    } finally {
      await alice.context.close()
      await carol.context.close()
      await viewer.context.close()
    }
  })
  test('room: a join whose tab closes while its guard waits leaves no member behind', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-join-races')
    const { roomId } = await autoRetry(() => getResult<{ roomId: string }>('#room-result'), { timeout: 10_000 })
    const context = await page.context().browser()!.newContext()
    const other = await context.newPage()
    await navigate(`${getServerUrl()}/room?roomId=${encodeURIComponent(roomId)}`, undefined, other)
    await other.click('#test-room-join-slowly')
    await autoRetry(async () => expect(await getResult('#room-result')).deep.include({ closerJoining: true }))
    await context.close()
    await autoRetry(async () => {
      const r = await getResult<{
        events: string[]
        count: number
        serverCount: number
        settled: boolean
      }>('#room-result')
      expect(r).deep.include({ settled: true, count: 0, serverCount: 0 })
      // Shown joining at most once, then leaving once.
      expect(r.events).to.be.deep.oneOf([[], ['join:Closer', 'leave:Closer:disconnected']])
    }, CLOSED_TAB_WAIT)
  })
  test('room: a room nothing holds closes by itself, departureTimeout after its last member left or emptyTimeout after it was created, while the page it was returned to holds it open until that page closes, and Infinity never closes', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-self-close')
    const { base } = await autoRetry(() => getResult<{ base: string }>('#room-result'), { timeout: 10_000 })
    const context = await page.context().browser()!.newContext()
    const other = await context.newPage()
    await navigate(`${getServerUrl()}/room?roomId=${encodeURIComponent(`${base}:held`)}`, undefined, other)
    await other.click('#test-room-hold-room')
    await autoRetry(async () => expect(await other.textContent('#room-result')).toBe('"holding"'))
    await autoRetry(
      async () => {
        expect(await getResult('#room-result')).deep.equal({
          base,
          open: { empty: false, departed: false, held: true, never: true },
          outlived: true,
        })
      },
      { timeout: 15_000 },
    )
    await context.close()
    await page.click('#test-room-self-close-released')
    await autoRetry(
      async () => expect(await getResult('#room-result')).deep.equal({ heldOpen: false }),
      CLOSED_TAB_WAIT,
    )
  })
  testRoomScenario<{ calls: string[] }>(
    'listeners',
    'room: a callback registered twice is called once per registration, in registration order, and each unlisten removes only its own',
    (r) => {
      expect(r.calls).deep.equal(['a', 'b', 'a', 'b', 'a', 'b'])
    },
  )
  testRoomScenario<{
    text: string[]
    frames: Array<[string | null, number, string]>
    later: string[]
    server: { text: string[]; frames: number[] }
  }>(
    'retain-late',
    'room: a late subscriber, on the page or the server, gets each retained message once before the live ones, and none of a member who left',
    (r) => {
      expect(r.text).deep.equal(['pinned', 'live', 'live-2'])
      expect(r.frames).deep.equal([
        ['cam', 1, 'A'],
        ['cam', 4, 'A'],
      ])
      expect(r.later).deep.equal(['live-2'])
      expect(r.server).deep.equal({ text: ['pinned', 'live', 'live-2'], frames: [1, 4] })
    },
  )
  testRoomScenario<{
    events: string[]
    handedEvents: string[]
    memberEvents: string[]
    counts: number[]
    empty: boolean
    alone: { cause: { type: string } | null; closed: boolean }
  }>(
    'hidden',
    'room: a hidden member is not counted, its meta and leave reach only the page handed it, and it holds no room open',
    (r) => {
      expect(r.counts).deep.equal([1, 1])
      expect(r.events).deep.equal(['join:P', 'leave:P', 'join:Q'])
      expect(r.handedEvents.filter((e) => e.includes('Bot'))).deep.equal([])
      expect(r.memberEvents).deep.equal(['update:Bot2', 'leave:left'])
      expect(r.empty).toBe(true)
      expect(r.alone).deep.equal({ cause: { type: 'closed' }, closed: true })
    },
  )
  test('room: React reads a room through useSyncExternalStore with its methods as they are; a member returned with a closed room has left', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-proxy')
    await autoRetry(async () => {
      const r = await getResult<{
        sameMethods: boolean
        detachedCount: number
        causes: unknown[]
        closedIsClosed: boolean
      }>('#room-result')
      expect(r.sameMethods).toBe(true)
      expect(r.detachedCount).greaterThanOrEqual(1)
      expect(r.causes).deep.equal([{ type: 'closed' }])
      expect(r.closedIsClosed).toBe(true)
      expect(await page.textContent('#room-live-count')).toBe('2')
    })
  })
  test('room: a listener that fails or a RoomError that escapes app code is reported as a bug, and an un-awaited write that fails leaves no unhandled rejection', async () => {
    await navigate(`${getServerUrl()}/room`)
    const consoleErrors: string[] = []
    const onConsole = (msg: { type(): string; text(): string }) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text())
    }
    const pageErrors: string[] = []
    const onPageError = (err: Error) => void pageErrors.push(err.message)
    page.on('console', onConsole)
    page.on('pageerror', onPageError)
    try {
      await page.click('#test-room-errors')
      await autoRetry(async () => {
        const r = await getResult<{
          awaited: string | null
          server: { reported: string[]; unhandled: number; awaited: string | null }
        }>('#room-result')
        expect(r.awaited).toBe('Participant left the room')
        expect(r.server).deep.equal({
          reported: [
            'room-listener-bug: Participant left the room',
            'room-listener-bug: async listen',
            'room-listener-bug: async onJoin',
            'room-listener-bug: async onJoin',
            'room-listener-bug: async onJoin',
          ],
          unhandled: 0,
          awaited: 'Participant left the room',
        })
        const reported = consoleErrors.filter((text) => text.includes('room-listener-bug'))
        expect(reported.map((text) => text.split('\n')[0])).deep.equal([
          '[telefunc:room-error] Error: room-listener-bug: async subscribe',
        ])
        expect(pageErrors).deep.equal([])
      })
    } finally {
      page.off('console', onConsole)
      page.off('pageerror', onPageError)
    }
  })
  testRoomScenario<{
    inOrder: number
    defaultTrack: number[]
    demand: Array<[string | null, boolean]>
    wanted: number
    otherOwn: Array<[string, boolean]>
    tooMany: string | null
  }>(
    'tracks',
    "room: each of 1,000 named tracks reaches its subscriber in order, onDemand follows the wants, and a selfDelivery:false member's own side is no demand",
    (r) => {
      expect(r.inOrder).toBe(1_000)
      expect(r.defaultTrack).deep.equal([7])
      expect(r.demand.filter(([track]) => track !== null)).deep.equal([
        ['t0', true],
        ['t999', true],
        ['t0', false],
      ])
      expect(r.demand.filter(([track]) => track === null)).deep.equal([[null, true]])
      expect(r.wanted).toBe(1_001)
      expect(r.otherOwn).deep.equal([['own', true]])
      expect(r.tooMany).include('[Wrong Usage] Too many binary tracks subscribed on one room (4096 max)')
    },
  )
  test('room: a page back from offline gets what its room sent meanwhile, in order, and the leaves, kick and close that happened while it was away, with their causes', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-away')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-away:/)
    })
    await page.context().setOffline(true)
    await cutWire()
    await runServerAction(`roomAway_${roomId}`)
    await autoRetry(async () => {
      expect((await getCleanupState())[`roomAway_${roomId}`]).toBe('done')
    })
    await page.context().setOffline(false)
    await autoRetry(async () => {
      expect(await getResult('#room-result')).deep.equal({
        roomId,
        messages: ['m1', 'm2', 'm3'],
        leaves: ['Gone:left', 'Me:removed'],
        topic: 'while-away',
        myCause: { type: 'removed', reason: 'while-away' },
        closed: true,
      })
    })
  })
  test('room: a page offline while its room sent it more than bufferLimit holds is let go at its reconnect: its view closes and its member leaves, with no gap delivered', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-overflow')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-overflow:/)
    })
    await page.context().setOffline(true)
    await cutWire()
    await runServerAction(`roomOverflow_${roomId}`)
    await autoRetry(async () => {
      expect((await getCleanupState())[`roomOverflow_${roomId}`]).toBe('done')
    })
    await page.context().setOffline(false)
    await autoRetry(async () => {
      expect(await getResult('#room-result')).deep.equal({
        roomId,
        seen: ['before'],
        myCause: { type: 'disconnected' },
        closed: true,
        isClosed: true,
        leaves: ['Me:disconnected'],
      })
    })
  })
  test("room: on a 1 Mbit/s link, a page gets 640 KB of its room's messages and publishes 640 KB without reconnecting", async () => {
    await navigate(`${getServerUrl()}/room`)
    let sseStreams = 0
    const onResponse = (response: Response) => {
      if (response.headers()['content-type']?.startsWith('text/event-stream')) sseStreams++
    }
    let webSockets = 0
    const onWebSocket = (ws: WebSocket) => {
      if (ws.url().includes('/_telefunc')) webSockets++
    }
    page.on('response', onResponse)
    page.on('websocket', onWebSocket)
    // test-e2e's `page` is a proxy, which newCDPSession() doesn't take.
    const cdp = await page.context().newCDPSession(
      page
        .context()
        .pages()
        .find((p) => p.url() === page.url())!,
    )
    try {
      await cdp.send('Network.enable')
      await cdp.send('Network.emulateNetworkConditions', {
        offline: false,
        latency: 0,
        downloadThroughput: 125_000,
        uploadThroughput: 125_000,
      })
      await page.click('#test-room-slow-link')
      await autoRetry(
        async () => {
          expect(await getResult('#room-result')).deep.equal({
            received: Array.from({ length: 40 }, (_, n) => n),
            watched: 80,
          })
        },
        { timeout: 40_000 },
      )
      // `['sse', 'ws']` starts on SSE and upgrades to a WebSocket.
      const transports: string[] = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!)
      expect({ sseStreams, webSockets }).deep.equal({
        sseStreams: transports.includes('sse') ? 1 : 0,
        webSockets: transports.includes('ws') ? 1 : 0,
      })
    } finally {
      await cdp.send('Network.emulateNetworkConditions', {
        offline: false,
        latency: 0,
        downloadThroughput: -1,
        uploadThroughput: -1,
      })
      await cdp.detach()
      page.off('response', onResponse)
      page.off('websocket', onWebSocket)
    }
  })
  test('room: the server lets go of a view whose listeners are gone, a member that left and the listeners on it, and a closed room and its members', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-release')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-release:/)
    })
    await autoRetry(
      async () => {
        await forceServerGc()
        await runServerAction(`roomRelease_${roomId}`)
        expect((await getCleanupState())[`roomRelease_${roomId}`]).toBe(
          'view:released,left:released,closed:released,member:released',
        )
      },
      { timeout: 10_000 },
    )
  })
  testRoomScenario<{ received: string[]; announcements: string[]; kept: string[] }>(
    'tail-order',
    'room: a tail hands its first subscriber what it held in order, though an announcement came first, and only the latest 256',
    (r) => {
      expect(r.announcements).deep.equal(['ann'])
      expect(r.received).deep.equal(['a', 'b'])
      expect(r.kept).deep.equal(Array.from({ length: 256 }, (_, i) => `n${i + 44}`))
    },
  )
  test('room: a store write that fails once heals: a failed leave can be retried, a failed join leaves no member, and a lane the store ends is replaced', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-hiccups')
    await autoRetry(async () => {
      expect(await getResult('#room-result')).deep.equal({
        leave: {
          firstLeave: 'Internal Server Error — see server logs',
          publishAfter: 'ok',
          secondLeave: 'ok',
          leaves: ['Me:left'],
          count: 0,
        },
        join: { firstJoin: 'Internal Server Error — see server logs', count: 1, listed: ['Second'] },
        lane: { ended: true, got: ['before', 'after'] },
      })
    })
  })
  test('room: a page that reconnects while its join is pending gets the leave, with its cause, of a member removed while it was away', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-away-join')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-away-join:/)
    })
    await page.context().setOffline(true)
    await cutWire()
    await page.click('#test-room-away-join-now')
    await runServerAction(`roomLeaveWhileJoining_${roomId}`)
    await autoRetry(async () => {
      expect((await getCleanupState())[`roomLeaveWhileJoining_${roomId}`]).toBe('done')
    })
    await page.context().setOffline(false)
    await autoRetry(async () => {
      expect(await getResult('#room-result')).deep.equal({
        roomId,
        joined: true,
        leaves: [['Other', { type: 'removed', reason: 'while-joining' }]],
        count: 1,
      })
    })
  })
}

/** Waits out the 60 s acknowledgement timeout: one lane runs it. */
function testRoomAckTimeout() {
  test('room: an acknowledged DM its recipient never answers rejects at the 60 s acknowledgement timeout', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-ack-timeout')
    await autoRetry(
      async () => {
        const r = await getResult<{ error: string | null; seconds: number }>('#room-result')
        expect(r.error).toBe('rejected')
        expect(r.seconds).within(59, 65)
      },
      { timeout: 75_000 },
    )
  })
}

/** Runs in a lane whose config.channel.bufferLimit is 4,096 bytes. */
function testRoomSmallBuffer() {
  testRoomScenario<{ first: string; outcomes: string[]; got: string[] }>(
    'dm-overflow',
    'room: DMs to a page before its first listen() are held within bufferLimit and delivered on listen(); past it the oldest is dropped and its acknowledged send rejects',
    (r) => {
      expect(r.first).toBe('rejected')
      expect(r.outcomes).deep.equal(['rejected', 'answered', 'answered'])
      expect(r.got).deep.equal(['2', '3'])
    },
  )
  testRoomScenario<{ serverOutcomes: string[]; seen: string[] }>(
    'hold-overflow',
    'room: publishes held while a subscription establishes fit bufferLimit by their bytes, and the one past it rejects with ChannelOverflowError',
    (r) => {
      expect(r.serverOutcomes).deep.equal(['sent', 'sent', 'ChannelOverflowError'])
      expect(r.seen).deep.equal(['1', '2'])
    },
  )
}

/** Tests that wait out Room's fixed timeouts, run in one lane. */
function testRoomLongWaits() {
  test('room: a tail nobody subscribes to within 60 seconds drops what it held', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-tail-lease')
    await autoRetry(
      async () => {
        expect(await getResult('#room-result')).deep.equal({ received: ['live'] })
      },
      { timeout: 75_000 },
    )
  })

  test("room: a store write that fails once heals at the next heartbeat: a page's view keeps its room open, and a failed join leaves no member", async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-hiccups-heartbeat')
    let viewRoomId = ''
    await autoRetry(
      async () => {
        const r = await getResult<{ viewRoom: { listed: string[] } }>('#room-result')
        viewRoomId = r.viewRoom.listed[0] ?? ''
        expect(r).deep.equal({
          viewRoom: { listed: [viewRoomId], isClosed: false },
          joinRoom: { firstJoin: 'Internal Server Error — see server logs', listed: ['Second'], count: 1 },
        })
      },
      { timeout: 60_000 },
    )
    expect(viewRoomId).match(/^e2e-hiccup-view:/)
  })

  test('room: a room of 100,000 members streams its roster to a page, with a join and a leave landing meanwhile, and the server answers the page promptly through a heartbeat', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-scale')
    await autoRetry(
      async () => {
        const r = await getResult<{ roster: unknown; pings: number; maxPingMs: number }>('#room-result')
        expect(r.roster).deep.equal({ count: 100_000, listed: 100_000, late: true, m0: false })
        expect(r.pings).greaterThan(100)
        expect(r.maxPingMs).lessThan(1_000)
      },
      { timeout: 110_000 },
    )
  })
}
