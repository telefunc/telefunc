export {
  testRoom,
  testRoomDemand,
  testRoomFailingBackend,
  testRoomMetaReads,
  testRoomLimits,
  testRoomAckTimeout,
  testRoomSmallMemory,
  testRoomPublishHold,
  testRoomLongWaits,
  testRoomRedisSubscriberDropped,
  testRoomRedisPublishLimit,
  testRoomStaleRead,
}
import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Response, WebSocket } from 'playwright-chromium'
import { startBlackHoleProxy } from '../../black-hole-proxy'
import {
  navigate,
  getResult,
  getCleanupState,
  retryOnNetworkChange,
  runServerAction,
  cutWire,
  forceServerGc,
  sleep,
  canCutWire,
} from '../../e2e-utils'
import { newCDPSession } from '../../../utils'
import type { NamedDemand } from './demandNames'

/** On SSE the server waits out the 60 s reconnectTimeout for a closed tab. */
const CLOSED_TAB_WAIT = { timeout: 90_000 }

type ChatResult = {
  events: string[]
  received: Array<{ text: string; from: string }>
  updates: unknown[]
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
    track: string
    meta: Record<string, unknown> | null
  }>
  cameraOnly: number[]
  defaultOnly: number[]
}

type GuardResult = {
  joinError: string | null
  unguardedJoinError: string | null
  publishError: string | null
  trackError: string | null
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

/** What a view received after its page's participant was removed, and how often it closed. */
type RemovedView = { text: string[]; closes: number; closeCause: unknown; isClosed: boolean }

/** What a view received of messages, of track `cam` and of the lossy track `cursor`. */
type OwnTrack = { text: string[]; frames: number[]; lossy: number[] }

/** What a view receives of `Other`'s messages and frames, beside a `me` whose own it doesn't get back. */
const othersOnly: OwnTrack = { text: ['hello', 'bye'], frames: [1, 9], lossy: [1, 9] }

type MemberResult = {
  hasMember: boolean
  memberName: string | null
  sameObject: boolean
  listedOnServer: string[]
}

/** `cam` received by `watchers` and `watcherCount` people in all. */
const cam = (watchers: string[], watcherCount: number): NamedDemand => ({ track: 'cam', watchers, watcherCount })

function testRoomScenario<Result>(id: string, name: string, validate: (result: Result) => void | Promise<void>) {
  test(name, async () => {
    try {
      await retryOnNetworkChange(async (cut) => {
        await navigate(`${getServerUrl()}/room`)
        await page.click(`#test-room-${id}`)
        await autoRetry(async () => {
          if (!cut()) await validate(await getResult<Result>('#room-result'))
        })
      })
    } finally {
      // The page leaves with its connection, so a later test that kills its server meets no page errors.
      await page.goto('about:blank')
    }
  })
}

/** A card whose recipient saves its reply to an acknowledged DM, and whose second button acts meanwhile. */
function testRoomAckDmWhileSaving(id: string, button: string, name: string, meanwhile: object, after: object) {
  test(name, async () => {
    await navigate(`${getServerUrl()}/room`)
    try {
      await page.click(`#test-room-${id}`)
      let roomId = ''
      await autoRetry(async () => {
        const r = await getResult<{ roomId: string; saving?: boolean }>('#room-result')
        expect(r.saving).toBe(true)
        roomId = r.roomId
      })
      await page.click(`#test-room-${id}-${button}`)
      await autoRetry(async () => {
        expect(await getResult('#room-result')).deep.equal({ roomId, ...meanwhile })
      })
      await runServerAction(`saveReply_${roomId}`)
      await autoRetry(async () => {
        expect(await getResult('#room-result')).deep.equal(after)
      })
    } finally {
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
  testRoomScenario<{ first: string; outcomes: string[]; got: string[] }>(
    'dm-overflow',
    'room: DMs to a page before its first listen() are held within 2 MiB and delivered on listen(); past it the oldest is dropped and its acknowledged send rejects',
    (r) => {
      expect(r.first).toBe('rejected')
      expect(r.outcomes).deep.equal(['rejected', 'answered', 'answered'])
      expect(r.got).deep.equal(['2', '3'])
    },
  )
  testRoomScenario<{
    unchanged: boolean
    received: string[]
    onServer: string[]
    stopped: boolean
    abortedWithReason: boolean
  }>('signals', 'room: abort removes registrations, and iterated messages keep their sender and track', (result) => {
    expect(result).deep.equal({
      unchanged: true,
      received: [
        'binary Alice camera: 1 {"frame":1}',
        'binary Alice screen: 2 null',
        'binary Bob camera: 3 null',
        'text Alice: hello',
        'text Bob: hi',
      ],
      onServer: ['binary Alice: 1', 'binary Alice: 2', 'binary Bob: 3', 'text Alice: hello', 'text Bob: hi'],
      stopped: true,
      abortedWithReason: true,
    })
  })
  testRoomScenario<ChatResult>('chat', 'room: join, publish with sender identity, setMeta, leave', (result) => {
    expect(result.events).deep.equal(['join:Alice', 'leave:Alice'])
    expect(result.countAfterJoin).toBe(1)
    expect(result.count).toBe(0)
    expect(result.received).deep.equal([{ text: 'hello', from: 'Alice' }])
    expect(result.ack.key).match(/^e2e-chat:/)
    expect(result.ack.seq).greaterThan(0)
    expect(result.updates).deep.equal([42])
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
    expect(result.frames.map((f) => f.track)).deep.equal(['main', 'main', 'main', 'camera'])
    expect(result.frames.map((f) => f.meta)).deep.equal([null, null, null, { key: true }])
    expect(result.cameraOnly).deep.equal([9])
    expect(result.defaultOnly).deep.equal([1, 2, 3])
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
  testRoomScenario<{ dms: string[]; packaged: unknown[] }>(
    'package-message',
    "room: a package's private message reaches the package on the page holding its recipient, joined there or returned alone, never the app's listen()",
    (r) => {
      expect(r.dms).to.have.members(['me: from the app', 'alone: from the app'])
      expect(r.packaged).deep.equal(['from the package', 'from the package'])
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
    calls: Array<{ hook: string; name: unknown; frozen: boolean; track?: string | null }>
  }>(
    'guard-calls',
    "room: guards get a frozen sender and never a stand-in for a member that left; a binary publish a guard refuses rejects with the guard's Abort; an after-hook that throws keeps the publish; both hooks get a message's track",
    (r) => {
      const tracks = (hook: string) => r.calls.filter((call) => call.hook === hook).map((call) => call.track)
      expect(tracks('publish')).include('cursor')
      expect(tracks('after-publish')).include('cursor')
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
  testRoomScenario<{ onServer: { isBytes: boolean[] }; onPage: unknown[] }>(
    'meta-bytes',
    "room: bytes in a room's and a member's meta stay bytes, on the server and on the page",
    (r) => {
      expect(r.onServer.isBytes).deep.equal([true, true])
      expect(r.onPage).deep.equal([
        [1, 2, 3],
        [4, 5, 6],
        [7, 8],
      ])
    },
  )
  testRoomScenario<{
    leave: string
    kick: string
    close: string
    atLeave: { cause: string; isClosed: boolean; count: number } | null
    unsubscribeInOnClose: string | null
    closeCause: unknown
    lateOnClose: unknown
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
      expect(r.closeCause).deep.equal({ type: 'closed' })
      expect(r.lateOnClose).deep.equal({ type: 'closed' })
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
  testRoomScenario<{ otherWorkRan: boolean }>(
    'publish-loop',
    'room: a loop of awaited publishes leaves the event loop its turns',
    (r) => {
      expect(r.otherWorkRan).toBe(true)
    },
  )
  testRoomScenario<{ thrower: number; healthy: number }>(
    'throwing-receiver',
    "room: a store's receiver that throws is reported, and the receiver beside it and the frames after it still arrive",
    (r) => {
      expect(r).deep.equal({ thrower: 2, healthy: 2 })
      expectLog('Test: a receiver throws')
    },
  )
  testRoomScenario<{
    unguardedError: string | null
    guardedError: string | null
    guardRuns: number
    duringGuardError: string | null
  }>(
    'join-closed',
    "room: a join through a handle that hasn't heard of its room's close fails as closed, without running the guard or logging anything; so does one whose guard closes the room",
    (r) => {
      expect(r.unguardedError).toBe('Room is closed: <id>:unguarded')
      expect(r.guardedError).toBe('Room is closed: <id>:guarded')
      expect(r.guardRuns).toBe(0)
      expect(r.duringGuardError).toBe('Room is closed: <id>:during-guard')
    },
  )
  testRoomScenario<{
    fromPage: unknown
    fromPageUnsendable: unknown
    fromServer: unknown
    fromHeld: unknown
    fromHeldUnsendable: unknown
    fromObserved: unknown
    fromCompeting: unknown
    refused: unknown
    unhandled: string[]
  }>(
    'ack-dm',
    "room: an acknowledged DM resolves with its recipient's reply, from a page, a server member or a server member a page holds, and rejects with the recipient's Abort, or an error when a reply from a page or from a server member a page holds can't be serialized, without an unhandled rejection",
    (r) => {
      expect(r.fromPage).deep.equal({ echo: 'hi' })
      expect(r.fromPageUnsendable).toBe("Internal client room error — see the recipient's client logs")
      expect(r.fromServer).deep.equal({ echo: 'hi' })
      expect(r.fromHeld).toBe('pong')
      expect(r.fromHeldUnsendable).toBe("Internal client room error — see the recipient's client logs")
      expect(r.fromObserved).toBe('ok')
      expect(r.fromCompeting).toBe('Only one listen() callback may return a reply')
      expect(r.refused).toBe('refused')
      expect(r.unhandled).deep.equal([])
    },
  )
  testRoomAckDmWhileSaving(
    'ack-dm-close',
    'chat',
    'room: a server member a page holds and closes while it saves its reply to an acknowledged DM sends the reply, and the close finishes after it',
    { closing: true },
    { answer: 'bye', closed: true },
  )
  testRoomAckDmWhileSaving(
    'ack-dm-close-view',
    'chat',
    'room: a member a page joined, whose room view the page closes while the member saves its reply to an acknowledged DM, sends the reply, and the close finishes after it',
    { closing: true },
    { answer: 'bye', closed: true },
  )
  testRoomAckDmWhileSaving(
    'ack-dm-hang-up',
    'now',
    "room: an acknowledged DM to a server member a page holds rejects with the member's leave at once when the page aborts it while it saves its reply, with no unhandled rejection after the save",
    { fromHangingUp: 'Participant left the room' },
    { fromHangingUp: 'Participant left the room', unhandled: [] },
  )
  testRoomScenario<{ listed: number; serverCount: number; count: number; emptied: boolean }>(
    'roster-churn',
    'room: a page opening a room of 2,000 members while members join, leave and change meta gets its roster, counts only visible members, and sees no empty room',
    (r) => {
      expect(r.listed).greaterThanOrEqual(2000)
      expect(r.serverCount).toBe(2001) // 2,000 members and the page's; not the hidden one
      expect(r.count).toBe(r.serverCount)
      expect(r.emptied).toBe(false)
    },
  )
  testRoomScenario<{ closed: number; reported: string[] }>(
    'close-while-subscribing',
    'room: a room closed while a view of it subscribes closes the view, and no bug is reported',
    (r) => {
      expect(r).deep.equal({ closed: 10, reported: [] })
    },
  )
  testRoomScenario<MemberResult>(
    'member',
    'room: a returned RemoteParticipant is the same object as the room view, and a server lists it at once after Room.get()',
    (result) => {
      expect(result.hasMember).toBe(true)
      expect(result.memberName).toBe('Viewed')
      expect(result.sameObject).toBe(true) // ref-identity binds the view to its room
      expect(result.listedOnServer).deep.equal(['Viewed'])
    },
  )
  testRoomScenario<{ identity: string; hidden: string; listHidden: string }>(
    'server-only',
    "room: a page's join({ identity }), join({ hidden }) and getParticipants({ hidden }) fail with what only a server may do",
    (r) => {
      expect(r).deep.equal({
        identity:
          '[@telefunc/room@0.1.0][Wrong Usage] join() options.identity is server-assigned: identity is trusted, so set ' +
          'it where trust lives, in the granting telefunction (server-side join()), not on the client.',
        hidden:
          '[@telefunc/room@0.1.0][Wrong Usage] join() options.hidden is server-side only: a hidden participant is ' +
          'created by the granting telefunction (server-side join({ hidden: true })), not by a client.',
        listHidden: '[@telefunc/room@0.1.0][Wrong Usage] Hidden participants can only be enumerated on the server',
      })
    },
  )
  testRoomScenario<{ arrived: unknown; joins: unknown[] }>(
    'complete-room',
    'room: a page gets a room with its participants: on the next line both reads list them and the snapshot shows them, and a later join reaches onJoin',
    (r) => {
      expect(r.arrived).deep.equal({ listed: ['A', 'B'], shown: ['A', 'B'], count: 2, one: 'A' })
      expect(r.joins).deep.equal(['C'])
    },
  )
  testRoomScenario<{
    kept: boolean[]
    replaced: boolean
    status: (string | null)[]
    fromA: unknown[]
    revived: { sameObject: boolean; name: string }
  }>(
    'snapshot-grid',
    "room: a snapshot's participants act as the participants, and one that changes is the only one replaced",
    (r) => {
      expect(r.kept).deep.equal([true, true])
      expect(r.replaced).toBe(true)
      expect(r.status).deep.equal([null, 'away'])
      expect(r.fromA).deep.equal(['from A'])
      expect(r.revived).deep.equal({ sameObject: true, name: 'C' })
    },
  )
  testRoomScenario<GuardResult>(
    'guard',
    "room: guards reject over the wire, a room returned unguarded refuses a page's join, and allowed messages flow",
    (result) => {
      expect(result.joinError).toBe('blocked join of Banned')
      // A room returned without Room.guard() refuses a page's join, and says how to allow it
      expect(result.unguardedJoinError).toContain('Room.guard(room, { anyoneCanJoin: true })')
      expect(result.publishError).toBe('blocked publish from Mallory')
      expect(result.trackError).toBe('blocked publish from Mallory')
      expect(result.sendError).toBe('blocked send from Mallory')
      expect(result.received).deep.equal(['fine'])
      expect(result.inbox).deep.equal(['psst'])
    },
  )
  testRoomScenario<{ okAck: boolean; badError: string | null; trackBadError: string | null; received: string[] }>(
    'shield',
    'room: the declared message type is shielded at runtime (a malformed publish is rejected)',
    (r) => {
      expect(r.okAck).toBe(true)
      // The generated shield rejects malformed ingress through the publish promise.
      expect(r.badError).toBe('ShieldValidationError')
      expect(r.trackBadError).toBe('ShieldValidationError')
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
  testRoomDemand()
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
  testRoomScenario<{ roomFirst: OwnTrack; meFirst: OwnTrack }>(
    'self-separate',
    "room: selfDelivery: false holds on the page's view of the room when the room and `me` come from two telefunction calls, in either order",
    (r) => {
      expect(r).deep.equal({ roomFirst: othersOnly, meFirst: othersOnly })
    },
  )
  testRoomScenario<OwnTrack>(
    'self-two-connections',
    "room: selfDelivery: false holds on the page's view of the room when `me` comes from a call on another of the page's connections",
    (r) => {
      expect(r).deep.equal(othersOnly)
    },
  )
  testRoomScenario<OwnTrack>(
    'self-returned',
    "room: selfDelivery: false holds on the page's view of the room for what `me` publishes right after a telefunction returned it",
    (r) => {
      expect(r).deep.equal(othersOnly)
    },
  )
  testRoomScenario<{ earlier: string[]; returned: string[]; selfDelivery: boolean }>(
    'self-greeting',
    'room: a message a telefunction publishes as its selfDelivery:false participant before returning it never reaches the room returned with it, and nothing it publishes after the return reaches the page',
    (r) => {
      // A room the page already had open may get the greeting: on one server it does, elsewhere it may arrive too late.
      expect(r.earlier.filter((data) => data !== 'greeting')).deep.equal(['bye'])
      expect(r).deep.include({ returned: ['bye'], selfDelivery: false })
    },
  )
  testRoomScenario<{ heard: string[] }>(
    'self-server-listener',
    "room: a server's room that joined a selfDelivery:false participant doesn't get its messages, before or after a page holds it",
    (r) => {
      expect(r.heard).deep.equal(['other'])
    },
  )
  testRoomScenario<{ myCause: unknown; closes: unknown[]; isClosed: boolean }>(
    'self-removed',
    "room: removing the page's participant closes the page's view of the room when they came over two of the page's connections",
    (r) => {
      const removed = { type: 'removed', reason: 'be nice' }
      expect(r).deep.equal({ myCause: removed, closes: [removed], isClosed: true })
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
    expect(r.updates).toContain('updated') // room.onChange fired with the new meta
    expect(r.topic).toBe('updated')
    expect([...r.listed].sort()).deep.equal([...r.expectedIds].sort())
    expect(r.sameId).toBe(true)
    expect(r.sameCount).toBe(1) // ...with exactly its original member preserved
  })
  testRoomScenario<{ alone: RemovedView & { myCause: unknown; pubCause: unknown }; two: RemovedView }>(
    'removed',
    "room: a removed participant's page stops receiving the room it got from another call, and a page holding another participant keeps it",
    (r) => {
      const removed = { type: 'removed', reason: 'be nice' }
      // Its participant leaves with the removal, the view closes with it and gets nothing after, and another participant
      // on the view leaves it as a closed view's do, not as removed.
      expect(r.alone).deep.equal({
        text: ['0'],
        closes: 1,
        closeCause: removed,
        isClosed: true,
        myCause: removed,
        pubCause: { type: 'closed' },
      })
      expect(r.two).deep.equal({ text: ['still here'], closes: 0, closeCause: null, isClosed: false })
    },
  )
  test('room: a removal by identity closes the room on each tab holding that participant, and a tab that never joined keeps receiving', async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-removed-identity')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-removed-identity:/)
    })
    const context = await page.context().browser()!.newContext()
    const tabs = [await context.newPage(), await context.newPage()]
    const tabResult = async (tab: (typeof tabs)[number]) => JSON.parse((await tab.textContent('#room-result'))!)
    try {
      for (const tab of tabs) {
        await navigate(`${getServerUrl()}/room?roomId=${encodeURIComponent(roomId)}`, undefined, tab)
        await tab.click('#test-room-removed-identity-held')
        await autoRetry(async () => expect(await tabResult(tab)).deep.equal({ ready: true }))
      }
      await page.click('#test-room-removed-identity-remove')
      await autoRetry(async () => expect(await getResult('#room-result')).deep.equal({ text: ['0'], isClosed: false }))
      for (const tab of tabs) {
        await autoRetry(async () =>
          expect(await tabResult(tab)).deep.equal({
            text: [],
            closes: 1,
            cause: { type: 'removed', reason: 'multi-tab' },
            isClosed: true,
          }),
        )
      }
    } finally {
      await context.close()
      await page.goto('about:blank')
    }
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
  for (const kind of ['request', 'publish', 'reply'] as const)
    test(`room: a page that sends a malformed Room ${kind} loses its connection: its member leaves, and the room keeps working for the others`, async () => {
      await navigate(`${getServerUrl()}/room`)
      await page.click('#test-room-malformed')
      const { roomId } = await autoRetry(() => getResult<{ roomId: string }>('#room-result'))
      const context = await page.context().browser()!.newContext()
      try {
        const other = await context.newPage()
        await navigate(`${getServerUrl()}/room?roomId=${encodeURIComponent(roomId)}`, undefined, other)
        await other.click(`#test-room-malformed-${kind}`)
        await autoRetry(async () => {
          expect(await getResult('#room-result')).deep.equal({
            roomId,
            events: ['join:Witness', 'join:Mallory', 'leave:Mallory:disconnected'],
            received: ['bye Mallory'],
            // In the `reply` case, Mallory answers this DM malformed.
            dm: 'rejected',
          })
        })
      } finally {
        await context.close()
        await page.goto('about:blank')
      }
    })
  test("room: another page's member shows when its meta changes and its page drops and comes back, through onChange and its events, and leaves with 'disconnected' once its page never returns", async () => {
    if (!canCutWire()) return
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
    await other.click('#test-room-join-only-away')
    await autoRetry(async () => expect(await other.textContent('#room-result')).toBe('"away"'))
    const away = ['change:away:true']
    await autoRetry(async () => expect(await seen()).deep.equal({ events: away, closerConnected: true }))
    await context.setOffline(true)
    await cutWire()
    const dropped = [...away, 'change:away:false', 'disconnect:false']
    await autoRetry(async () => expect(await seen()).deep.equal({ events: dropped, closerConnected: false }))
    await context.setOffline(false)
    const back = [...dropped, 'change:away:true', 'reconnect:true']
    await autoRetry(async () => expect(await seen()).deep.equal({ events: back, closerConnected: true }))
    await context.setOffline(true)
    await cutWire()
    await context.close()
    await autoRetry(async () => {
      expect(await seen()).deep.equal({
        events: [...back, 'change:away:false', 'disconnect:false', 'leave:disconnected'],
        closerConnected: null,
      })
    }, CLOSED_TAB_WAIT)
  })
  test("room: in a chat, everyone sees a member whose tab went offline as reconnecting, in the list, the activity, Who's online and that member's profile, and as back once it is; the member sees itself reconnecting at once, in the list and on its own profile, and back at once with no flicker", async () => {
    if (!canCutWire()) return
    const chatId = `e2e-chat:${crypto.randomUUID()}`
    const alice = await openChat(chatId, 'Alice', 'join')
    const bob = await openChat(chatId, 'Bob', 'join')
    try {
      await autoRetry(async () =>
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob'] }),
      )
      await bob.tab.click('#chat-members button:has-text("Bob (you)")')
      await autoRetry(async () => expect(await bob.tab.textContent('#chat-profile')).toBe('Bob: online'))
      await bob.context.setOffline(true)
      await cutWire()
      await autoRetry(async () => {
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob (reconnecting…)'] })
        expect(await alice.activity('Bob')).deep.equal(['Bob lost connection'])
        expect(await bob.sees()).deep.equal({
          status: 'Reconnecting…',
          members: ['Alice', 'Bob (you) (reconnecting…)'],
        })
        expect(await bob.activity('Bob')).deep.equal(['Bob lost connection'])
        expect(await bob.tab.textContent('#chat-profile')).toBe('Bob: reconnecting…')
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
      })
      const bobBack = async () => {
        expect(await bob.sees()).deep.equal({ status: 'Connected', members: ['Alice', 'Bob (you)'] })
        expect(await bob.activity('Bob')).deep.equal(['Bob lost connection', 'Bob is connected again'])
        expect(await bob.tab.textContent('#chat-profile')).toBe('Bob: online')
      }
      await autoRetry(bobBack)
      // The room's own record of Bob's drop and return reaches his tab as it comes back, and changes nothing he sees.
      await sleep(1_000)
      await bobBack()
      await alice.tab.click('#chat-who')
      await autoRetry(async () => expect(await alice.tab.textContent('#chat-online')).toBe('Alice, Bob'))
    } finally {
      await alice.context.close()
      await bob.context.close()
    }
  })
  test("room: in a members-only chat, a member whose tab goes offline while the membership service decides is shown reconnecting once admitted, and back once its tab is, and its tab then shows the member's own renames", async () => {
    if (!canCutWire()) return
    const chatId = `e2e-chat:${crypto.randomUUID()}`
    const alice = await openChat(chatId, 'Alice', 'join')
    const context = await page.context().browser()!.newContext()
    const tab = await context.newPage()
    try {
      await navigate(`${getServerUrl()}/room?chat=${encodeURIComponent(chatId)}&as=Bob`, undefined, tab)
      await tab.click('#chat-join-member')
      const lookup = `chatMember_${chatId}_Bob`
      await autoRetry(async () => expect((await getCleanupState())[lookup]).toBe('asked'))
      await context.setOffline(true)
      await cutWire()
      await runServerAction(lookup)
      await autoRetry(async () =>
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob (reconnecting…)'] }),
      )
      await alice.tab.click('#chat-who')
      await autoRetry(async () => expect(await alice.tab.textContent('#chat-online')).toBe('Alice, Bob (away)'))
      await context.setOffline(false)
      await autoRetry(async () =>
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob'] }),
      )
      // Bob's join ends only now: his tab heard, while he was joining, that he started disconnected and came back.
      await runServerAction(`chatWelcome_${chatId}_Bob`)
      await autoRetry(async () => {
        expect(await tab.textContent('#chat-status')).toBe('Connected')
        expect(await tab.locator('#chat-members li').allTextContents()).deep.equal(['Alice', 'Bob (you)'])
      })
      await alice.tab.click('#chat-who')
      await autoRetry(async () => expect(await alice.tab.textContent('#chat-online')).toBe('Alice, Bob'))
      await tab.click('#chat-rename')
      await autoRetry(async () => {
        expect(await tab.textContent('#chat-rename')).toBe('Bob!')
        expect(await alice.sees()).deep.equal({ status: 'Connected', members: ['Alice (you)', 'Bob!'] })
      })
    } finally {
      await alice.context.close()
      await context.close()
    }
  })
  test("room: in a chat, the server announces when a signed-in member's tab goes offline and comes back, and someone watching unseen drops without anyone seeing it; the signed-in member sees itself reconnecting, in the list and on its own profile", async () => {
    if (!canCutWire()) return
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
      await carol.tab.click('#chat-members button:has-text("Carol (you)")')
      await autoRetry(async () => expect(await carol.tab.textContent('#chat-profile')).toBe('Carol: online'))
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
        expect(await carol.sees()).deep.equal({
          status: 'Reconnecting…',
          members: ['Alice', 'Carol (you) (reconnecting…)'],
        })
        expect(await carol.tab.textContent('#chat-profile')).toBe('Carol: reconnecting…')
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
        expect(await carol.sees()).deep.equal({ status: 'Connected', members: ['Alice', 'Carol (you)'] })
        expect(await carol.tab.textContent('#chat-profile')).toBe('Carol: online')
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
    frames: Array<[string, number, string]>
    later: string[]
    server: { text: string[]; frames: number[]; lossy: number[] }
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
      expect(r.server).deep.equal({ text: ['pinned', 'live', 'live-2'], frames: [1, 4], lossy: [] })
    },
  )
  testRoomScenario<{ frames: Array<[string, number, unknown]>; server: Array<[number, unknown]> }>(
    'retain-leave',
    "room: a member who leaves while a late subscriber's replay holds its live frames has none of them delivered after its leave, on the page or the server, and another member's held frames still arrive",
    (r) => {
      expect(r.frames).deep.equal([
        ['cam', 5, 'B'],
        ['cam', 9, 'B'],
      ])
      expect(r.server).deep.equal([
        [5, 'B'],
        [9, 'B'],
      ])
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
      expect(r.memberEvents).deep.equal(['change:Bot2', 'leave:left'])
      expect(r.empty).toBe(true)
      expect(r.alone).deep.equal({ cause: { type: 'closed' }, closed: true })
    },
  )
  testRoomScenario<{ names: string[]; count: number; causes: Array<string | null>; listed: boolean[] }>(
    'departed-returned',
    'room: a member returned after it left, held across an await, from an older snapshot or hidden, has left on the page too',
    (r) => {
      expect(r).deep.equal({
        names: [],
        count: 0,
        causes: ['unknown', 'unknown', 'unknown'],
        listed: [false, false, false],
      })
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
    demand: Array<[string, boolean]>
    wanted: number
    pubOwn: Array<[string, boolean]>
    otherOwn: Array<[string, boolean]>
    tooMany: string | null
  }>(
    'tracks',
    "room: each of 1,000 named tracks reaches its subscriber in order, me.demand follows the wants, and a selfDelivery:false member's own side is no demand",
    (r) => {
      expect(r.inOrder).toBe(1_000)
      expect(r.defaultTrack).deep.equal([7])
      expect(r.demand.filter(([track]) => track !== 'main')).deep.equal([
        ['t0', true],
        ['t999', true],
        ['t0', false],
      ])
      expect(r.demand.filter(([track]) => track === 'main')).deep.equal([['main', true]])
      expect(r.wanted).toBe(1_002)
      expect(r.pubOwn).deep.equal([['own', true]])
      expect(r.otherOwn).deep.equal([])
      expect(r.tooMany).include('[Wrong Usage] Too many binary tracks subscribed on one room (4096 max)')
    },
  )
  test('room: a page back from offline gets what its room sent meanwhile, in order, and the leaves, kick and close that happened while it was away, with their causes', async () => {
    if (!canCutWire()) return
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
  test("room: a page back from offline, twice, gets its room's meta each time, as an attach sends it again", async () => {
    if (!canCutWire()) return
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-meta-away')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-meta-away:/)
    })
    for (const absence of [1, 2]) {
      await page.context().setOffline(true)
      await cutWire()
      await runServerAction(`roomMetaAway_${roomId}`)
      await autoRetry(async () => {
        expect((await getCleanupState())[`roomMetaAway_${roomId}`]).toBe(`done-${absence}`)
      })
      await page.context().setOffline(false)
      await autoRetry(async () => {
        expect(await getResult('#room-result')).deep.equal({ roomId, topic: `away-${absence}`, filler: `${absence}` })
      })
    }
  })
  test("room: on a 1 Mbit/s link, a page gets 640 KB of its room's messages and publishes 640 KB without reconnecting", async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    let sseStreams = 0
    const onResponse = (response: Response) => {
      if (response.headers()['content-type']?.startsWith('text/event-stream')) sseStreams++
    }
    let webSockets = 0
    const onWebSocket = (ws: WebSocket) => {
      if (ws.url().includes('/_telefunc')) webSockets++
    }
    try {
      await navigate(`${proxy.url}/room`)
      page.on('response', onResponse)
      page.on('websocket', onWebSocket)
      // 1 Mbit/s each way.
      proxy.pace({ downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 })
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
      page.off('response', onResponse)
      page.off('websocket', onWebSocket)
      // The page leaves before the proxy closes, which would cut its connections.
      await page.goto('about:blank')
      await proxy.close()
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
  test('room: a page that reconnects while its join is pending gets the leave, with its cause, of a member removed while it was away', async () => {
    if (!canCutWire()) return
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

/** Who receives a participant's tracks: `me.demand`, and the `onChange` it is read on. */
function testRoomDemand() {
  testRoomScenario<{ cam: boolean[] }>(
    'demand',
    'room: a track is in me.demand while a subscriber receives it, and leaves it when the subscriber stops',
    (r) => {
      expect(r.cam).toContain(true) // a viewer arrived (the track is watched)
      expect(r.cam[r.cam.length - 1]).toBe(false) // ...and left again, back to unwatched
    },
  )
  testRoomScenario<Record<'docsPage' | 'twoOnOnePage' | 'nameless' | 'hidden' | 'oneTask', NamedDemand[][]>>(
    'watchers',
    "room: me.demand lists the people at each page that receives a track and counts them: `me` beside the page's watching view, two people on one page, one for a page with nobody to name, a hidden one never named; changes of one task call onChange once",
    (r) => {
      expect(r.docsPage.at(-1)).deep.equal([cam(['Me'], 1)])
      expect(r.twoOnOnePage.at(-1)).deep.equal([cam(['Ann', 'Bob'], 2)])
      expect(r.nameless.at(-1)).deep.equal([cam([], 1)])
      expect(r.hidden.flat().flatMap(({ watchers }) => watchers)).deep.equal([])
      expect(r.hidden.at(-1)).deep.equal([cam([], 1)])
      // Two tracks a server-side room started receiving in one task: each is told once its lane delivers, and on Redis the
      // two lanes can deliver in two tasks, the first track then told alone first.
      const mic = { track: 'mic', watchers: [], watcherCount: 1 }
      expect(r.oneTask.at(-1)).deep.equal([cam([], 1), mic])
      expect([[], [[cam([], 1)]], [[mic]]]).deep.include(r.oneTask.slice(0, -1))
    },
  )
  testRoomScenario<{ reads: NamedDemand[][]; sameList: boolean; sameEntry: boolean; tracks: NamedDemand[] }>(
    'watchers-on-page',
    "room: a page's me.demand names the people at a server-side room that receives its track, counts a room with nobody to name, and keeps its array and entries while they don't change",
    (r) => {
      expect(r.reads).deep.include([cam(['Sam', 'Tia'], 2)])
      expect(r.reads).deep.include([cam(['Sam', 'Tia'], 3)])
      expect(r.sameList).toBe(true) // a meta change called onChange, and demand stayed the same array
      expect(r.sameEntry).toBe(true) // `mic` came, and the `cam` entry stayed the same object
      expect(r.tracks).deep.equal([cam(['Sam', 'Tia'], 3), { track: 'mic', watchers: [], watcherCount: 1 }])
    },
  )
  test("room: a view that watches beside the page's `me` while the page's connection upgrades to a WebSocket shows its publisher `me` throughout, never a watcher it can't name", async () => {
    const transports: string[] = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!)
    if (!transports.includes('sse') || !transports.includes('ws')) return
    const context = await page.context().browser()!.newContext()
    try {
      // The page's WebSocket opens once the page watches: the upgrade then moves a watching page.
      const { promise: watching, resolve: watched } = Promise.withResolvers<void>()
      await context.routeWebSocket(/\/_telefunc/, async (ws) => {
        await watching
        ws.connectToServer()
      })
      const tab = await context.newPage()
      let upgradedFrames = 0
      tab.on('websocket', (ws) => ws.on('framereceived', () => upgradedFrames++))
      await navigate(`${getServerUrl()}/room`, undefined, tab)
      await tab.click('#test-room-watchers-upgrade')
      await autoRetry(async () =>
        expect(JSON.parse((await tab.textContent('#room-result'))!)).deep.equal({ watching: true }),
      )
      watched()
      await autoRetry(() => expect(upgradedFrames).greaterThan(0))
      // The upgrade's join, and the planning pass after it, had their chance to tell the publisher a new page.
      await sleep(1000)
      await tab.click('#test-room-watchers-upgrade-check')
      await autoRetry(async () => {
        const { reads } = JSON.parse((await tab.textContent('#room-result'))!) as { reads: NamedDemand[][] }
        expect(reads.length).greaterThan(0)
        for (const read of reads) expect(read).deep.equal([cam(['Me'], 1)])
      })
    } finally {
      await context.close()
    }
  })
  test("room: selfDelivery: false holds on the page's view of the room from another call after the page's connection upgrades to a WebSocket", async () => {
    const transports: string[] = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!)
    if (!transports.includes('sse') || !transports.includes('ws')) return
    const context = await page.context().browser()!.newContext()
    try {
      // The page's WebSocket opens once `me` and the room are on the page: the upgrade then moves both.
      const { promise: ready, resolve: release } = Promise.withResolvers<void>()
      await context.routeWebSocket(/\/_telefunc/, async (ws) => {
        await ready
        ws.connectToServer()
      })
      const tab = await context.newPage()
      let upgradedFrames = 0
      tab.on('websocket', (ws) => ws.on('framereceived', () => upgradedFrames++))
      await navigate(`${getServerUrl()}/room`, undefined, tab)
      await tab.click('#test-room-self-upgrade')
      await autoRetry(async () =>
        expect(JSON.parse((await tab.textContent('#room-result'))!)).deep.equal({ ready: true }),
      )
      release()
      await autoRetry(() => expect(upgradedFrames).greaterThan(0))
      // The upgrade's reconcile had its chance to move every end of the page.
      await sleep(1000)
      await tab.click('#test-room-self-upgrade-publish')
      await autoRetry(async () => expect(JSON.parse((await tab.textContent('#room-result'))!)).deep.equal(othersOnly))
    } finally {
      await context.close()
    }
  })
  testRoomScenario<{ changes: number; changed: boolean }>(
    'change-once',
    'room: room.onChange is called once for the changes of one task, and me.onChange works unbound',
    (r) => {
      expect(r).deep.equal({ changes: 1, changed: true })
    },
  )
}

/** Tests that need the failing backend (failing-backend.ts), which a server without Redis installs. */
function testRoomFailingBackend() {
  test("room: a store write that fails once heals: a failed leave can be retried, a failed join leaves no member, a leave whose event and reply are lost ends once with 'left', a DM whose handoff is lost rejects its send, and a lane the store drops delivers again", async () => {
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
        lost: {
          leave: 'Internal Server Error — see server logs',
          leaves: ['Leaver:left'],
          dm: "Message may not be delivered: its handoff to the recipient failed, and it isn't sent again",
          dms: ['after'],
        },
        lane: { dropped: true, got: ['before', 'after'] },
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

/** Runs in a lane whose server has the smallest `maxMemory`, 2 MiB. */
function testRoomSmallMemory() {
  test('room: a page offline while its room sent it more than maxMemory holds is let go at its reconnect: its view closes and its member leaves, with no gap delivered', async () => {
    if (!canCutWire()) return
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
  test('room: a page offline while its server-joined participant was sent more than maxMemory holds is let go at its reconnect: its participant leaves, with no gap delivered', async () => {
    if (!canCutWire()) return
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-participant-overflow')
    let roomId = ''
    await autoRetry(async () => {
      roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
      expect(roomId).match(/^e2e-participant-overflow:/)
    })
    await page.context().setOffline(true)
    await cutWire()
    await runServerAction(`participantOverflow_${roomId}`)
    await autoRetry(async () => {
      expect((await getCleanupState())[`participantOverflow_${roomId}`]).toBe('done')
    })
    await page.context().setOffline(false)
    await autoRetry(async () => {
      expect(await getResult('#room-result')).deep.equal({ roomId, got: ['before'], myCause: { type: 'disconnected' } })
    })
  })
  testRoomScenario<{ seen: number[]; closed: boolean }>(
    'behind',
    'room: a connected page that falls behind what the server holds is let go, with no gap in what it got',
    (r) => {
      expect(r.closed).toBe(true)
      expect(r.seen.length).lessThan(16)
      expect(r.seen).deep.equal(r.seen.map((_, n) => n))
    },
  )
  testRoomScenario<Record<string, string | null>>(
    'track-lossy',
    "room: each publish on a track says lossy itself, a track takes publishes with and without it, and a message's lossy: true needs a track",
    ({ noTrack, ...accepted }) => {
      expect(accepted).deep.equal({
        first: null,
        omitted: null,
        text: null,
        plain: null,
        lossyAfterPlain: null,
        defaultTrack: null,
      })
      expect(noTrack ?? 'no error').toContain('publish() options.lossy: true needs options.track')
    },
  )
  testRoomScenario<{ closeCause: unknown; myCause: unknown }>(
    'meta-flood',
    "room: a connected page whose room's events alone are more than the server holds for it is let go",
    (r) => {
      expect(r).deep.equal({ closeCause: { type: 'disconnected' }, myCause: { type: 'disconnected' } })
    },
  )
  testRoomScenario<{ receipts: number; seen: number[]; listed: number }>(
    'lossy-uplink',
    "room: lossy frames that wait for the page's uplink while it reconnects are replaced by the newest of their track, which its viewers get, their publishes share its receipt, and none stays listed once settled",
    (r) => {
      expect(r.listed).toBe(0)
      expect(r.receipts).lessThan(20)
      expect(r.seen.at(-1)).toBe(19)
      expect(r.seen).deep.equal([...r.seen].sort((a, b) => a - b))
    },
  )
  testRoomScenario<{ refused: string | null; next: string; closed: boolean; myCause: unknown; leaves: unknown[] }>(
    'meta-past-memory',
    'room: a setMeta() larger than maxMemory rejects with ChannelOverflowError, its page stays in the room and the others see no leave',
    (r) => {
      expect(r).deep.equal({
        refused: 'ChannelOverflowError',
        next: 'accepted',
        closed: false,
        myCause: null,
        leaves: [],
      })
    },
  )
  testRoomTracksBehind(
    'cursor',
    "room: a page offline while its room sent it more than maxMemory holds in lossy frames gets each track's newest frame, with the jump in its info.seq, and gets the reliable track and the messages whole",
    (r) => {
      expect(r).deep.include({ closed: false, myCause: null })
      const all = Array.from({ length: 16 }, (_, n) => n)
      expect(r.text.map(({ data }) => data)).deep.equal(['before', ...all.map(String)])
      expect(r.drawing.map(({ n }) => n)).deep.equal(all)
      for (const stream of [r.text, r.drawing]) {
        expect(stream.map(({ seq }) => seq - stream[0]!.seq)).deep.equal(stream.map((_, i) => i))
      }
      // Each gets its newest, the pointer's too though it went quiet first, and its seq jumps by as many as it missed.
      for (const [stream, count] of [
        [r.pointer, 8],
        [r.cursor, 16],
      ] as const) {
        expect(stream.length).lessThan(count)
        expect(stream.at(-1)?.n).toBe(count - 1)
        expect(stream.map(({ n, seq }) => seq - n)).deep.equal(stream.map(() => stream[0]!.seq - stream[0]!.n))
      }
    },
  )
  testRoomTracksBehind(
    'drawing',
    'room: a page offline while its room sent it more than maxMemory holds on a reliable track is let go at its reconnect, with no gap in that track',
    (r) => {
      expect(r).deep.include({ closed: true, myCause: { type: 'disconnected' } })
      expect(r.drawing.map(({ n }) => n)).deep.equal(r.drawing.map((_, i) => i))
    },
  )
  test('room: a page offline while a text track sent it more than maxMemory holds in lossy messages gets the newest one, with the jump in its info.seq, and stays in its room; a publish on the track without lossy is never skipped, and arrives after the lossy ones before it', async () => {
    if (!canCutWire()) return
    const roomId = await runServerActionOffline('text-cursor', 'roomTextCursor')
    const plain = (await getCleanupState())[`roomTextCursorPlain_${roomId}`]
    await autoRetry(async () => {
      const r = await getResult<{ cursor?: { n: number | 'plain'; seq: number }[]; closed: boolean; myCause: unknown }>(
        '#room-result',
      )
      expect(r).deep.include({ closed: false, myCause: null })
      expect(plain).toBe('published')
      const cursor = r.cursor!
      expect(cursor.length).lessThan(18)
      expect(cursor.at(-1)?.n).toBe(15)
      // The order they were published in: each arrives in it, its seq jumping by as many as were skipped before it.
      const sent = [
        -1,
        ...Array.from({ length: 8 }, (_, n) => n),
        'plain',
        ...Array.from({ length: 8 }, (_, n) => n + 8),
      ]
      expect(cursor.map(({ n }) => n)).toContain('plain')
      expect(cursor.map(({ n, seq }) => seq - sent.indexOf(n))).deep.equal(
        cursor.map(() => cursor[0]!.seq - sent.indexOf(cursor[0]!.n)),
      )
      expect(cursor.map(({ n }) => sent.indexOf(n))).deep.equal(
        cursor.map(({ n }) => sent.indexOf(n)).sort((a, b) => a - b),
      )
    })
  })
  test('room: a page offline while reliable messages pass 7/8 of maxMemory, its publish waiting for a receipt, is let go: no message and no receipt crosses what it missed', async () => {
    if (!canCutWire()) return
    await runServerActionOffline('reliable-gap', 'roomReliableGap')
    await autoRetry(async () => {
      const r = await getResult<{ received?: string[]; receipt: string | null; closed: boolean; myCause: unknown }>(
        '#room-result',
      )
      expect(r).deep.equal({
        ...r,
        received: ['before'],
        receipt: 'ChannelOverflowError',
        closed: true,
        myCause: { type: 'disconnected' },
      })
    })
  })
  test("room: a page offline with only lossy frames waiting is not let go over them where the server still holds each track's newest", async () => {
    if (!canCutWire()) return
    await runServerActionOffline('lossy-near-budget', 'roomLossyNearBudget')
    await autoRetry(async () => {
      const { tracks, closed } = await getResult<{ tracks?: string[]; closed?: boolean }>('#room-result')
      expect(closed).toBe(false)
      expect(tracks).deep.equal(['a', 'b', 'c'])
    })
  })
}

/** Runs the room scenario `id`, whose result names its room, then its server `action` while the page is offline; the
 *  room's id. */
async function runServerActionOffline(id: string, action: string): Promise<string> {
  await navigate(`${getServerUrl()}/room`)
  await page.click(`#test-room-${id}`)
  let roomId = ''
  await autoRetry(async () => {
    roomId = (await getResult<{ roomId: string }>('#room-result')).roomId
    expect(roomId).match(new RegExp(`^e2e-${id}:`))
  })
  await page.context().setOffline(true)
  await cutWire()
  await runServerAction(`${action}_${roomId}`)
  await autoRetry(async () => {
    expect((await getCleanupState())[`${action}_${roomId}`]).toBe('done')
  })
  await page.context().setOffline(false)
  return roomId
}

type TracksBehind = {
  text: { data: string; seq: number }[]
  pointer: { n: number; seq: number }[]
  cursor: { n: number; seq: number }[]
  drawing: { n: number; seq: number }[]
  closed: boolean
  myCause: unknown
}

/** A page offline while its room's speaker sends 8 lossy `pointer` frames, then 16 rounds of a lossy `cursor` frame, a
 *  reliable `drawing` frame and a message, the `scenario`'s track 4 MiB in all. */
function testRoomTracksBehind(scenario: 'cursor' | 'drawing', name: string, validate: (r: TracksBehind) => void) {
  test(name, async () => {
    if (!canCutWire()) return
    await runServerActionOffline(`tracks-${scenario}`, 'roomTracks')
    await autoRetry(async () => validate(await getResult<TracksBehind>('#room-result')))
  })
}

/** Runs on Redis. */
function testRoomPublishHold() {
  testRoomScenario<{ serverOutcomes: string[]; seen: string[] }>(
    'hold-overflow',
    'room: publishes held while a subscription establishes fit 2 MiB by their bytes, and the one past it rejects with ChannelOverflowError',
    (r) => {
      expect(r.serverOutcomes).deep.equal(['sent', 'sent', 'ChannelOverflowError'])
      expect(r.seen).deep.equal(['1', '2'])
    },
  )
  testRoomScenario<{ serverOutcomes: string[]; seen: string[] }>(
    'hold-alone',
    'room: a publish held alone goes through past 2 MiB, and a second one rejects with ChannelOverflowError',
    (r) => {
      expect(r.serverOutcomes).deep.equal(['sent', 'ChannelOverflowError'])
      expect(r.seen).deep.equal(['1'])
    },
  )
}

/** Tests that wait out Room's fixed timeouts, run in one lane. */
function testRoomRedisSubscriberDropped() {
  testRoomScenario<{ published: number; received: number; reported: number }>(
    'subscriber-dropped',
    "room: Redis drops a server's subscriber connection during publishes: what didn't arrive is reported, count for count",
    (r) => {
      expect(r.published).toBe(600)
      expect(r.reported > 0).toBe(true)
      expect(r.received + r.reported).toBe(600)
      expectLog('Redis subscriber connection closed')
    },
  )
  testRoomScenario<{ reported: string[]; arrivedAfter: number }>(
    'dropped-at-rest',
    "room and broadcast: Redis drops a server's subscriber connection with nothing published meanwhile: no lane reports a loss",
    (r) => {
      expect(r.reported).deep.equal([])
      // The room's message, three binary frames, and the key's message of each kind.
      expect(r.arrivedAfter).toBe(6)
      expectLog('Redis subscriber connection closed')
    },
  )
  testRoomScenario<{ reported: string[]; arrivedAfter: number }>(
    'rewound',
    "room and broadcast: a failover rewinds Redis across a server's dropped subscriber connection: each lane that delivered past it reports a loss it can't count",
    (r) => {
      const commits = "An unknown number of commits never reached this server's Redis subscription"
      const messages = "An unknown number of text messages never reached this server's Redis subscription"
      expect(r.reported.sort()).deep.equal([commits, commits, messages, messages])
      expect(r.arrivedAfter).toBe(3)
      expectLog('Redis subscriber connection closed')
    },
  )
}

function testRoomStaleRead() {
  test("room: a server's room that nothing follows warns, naming it, when read a heartbeat after its Room.get(); a room read right after its own Room.get() doesn't", async () => {
    await navigate(`${getServerUrl()}/room`)
    await page.click('#test-room-stale-read')
    let keptId = ''
    await autoRetry(
      async () => {
        const r = await getResult<{ keptId: string; kept: number; fresh: number }>('#room-result')
        keptId = r.keptId
        expect(r).deep.equal({ keptId, kept: 0, fresh: 0 })
      },
      { timeout: 60_000 },
    )
    // Any other warning, such as one for the room read after its own Room.get(), is a log the test doesn't expect.
    expectLog(`Room '${keptId}' was read`)
  })
}

function testRoomRedisPublishLimit() {
  testRoomScenario<{ refused: string }>(
    'over-redis-limit',
    "room: a publish over the Redis backend's ceiling rejects",
    (r) => {
      expect(r).deep.equal({ refused: 'ChannelOverflowError' })
    },
  )
}

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

  test('room: a room of 100,000 members reaches a page with its roster, with a join and a leave landing meanwhile, and the server answers the page promptly through a heartbeat', async () => {
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
