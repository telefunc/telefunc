export { testRoom, testRoomLongWaits }
import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Response, WebSocket } from 'playwright-chromium'
import { navigate, getResult, getCleanupState, runServerAction, cutWire, forceServerGc } from '../../e2e-utils'

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
    await autoRetry(async () => validate(await getResult<Result>('#room-result')))
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
}
