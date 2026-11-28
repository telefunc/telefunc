export { testRun }
export { testCloudflareBindings }

import { autoRetry, expect, expectLog, getServerUrl, page, test } from '@brillout/test-e2e'
import type { Page, WebSocket } from 'playwright-chromium'
import { LEASE_CALL_DEADLINE_MS, LEASE_RENEW_EVERY_MS } from 'telefunc/__internal'
import {
  exposeSendUnreadable,
  sendRefusedWarning,
  sessionEndedByUnreadable,
  testCounter,
  testRunClassic,
  upgradeStatus,
  withNetworkConditions,
} from '../../test/utils'
import { LAPSED_PRESENCE_REFUSED_FOR_MS } from './pages/lapsed-presence/presenceRecords'
import { fieldOf, listOf, numberAt, present, recordOf, textAt, textListOf, textOf } from '../checked'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  const isDev = cmd === 'pnpm run dev'
  // First, so no session Durable Object exists in the isolate yet.
  testBroadcastPublishFromWorker()
  testSubscribeOutsideSession()
  // Before any page loads a .telefunc file, which would register Room.
  testRoomRoleFromWorker()
  testCloudflareBindings()
  testRunClassic(cmd, {
    // testLapsedPresence() waits as long as its presence may take to be recorded again, longer than testResubscribe()
    // waits out a presence's TTL.
    additionalTimeout: LAPSED_PRESENCE_RECORDED_AGAIN_WITHIN_MS,
    tolerateError: (log) =>
      log.logText.includes('Detected multiple renderers concurrently rendering') ||
      // The Channel/Chat streaming (SSE) connection surfaces a benign
      // `net::ERR_ALPN_NEGOTIATION_FAILED` browser error; the channel still works
      // (the message flow is asserted by testChannel()/testChat()).
      log.logText.includes('ERR_ALPN_NEGOTIATION_FAILED') ||
      // workerd logs a response it wrote to a connection the browser closed as it navigated away,
      // before the request's signal could report it: "Broken pipe" and "Connection reset by peer" are both that write.
      (log.logText.includes('disconnected: ::write(') &&
        (log.logText.includes('Broken pipe') || log.logText.includes('Connection reset by peer'))) ||
      // testRefusedPresence()'s authority refuses a presence, and the subscription it ended is reported.
      log.logText.includes('Backend subscription closed: text:refused-presence') ||
      // testLapsedPresence()'s authority refuses its refreshes, and the route reports it lost.
      log.logText.includes("Cloudflare Broadcast presence for 'lapsed-presence:") ||
      // testRoomStorage()'s server listener throws, as a test of what Room does then.
      log.logText.includes('Test: a listener throws') ||
      // testRefusedDelivery()'s session refuses a delivery, which its coordinator reports.
      log.logText.includes('Test: the session refused the delivery'),
  })
  testTodolist()
  testChannel()
  testChannelOversized()
  testSessionEnd()
  testBacklog()
  testChat()
  testChatMembers()
  testUnsubscribeTwice()
  testResubscribe()
  testRefusedPresence()
  testRefusedDelivery()
  if (!isDev) testLapsedPresence()
  testColdPublish()
  testPublishOrder()
  testRoom()
  // Under `vite dev`, @cloudflare/vite-plugin awaits each Durable Object call apart, so one stub's calls can run out of order.
  if (!isDev) testRoomOrderAcrossSessions()
  if (!isDev) testRoomJoinRoundTrip()
  testRoomJoinInbox()
  testRoomAcrossSessions()
  testRoomAcrossRegions()
  testRoomCreatedInRegion()
  testRoomStorage()
  testRoomHostingAtCommit()
  testServerFirst()
}

function testBroadcastPublishFromWorker() {
  test('Broadcast.publish() from the Worker, outside any session', async () => {
    const response = await fetch(getServerUrl() + '/__broadcast-publish-from-worker')
    expect(response.status).toBe(204)
  })
}

function testSubscribeOutsideSession() {
  test('Broadcast.subscribe() from the Worker, outside any session, throws a usage error naming what it needs', async () => {
    const response = await fetch(getServerUrl() + '/__broadcast-subscribe-from-worker')
    expect(await response.text()).toContain(
      'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.',
    )
  })
}

function testRoomRoleFromWorker() {
  test('a Room Durable Object call from the Worker, before any .telefunc file loads', async () => {
    const response = await fetch(getServerUrl() + '/__room-call-from-worker')
    expect(response.status).toBe(204)
  })
}

function testCloudflareBindings() {
  test('Cloudflare Bindings', async () => {
    await page.goto(getServerUrl() + '/')
    await testCounter()
    const bodyText = await page.textContent('body')
    expect(bodyText).toContain('someEnvVar')
    expect(bodyText).toContain('some-value')
    expect(bodyText).toContain(`pageContext.globalContext.someEnvVar === ${JSON.stringify('some-value')}`)
  })
}

function testTodolist() {
  test('To-Do list', async () => {
    await page.goto(getServerUrl() + '/todo')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    // Reset only changes the DOM if the list differs from its initial 2 items, so its response is observable
    const todoItemOld = `Buy eggs ${Math.random()}`
    const numberOfItemsBefore = await getNumberOfItems()
    await page.fill('input[type="text"]', todoItemOld)
    await page.click('button[type="submit"]')
    await autoRetry(
      async () => {
        expect(await getNumberOfItems()).toBe(numberOfItemsBefore + 1)
      },
      { timeout: 5000 },
    )
    await page.locator('button', { hasText: 'Reset' }).click()
    await autoRetry(
      async () => {
        expect(await getNumberOfItems()).toBe(3)
      },
      { timeout: 5000 },
    )
    {
      const reactText = await page.textContent('#root')
      expect(reactText).toContain('Buy milk')
      expect(reactText).not.toContain('Buy bananas')
    }
    const todoItemNew = `Buy bananas ${Math.random()}`
    await page.fill('input[type="text"]', todoItemNew)
    await page.click('button[type="submit"]')
    await autoRetry(
      async () => {
        expect(await getNumberOfItems()).toBe(4)
      },
      { timeout: 5000 },
    )
    expect(await page.textContent('#root')).toContain(todoItemNew)
    await fullPageReload()
    expect(await getNumberOfItems()).toBe(4)
    expect(await page.textContent('#root')).toContain(todoItemNew)
  })
}
// Tests the `Channel` stream use case (pages/channel): a bidirectional channel
// that streams server-pushed ticks, replies to a ping with a welcome message,
// and echoes messages back to the client.
function testChannel() {
  test('Channel: the opening telefunction reads its request context and a later SSE acknowledgement has its own scope', async () => {
    const browser = present(page.context().browser(), 'the browser')
    const context = await browser.newContext({ extraHTTPHeaders: { 'x-test-user': 'Alice' } })
    try {
      const target = await context.newPage()
      await target.routeWebSocket(/\/_telefunc/, (socket) => socket.close())
      await target.goto(getServerUrl() + '/channel')
      await target.locator('#hydrated').waitFor()
      await target.click('#connect-btn')
      await target.click('#channel-context-btn')
      await autoRetry(async () => {
        const log = await target.textContent('#log')
        expect(log).toContain('Opened by Alice')
        expect(log).toContain('Channel request: unscoped')
      })
    } finally {
      await context.close()
    }
  })

  test('Channel (stream)', async () => {
    await page.goto(getServerUrl() + '/channel')
    await autoRetry(
      async () => {
        expect(await page.locator('#hydrated').count()).toBe(1)
      },
      { timeout: 5000 },
    )
    const webSockets: string[] = []
    const onWebSocket = (ws: WebSocket) => webSockets.push(ws.url())
    page.on('websocket', onWebSocket)
    await page.click('#connect-btn')
    // The client sends a `ping` on connect and the server replies with a `welcome`.
    await autoRetry(
      async () => {
        const log = await page.textContent('#log')
        expect(log).toContain('Connected!')
        expect(log).toContain('{"type":"welcome"}')
      },
      { timeout: 10000 },
    )
    // The channel moves to WebSocket, though +server.ts assigns config.channel after the adapter. The page opens it
    // over SSE and upgrades once its RECONCILED arrives, so the welcome may come before the WebSocket opens.
    await autoRetry(
      async () => {
        expect(webSockets.some((url) => new URL(url).pathname === '/_telefunc')).toBe(true)
      },
      { timeout: 10000 },
    )
    await page.click('#channel-context-btn')
    await autoRetry(async () => expect(await page.textContent('#log')).toContain('Channel request: unscoped'))
    page.off('websocket', onWebSocket)
    // The page's own origin opened it; a page of another site may not. The Worker answers it 403, and the dev and
    // preview server of @cloudflare/vite-plugin, which hands upgrades to the Worker, ends the connection without
    // passing on an answer that isn't a WebSocket (its dist/index.mjs, `if (!workerWebSocket) socket.destroy()`).
    expect(await upgradeStatus('https://another-site.example')).toBe('no answer')
    // The server's warning comes through its own output, apart from the connection.
    await autoRetry(() => expectLog('A WebSocket upgrade from https://another-site.example was refused'), {
      timeout: 5000,
    })
    // The server pushes a `tick` message every second.
    await autoRetry(
      async () => {
        expect(await page.textContent('#log')).toContain('{"type":"tick","count":1}')
      },
      { timeout: 10000 },
    )
    // The server echoes messages back to the client.
    const echoText = `echo-${Math.random()}`
    await page.fill('#echo-input', echoText)
    await page.click('#send-echo-btn')
    await autoRetry(
      async () => {
        const log = (await page.textContent('#log')) ?? ''
        // Once for the outgoing message, once for the server's echo back.
        const occurrences = log.split(echoText).length - 1
        expect(occurrences >= 2).toBe(true)
      },
      { timeout: 10000 },
    )
    await page.click('#disconnect-btn')
    await autoRetry(
      async () => {
        expect(await page.textContent('#log')).toContain('Disconnected')
      },
      { timeout: 5000 },
    )
  })
}

function testSessionEnd() {
  test("Channel: a session the server ends tells the channel its cause by the WebSocket's close, its code and reason as the browser hands them to the page, and the page opens no other WebSocket", async () => {
    const browser = page.context().browser()
    if (browser === null) throw new Error('no browser')
    // A page of its own: its WebSocket stays wrapped for as long as it lives.
    const context = await browser.newContext()
    try {
      const own = await context.newPage()
      await own.addInitScript(exposeSendUnreadable)
      const sockets: string[] = []
      // The channel's data comes on the WebSocket once the page moved to it: the move is over.
      let dataOnWebSocket = false
      own.on('websocket', (socket) => {
        sockets.push(socket.url())
        socket.on('framereceived', ({ payload }) => {
          if (typeof payload !== 'string' && payload.includes('"tick"')) dataOnWebSocket = true
        })
      })
      await own.goto(getServerUrl() + '/channel')
      await autoRetry(async () => {
        expect(await own.locator('#hydrated').count()).toBe(1)
      })
      await own.click('#connect-btn')
      await autoRetry(
        async () => {
          expect(dataOnWebSocket).toBe(true)
        },
        { timeout: 10000 },
      )
      const opened = sockets.length
      expect(await own.evaluate(() => window.sendUnreadable?.() ?? false)).toBe(true)
      await autoRetry(async () => {
        expect(await own.textContent('#log')).toContain(`Closed: ${sessionEndedByUnreadable}`)
      })
      expect(await own.evaluate(() => window.socketCloses?.at(-1))).deep.equal({
        code: 4002,
        reason: 'text/vnd.telefunc.refusal; kind=frames; cause=8',
      })
      // A channel told its end needs no other wire.
      await new Promise((resolve) => setTimeout(resolve, 1_000))
      expect(sockets.length).toBe(opened)
    } finally {
      await context.close()
    }
  })
}

function testChannelOversized() {
  test('Channel: a 32 MiB message is refused, and the channel keeps working', async () => {
    await page.goto(getServerUrl() + '/channel')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#connect-btn')
    await autoRetry(async () => {
      expect(await page.textContent('#log')).toContain('{"type":"welcome"}')
    })
    // `sendBinary()` throws at the call: the page logs the refusal, and not the send.
    await page.click('#send-oversized-btn')
    await autoRetry(async () => {
      expect(await page.textContent('#log')).toContain('Refused: ')
    })
    expect(await page.textContent('#log')).not.toContain('binary: 32 MiB')
    const echoText = `echo-${Math.random()}`
    await page.fill('#echo-input', echoText)
    await page.click('#send-echo-btn')
    await autoRetry(async () => {
      expect(await page.textContent('#log')).toContain(`{"type":"echo","text":"${echoText}"}`)
    })
  })
}

// Tests the `BroadcastChannel` pub/sub use case (pages/chat): joining subscribes to
// the channel, whose server-side `onOpen` publishes a "joined" system message that is
// broadcast back to the subscribed client. This exercises the full BroadcastChannel
// round-trip (subscribe → onOpen → publish → client delivery → render).
function testChat() {
  test('Chat (broadcast channel)', async () => {
    await page.goto(getServerUrl() + '/chat')
    await autoRetry(
      async () => {
        expect(await page.locator('#hydrated').count()).toBe(1)
      },
      { timeout: 5000 },
    )
    const username = `user-${Math.floor(Math.random() * 1e6)}`
    await page.fill('input[placeholder="Pick a username..."]', username)
    await page.locator('button', { hasText: 'Join' }).click()
    // Joining opens the channel, whose `onOpen` publishes a system "joined" message
    // that is broadcast back to the subscribed client.
    await autoRetry(
      async () => {
        expect(await page.textContent('#root')).toContain(`${username} joined`)
      },
      { timeout: 10000 },
    )
    // `onOpen` also publishes two binary greetings from one reused buffer: each arrives as published.
    await autoRetry(async () => {
      const text = present(await page.textContent('#root'), 'the text of #root')
      expect(text).toContain(`welcome ${username}`)
      expect(text).toContain(`${username} is online`)
      expect(text.indexOf(`welcome ${username}`)).lessThan(text.indexOf(`${username} is online`))
    })
  })
}

function testBacklog() {
  test('Backlog: 24 MiB the server sends nobody awaiting to a page that stops reading all go out, and arrive whole and in order once it reads, the channel open', async () => {
    expect(await sendToStalledPage(1500)).toBe(0)
  })

  test("Backlog: of 96 MiB the same way, those past Cloudflare's default maxMemory of 64 MiB are refused, and the rest arrive in order, the channel open", async () => {
    expect(await sendToStalledPage(6000)).greaterThan(0)
    expectLog(sendRefusedWarning)
  })

  /** Returns how many were refused. */
  async function sendToStalledPage(count: 1500 | 6000) {
    await page.goto(getServerUrl() + '/backlog')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#backlog-open')
    const channelId = await autoRetry(async () => {
      const id = fieldOf(await shownJson(page, '#backlog-state'), 'channelId')
      expect(id).not.toBe(null)
      return textOf(id)
    })
    const runUrl = `${getServerUrl()}/backlog-run?channel=${channelId}`
    await page.click(`#backlog-run-${count}`)
    await autoRetry(async () => {
      expect(await (await fetch(runUrl)).json()).deep.equal({ started: true })
    })
    // A page that reads nothing: what the server sends waits on the way. The page can't see the run while stalled.
    const { sent, refused } = await withNetworkConditions({ downloadThroughput: 1, uploadThroughput: -1 }, async () => {
      await fetch(runUrl, { method: 'POST' })
      // The run reports nothing until its sends are done.
      return await autoRetry(async () => {
        const run = await jsonOf(await fetch(runUrl))
        return { sent: numberAt(run, 'sent'), refused: numberAt(run, 'refused') }
      })
    })
    expect(sent).toBe(count)
    await autoRetry(
      async () => {
        const state = await shownJson(page, '#backlog-state')
        expect(fieldOf(state, 'received')).toBe(count - refused)
        expect(fieldOf(state, 'outOfOrder')).toBe(0)
        expect(fieldOf(state, 'closeErr')).toBe(null)
      },
      { timeout: 30_000 },
    )
    return refused
  }
}

function testChatMembers() {
  test('Chat: members in separate browsers each get every message published after they joined, once', async () => {
    const browser = present(page.context().browser(), 'the browser')
    const names = ['member-1', 'member-2', 'member-3'].map((name) => `${name}-${Math.floor(Math.random() * 1e6)}`)
    const members: (typeof page)[] = []
    for (const name of names) {
      const member = await (await browser.newContext()).newPage()
      members.push(member)
      await member.goto(getServerUrl() + '/chat')
      await autoRetry(async () => {
        expect(await member.locator('#hydrated').count()).toBe(1)
      })
      await member.fill('input[placeholder="Pick a username..."]', name)
      await member.locator('button', { hasText: 'Join' }).click()
      await autoRetry(async () => {
        expect(await member.locator('#messages').textContent()).toContain(`${name} joined`)
      })
    }
    // Each member's onOpen publishes `<name> joined` and two binary greetings.
    await autoRetry(async () => {
      for (const [i, member] of members.entries()) {
        const messages = await member.locator('#messages > div').allTextContents()
        const later = names.slice(i)
        expect(messages.filter((text) => text.endsWith(' joined'))).deep.equal(later.map((name) => `${name} joined`))
        expect(messages.filter((text) => text.startsWith('welcome ')).sort()).deep.equal(
          later.map((name) => `welcome ${name}`).sort(),
        )
      }
    })
    for (const member of members) await member.context().close()
  })
}

function testUnsubscribeTwice() {
  test("Broadcast: a subscriber whose sibling's unsubscribe ran twice still gets what a page in another session publishes", async () => {
    const url = `${getServerUrl()}/unsubscribe-twice?key=unsubscribe-twice:${Math.random()}`
    await page.goto(url)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#subscribe')
    await autoRetry(async () => {
      expect(fieldOf(await shownJson(page, '#unsubscribe-twice-state'), 'subscribed')).toBe(true)
    })
    // Another browser context is another session, routed by its own token.
    const publisher = await (await present(page.context().browser(), 'the browser').newContext()).newPage()
    await publisher.goto(url)
    await autoRetry(async () => {
      expect(await publisher.locator('#hydrated').count()).toBe(1)
    })
    // Each try publishes again: another session's publish may reach the key's authority before the subscriber's presence.
    await autoRetry(
      async () => {
        await publisher.click('#publish')
        await page.click('#read')
        expect(fieldOf(await shownJson(page, '#unsubscribe-twice-state'), 'received')).include('hello')
      },
      { timeout: 10_000 },
    )
    await publisher.context().close()
  })
}

function testResubscribe() {
  test("Broadcast: a subscriber that arrives as its key's last one leaves keeps getting publishes past the presence TTL", async () => {
    await page.goto(getServerUrl() + '/resubscribe')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#resubscribe')
    await autoRetry(async () => {
      expect(fieldOf(await shownJson(page, '#resubscribe-state'), 'subscribed')).toBe(true)
    })
    // A presence lasts 90 s unless refreshed. The page reads meanwhile, as a page's traffic does: a session Durable
    // Object nothing calls is evicted 70-140 s after its last request, and the subscription it holds with it.
    for (let waited = 0; waited < 95_000; waited += 19_000) {
      await page.waitForTimeout(19_000)
      await page.click('#read')
    }
    await page.click('#publish')
    await autoRetry(
      async () => {
        expect(fieldOf(await shownJson(page, '#resubscribe-state'), 'received')).deep.equal(['hello'])
      },
      { timeout: 5000 },
    )
  })
}

function testRefusedPresence() {
  test("Broadcast: a publish made while the key's authority refuses its subscriber's presence settles, the subscriber is removed, and the one the app subscribes again receives the next publish once", async () => {
    await page.goto(getServerUrl() + '/refused-presence')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#subscribe')
    // KV refused the presence: the subscription's `opened` rejected, and the publish made meanwhile settled.
    await autoRetry(async () => {
      const state = await shownJson(page, '#refused-presence-state')
      // The subscriber's `opened` keeps the refusal of its first presence.
      expect({ published: fieldOf(state, 'published'), opened: fieldOf(state, 'opened') }).deep.equal({
        published: true,
        opened: 'rejected',
      })
    })
    await page.click('#publish')
    await autoRetry(async () => {
      expect(fieldOf(await shownJson(page, '#refused-presence-state'), 'received')).deep.equal(['again'])
    })
  })
}

/** The latest a `lapsed-presence:` subscriber's presence is recorded again, from when its subscription is up. Its
 *  session's refresh timer starts then and ticks every `LEASE_RENEW_EVERY_MS`. The first tick once the authority's
 *  refusals end renews, by a call waited on for `LEASE_RENEW_EVERY_MS`, finds the presence lapsed, and holds it again,
 *  by a call waited on for `LEASE_CALL_DEADLINE_MS`. */
const LAPSED_PRESENCE_RECORDED_AGAIN_WITHIN_MS =
  Math.ceil(LAPSED_PRESENCE_REFUSED_FOR_MS / LEASE_RENEW_EVERY_MS) * LEASE_RENEW_EVERY_MS +
  LEASE_RENEW_EVERY_MS +
  LEASE_CALL_DEADLINE_MS

function testLapsedPresence() {
  test("Broadcast: a session whose presence lapsed misses what is published and is told, then either gets what follows once it's recorded again or, evicted, ended its subscription with its object, and a fresh one gets it", async () => {
    await page.goto(getServerUrl() + '/lapsed-presence')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    const state = async () => recordOf(await shownJson(page, '#lapsed-presence-state'))
    await page.click('#subscribe')
    await autoRetry(async () => {
      expect((await state()).received).deep.equal(['before'])
    })
    // The presence's first hold, which starts the refresh timer once answered, went to the authority ahead of the publish
    // of 'before', through the same ordered stub.
    const recordedAgainBy = Date.now() + LAPSED_PRESENCE_RECORDED_AGAIN_WITHIN_MS
    // A presence lasts 90 s unless refreshed (`LEASE_TTL_MS`), and the authority refuses refreshes until 100 s in.
    await page.waitForTimeout(95_000)
    await page.click('#publish-during')
    await autoRetry(async () => {
      const { published, received, reported } = await state()
      expect(published).toBe('during')
      expect(received).not.toContain('during')
      expect(reported).deep.equal([])
    })
    expect((await state()).reported).deep.equal([])
    // Then one of the two outcomes the design allows, whichever comes first, waited on as state:
    // (a) the session's first refresh once the refusals end finds the presence lapsed, reports it, and records it again;
    // (b) the session's Durable Object was evicted, which ends its subscription with it (the owner's decision of
    //     2026-11-21).
    // The timeout only guards against a hang: it ends no sooner than (a) is due.
    const outcome = await autoRetry(
      async () => {
        await page.click('#read')
        const { rerecorded, evicted } = await state()
        expect(rerecorded === true || evicted === true).toBe(true)
        return evicted === true ? 'evicted' : 'recorded again'
      },
      { timeout: recordedAgainBy - Date.now() },
    )
    if (outcome === 'evicted') {
      await page.click('#subscribe')
      await autoRetry(async () => {
        const { received, evicted } = await state()
        expect(evicted).toBe(false)
        expect(received).deep.equal(['before'])
      })
    }
    await page.click('#publish-after')
    await autoRetry(async () => {
      const { received, reported, evicted } = await state()
      expect(evicted).toBe(false)
      expect(received).toContain('after')
      expect(received).not.toContain('during')
      if (outcome === 'recorded again') expect(listOf(reported).length).toBe(1)
    })
  })
}

function testRefusedDelivery() {
  test('Broadcast: a publish a session Durable Object refuses to take rejects, the other session still gets it, and the next publish reaches both', async () => {
    const key = `refused-delivery:${Math.random()}`
    const browser = present(page.context().browser(), 'the browser')
    const pages: Page[] = []
    // Two regions, so each session takes its deliveries in a Durable Object of its own (LAX is wnam, FRA is weur).
    // One after the other: in `vite dev`, a second first request to a page can get its half-evaluated entry and fail.
    for (const [name, colo] of Object.entries({ p1: 'LAX', p2: 'FRA' })) {
      const context = await browser.newContext()
      await context.addCookies([{ name: 'test-colo', value: colo, url: getServerUrl() }])
      const session = await context.newPage()
      await session.goto(`${getServerUrl()}/refused-delivery?key=${key}&page=${name}`)
      await autoRetry(async () => {
        expect(await session.locator('#hydrated').count()).toBe(1)
      })
      await session.click('#subscribe')
      await autoRetry(async () => {
        expect(fieldOf(await shownJson(session, '#refused-delivery-state'), 'subscribed')).toBe(true)
      })
      pages.push(session)
    }
    const publish = async (message: string) => {
      const query = new URLSearchParams({ key, message })
      const response = await fetch(`${getServerUrl()}/__refused-delivery-publish?${query}`)
      const answer = await jsonOf(response)
      return {
        rejected: fieldOf(answer, 'rejected'),
        cause: fieldOf(answer, 'cause'),
        causeRemote: fieldOf(answer, 'causeRemote'),
      }
    }
    const received = async () =>
      await Promise.all(
        pages.map(async (session) => {
          await session.click('#read')
          return textListOf(fieldOf(await shownJson(session, '#refused-delivery-state'), 'received'))
        }),
      )
    // Each try publishes again until both presences are recorded.
    await autoRetry(
      async () => {
        await publish('ready')
        for (const got of await received()) expect(got).toContain('ready')
      },
      { timeout: 10_000 },
    )
    const refused = await publish('refused')
    expect(refused.rejected).toContain('lost to 1/2 Durable Objects')
    // And why: the refusing session's own error, as its coordinator caught it, marks included.
    expect(refused.cause).toBe('Error: Test: the session refused the delivery')
    expect(refused.causeRemote).toBe(true)
    await autoRetry(async () => {
      expect((await received()).map((got) => got.includes('refused')).sort()).deep.equal([false, true])
    })
    expect(await publish('after')).deep.equal({ rejected: null, cause: null, causeRemote: null })
    await autoRetry(async () => {
      for (const got of await received()) expect(got).toContain('after')
    })
    for (const session of pages) await session.context().close()
  })
}

function testRoom() {
  test('Room inside a telefunction', async () => {
    await page.goto(getServerUrl() + '/room')
    await autoRetry(
      async () => {
        expect(await page.textContent('#room-result')).toBe(JSON.stringify({ joined: 1, received: ['hello'] }))
      },
      { timeout: 10000 },
    )
  })
}

function testRoomOrderAcrossSessions() {
  test('Room and Broadcast: what two sessions publish at once reaches each in the order each called it, and both in the same order', async () => {
    const id = crypto.randomUUID()
    const pages = await openSessions(id, ['p1', 'p2'])
    for (const page of pages) expect(await clickRoomCard(page, 'order-subscribe')).toBe('subscribed')
    await Promise.all(pages.map((page) => clickRoomCard(page, 'order-publish')))
    const calls = Array.from({ length: 30 }, (_, i) => i + 1)
    await autoRetry(async () => {
      const [first, second] = await Promise.all(pages.map((page) => clickRoomCard(page, 'order-received')))
      for (const lane of ['broadcast', 'room'] as const) {
        for (const from of ['p1', 'p2']) {
          const own = textListOf(fieldOf(first, lane)).filter((message) => message.startsWith(`${from}:`))
          expect({ lane, own }).deep.equal({ lane, own: calls.map((n) => `${from}:${n}`) })
        }
        expect({ lane, second: fieldOf(second, lane) }).deep.equal({ lane, second: fieldOf(first, lane) })
      }
    })
    for (const page of pages) await page.context().close()
  })
}

function testRoomJoinRoundTrip() {
  test("Room: a join reaches the room's authority as one write, which stores its inbox's route", async () => {
    const id = crypto.randomUUID()
    const [session] = await openSessions(id, ['p1'])
    await fetch(`${getServerUrl()}/__room-calls`, { method: 'PUT' })
    expect(await clickRoomCard(session, 'join')).toBe('joined')
    await autoRetry(async () => {
      const calls = listOf(await jsonOf(await fetch(`${getServerUrl()}/__room-calls`)))
      const methods = calls
        .filter((call) => textAt(call, 'object') === `__telefunc_room__:join:${id}`)
        .map((call) => textAt(call, 'method'))
      // The join's own write, and any other write that could carry a join: an operation, or the inbox's route alone.
      const writes = methods.filter(
        (method) => method === 'runJoinWithInbox' || method === 'runOperation' || method === 'registerRoute(inbox)',
      )
      expect(writes).deep.equal(['runJoinWithInbox'])
    })
    await fetch(`${getServerUrl()}/__room-calls`, { method: 'DELETE' })
    await session.context().close()
  })
}

function testRoomJoinInbox() {
  test('Room: a join into a room that closes while its inbox registers rejects at once with the closed room', async () => {
    await openRoomPage()
    const result = await clickRoomCard(page, 'join-closing', 90_000)
    expect(fieldOf(result, 'joined')).toBe(false)
    expect(textAt(fieldOf(result, 'failure'), 'message').startsWith('Room is closed: join-closing:')).toBe(true)
    expect(numberAt(result, 'seconds')).lessThan(5)
  })

  test('Room: a join the authority refuses rejects with the refusal, leaves no member, and the other participant sees no join of it', async () => {
    await openRoomPage()
    const result = await clickRoomCard(page, 'join-refused')
    expect({
      joined: fieldOf(result, 'joined'),
      members: fieldOf(result, 'members'),
      seen: fieldOf(result, 'seen'),
    }).deep.equal({
      joined: false,
      members: 1,
      seen: ['next'],
    })
    expect(textAt(fieldOf(result, 'failure'), 'message')).toBe('Test: the authority refused the inbox')
  })
}

function testRoomAcrossSessions() {
  test('Room and Broadcast: two sessions listening in one isolate each get a message once, on their own listeners', async () => {
    const id = crypto.randomUUID()
    const pages = await openSessions(id, ['p1', 'p2'])
    for (const page of pages) await clickRoomCard(page, 'listen')
    await clickRoomCard(pages[0], 'publish')
    await autoRetry(async () => {
      for (const page of pages) {
        expect(textListOf(await clickRoomCard(page, 'received')).sort()).deep.equal(['broadcast:hello', 'room:hello'])
      }
    })
    for (const page of pages) await page.context().close()
  })

  test('Broadcast: a publish right after a subscribe reaches it', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'subscribe-then-publish')).deep.equal(['at-once'])
  })
}

function testRoomAcrossRegions() {
  test('Room: a message to sessions in three regions crosses to each region once, and reaches each session once', async () => {
    const id = crypto.randomUUID()
    // Los Angeles is wnam, Frankfurt and Amsterdam are weur, Tokyo is apac.
    const colos = { p1: 'LAX', p2: 'FRA', p3: 'AMS', p4: 'NRT' }
    const pages = await openSessions(id, Object.keys(colos), colos)
    for (const page of pages) await clickRoomCard(page, 'listen')
    await fetch(`${getServerUrl()}/__region-trace?message=hello`, { method: 'PUT' })
    await clickRoomCard(pages[0], 'publish')
    await autoRetry(async () => {
      for (const page of pages) {
        expect(textListOf(await clickRoomCard(page, 'received')).sort()).deep.equal(['broadcast:hello', 'room:hello'])
      }
    })
    const trace = await fetch(`${getServerUrl()}/__region-trace`)
    const shown = await jsonOf(trace)
    const forwards = listOf(fieldOf(shown, 'forwards')).map((forward) => ({
      members: textListOf(fieldOf(forward, 'members')),
    }))
    const deliveries = listOf(fieldOf(shown, 'deliveries')).map((delivery) => ({
      member: textAt(delivery, 'member'),
      region: textAt(delivery, 'region'),
    }))
    // One forward per region, each naming only that region's sessions, and one delivery to each session.
    const regionOf = new Map(deliveries.map(({ member, region }) => [member, region]))
    const forwardedRegions = forwards.map(({ members }) => [...new Set(members.map((member) => regionOf.get(member)))])
    expect(forwardedRegions.sort()).deep.equal([['apac'], ['weur'], ['wnam']])
    expect(deliveries.length).toBe(new Set(forwards.flatMap(({ members }) => members)).size)
    for (const page of pages) await page.context().close()
  })
}

function testRoomCreatedInRegion() {
  test("Room: creating a room calls only Durable Objects in its creator's region, and Room.list() finds rooms of every region", async () => {
    const id = crypto.randomUUID()
    // Los Angeles is wnam, Frankfurt is weur.
    const pages = await openSessions(id, ['p1', 'p2'], { p1: 'LAX', p2: 'FRA' })
    for (const [page, name, region] of [
      [pages[0], 'p1', 'wnam'],
      [pages[1], 'p2', 'weur'],
    ] as const) {
      await fetch(`${getServerUrl()}/__room-calls`, { method: 'PUT' })
      expect(await clickRoomCard(page, 'create')).toBe('created')
      const calls = listOf(await jsonOf(await fetch(`${getServerUrl()}/__room-calls`, { method: 'DELETE' })))
      // Other tests' rooms keep their sessions' routes and listings meanwhile: only this room's calls count.
      const room = `regions:${id}:${name}`
      const objects = new Set(
        calls
          .filter((call) => fieldOf(call, 'room') === room || textAt(call, 'object').endsWith(room))
          .map((call) => textAt(call, 'object')),
      )
      expect([...objects].sort()).deep.equal([`__telefunc_room__:${room}`, `__telefunc_room_directory__:${region}`])
    }
    expect(await clickRoomCard(pages[0], 'list-created')).deep.equal([`regions:${id}:p1`, `regions:${id}:p2`])
    for (const page of pages) await page.context().close()
  })
}

function testRoomStorage() {
  test('Room.list(): lists rooms in code point order', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'list')).deep.equal(['B', 'a', 'b', 'z', 'é', '！', '😀'])
  })

  test('Room: a retained frame larger than a SQLite row reaches a later subscriber whole', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'retain-large')).deep.equal({ size: 3 * 1024 * 1024, intact: true })
  })

  test('Room: a room closes by itself once its last member left, and is no longer listed', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'close-when-left')).deep.equal({ listed: 0 })
  })

  test('Room: a view that subscribes after another session closed its room closes, and no bug is reported', async () => {
    const id = crypto.randomUUID()
    const [holder, closer] = await openSessions(id, ['holder', 'closer'])
    await clickRoomCard(holder, 'hold-view')
    await clickRoomCard(closer, 'close-held')
    expect(await clickRoomCard(holder, 'subscribe-held')).deep.equal([])
    for (const page of [holder, closer]) await page.context().close()
  })

  test('Room: a server listener that throws is reported, and the listener beside it and the messages after it still arrive', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'throwing-listener')).deep.equal({ thrower: 2, healthy: 2 })
  })

  test("Room: an acknowledged DM whose answer is over the store's row limit tells its sender ChannelOverflowError", async () => {
    await openRoomPage()
    expect(await overRowLimit('reply-outcome')).deep.equal({ told: 'ChannelOverflowError' })
  })

  test("Room: a removal whose leave is over the store's row limit is refused with ChannelOverflowError, and the visible tab stays", async () => {
    await openRoomPage()
    expect(await overRowLimit('removal-outcome')).deep.equal({
      told: 'ChannelOverflowError',
      visible: 1,
    })
  })

  test("Room: a Room.send() of a message over the store's row limit goes through, as its commit keeps no row of it", async () => {
    await openRoomPage()
    expect(await outcomeAt('send-outcome', ROW_LIMIT_BYTES)).deep.equal({ told: 'resolved' })
  })

  test('Room: a read of cells reads the cell each reference names beside its cell, by key and on every page of a prefix read', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'beside-read')).deep.equal({
      written: 'committed',
      byKey: { 'tm:a': 'lease one', 'tm:b': null },
      pages: true,
      without: 0,
      fillers: 1024,
      others: { 'tm:a': 'lease one', 'tm:b': null },
      plain: 'none',
    })
  })

  test("Room: a join over the store's row limit is refused with ChannelOverflowError, and the next join goes through", async () => {
    await openRoomPage()
    expect(await overRowLimit('join-outcome')).deep.equal({
      told: 'ChannelOverflowError',
      participants: 0,
      nextJoin: 'resolved',
    })
  })

  test("Room: a meta update over the store's row limit is refused with ChannelOverflowError, and the next update goes through", async () => {
    await openRoomPage()
    expect(await overRowLimit('meta-update-outcome')).deep.equal({
      told: 'ChannelOverflowError',
      keys: ['seed'],
      nextUpdate: 'resolved',
    })
  })

  test("Room: a handle that hears a member's track through her every track and every member's track gets each of her frames once on each listener", async () => {
    await openRoomPage()
    const each = Array.from({ length: 40 }, (_, n) => `m${n + 1}`)
    expect(await clickRoomCard(page, 'two-routes')).deep.equal({ everyTrack: each, everyMember: each })
  })

  test('Room: its meta keeps the key order its writes gave it', async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'meta-key-order')).deep.equal({
      merged: [
        ['b', 1],
        ['a', 4],
        ['c', 3],
      ],
      replaced: [
        ['z', 1],
        ['y', 2],
      ],
    })
  })
}

/** A Durable Object's SQLite storage refuses a row over 8 MiB and 34 bytes. */
const ROW_LIMIT_BYTES = 8 * 1024 * 1024 + 34

/** What the size card `card` tells of its call with a value of `length` characters. */
async function outcomeAt(card: string, length: number) {
  await page.fill('#room-length', String(length))
  return await clickRoomCard(page, card)
}

/** What `card`'s call tells one character over the longest value it takes, found by bisection between 4 KiB under the
 *  row limit and the limit, one call per try. */
async function overRowLimit(card: string) {
  let low = ROW_LIMIT_BYTES - 4096
  let high = ROW_LIMIT_BYTES
  expect(fieldOf(await outcomeAt(card, low), 'told')).toBe('resolved')
  while (high - low > 1) {
    const middle = Math.floor((low + high) / 2)
    if (fieldOf(await outcomeAt(card, middle), 'told') === 'resolved') low = middle
    else high = middle
  }
  return await outcomeAt(card, low + 1)
}

function testRoomHostingAtCommit() {
  test("Room: a Room.send() whose tab's lease lapsed between its lookup and its inbox commit is refused at the commit", async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'send-after-lapse')).deep.equal({ receipts: 0 })
  })

  test("Room: a participant's send() to a tab its view lists, whose lease a reap claimed, is told the participant is gone", async () => {
    await openRoomPage()
    expect(await clickRoomCard(page, 'send-to-claimed')).deep.equal({ sent: 'participant not found' })
  })
}

async function openRoomPage() {
  await page.goto(getServerUrl() + '/room')
  await autoRetry(async () => {
    expect(await page.locator('#hydrated').count()).toBe(1)
  })
}

/** Pages in browsers of their own, so each has its own Telefunc session; `colos` places a page's session in a colo's
 *  region. */
async function openSessions(id: string, names: string[], colos: Record<string, string> = {}) {
  const browser = present(page.context().browser(), 'the browser')
  return await Promise.all(
    names.map(async (name) => {
      const context = await browser.newContext()
      const colo = colos[name]
      if (colo) await context.addCookies([{ name: 'test-colo', value: colo, url: getServerUrl() }])
      const session = await context.newPage()
      await session.goto(`${getServerUrl()}/room?id=${id}&page=${name}`)
      await autoRetry(async () => {
        expect(await session.locator('#hydrated').count()).toBe(1)
      })
      return session
    }),
  )
}

/** What the room page's card `id` shows once clicked. */
async function clickRoomCard(target: Page, id: string, timeout = 30_000): Promise<unknown> {
  await target.locator('#room-card-result').evaluate((element) => {
    element.textContent = ''
  })
  await target.click(`#room-${id}`)
  await target.waitForFunction(
    () => {
      const result = document.querySelector('#room-card-result')
      if (!result) throw new Error('The page shows no #room-card-result')
      return result.textContent !== ''
    },
    undefined,
    { timeout },
  )
  return await shownJson(target, '#room-card-result')
}

/** The JSON `target` shows in the element `selector` finds. */
async function shownJson(target: Page, selector: string): Promise<unknown> {
  return JSON.parse(present(await target.textContent(selector), `the text of ${selector}`))
}

/** The JSON a response of the app's test routes carries. */
async function jsonOf(response: Response): Promise<unknown> {
  return await response.json()
}

async function getNumberOfItems() {
  return await page.evaluate(() => document.querySelectorAll('li').length)
}

async function fullPageReload() {
  await page.goto(getServerUrl() + '/')
  await testCounter()
  await page.goto(getServerUrl() + '/todo')
}

function testColdPublish() {
  test("Broadcast: a publish a listener makes in answer to a key's first publish, as its first presence is written, arrives", async () => {
    await page.goto(getServerUrl() + '/cold-publish')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#cold-publish')
    await autoRetry(
      async () => {
        const result = JSON.parse((await page.textContent('#cold-publish-result')) ?? 'null')
        expect(result).deep.equal({ first: 'resolved', received: ['a', 'b'] })
      },
      { timeout: 10_000 },
    )
  })
}

function testPublishOrder() {
  test('Broadcast: an unawaited loop of publishes to one key gets its seq, and arrives, in call order', async () => {
    await page.goto(getServerUrl() + '/publish-order')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#publish-order')
    await autoRetry(
      async () => {
        const result = JSON.parse((await page.textContent('#publish-order-result')) ?? 'null')
        expect(result).deep.equal({
          received: 200,
          seqInCallOrder: true,
          arrivedInSeqOrder: true,
          arrivedInCallOrder: true,
        })
      },
      { timeout: 30_000 },
    )
  })
}

function testServerFirst() {
  test("Broadcast: a channel the server subscribed to after an `await` delivers every publish to its page's object", async () => {
    // Pages on two objects, so that a subscription recorded under any one object but the page's misses on one of them.
    for (const shard of [1, 2]) {
      await page.goto(`${getServerUrl()}/server-first?session=${sessionTokenOfShard(shard)}`)
      await autoRetry(async () => {
        expect(await page.locator('#hydrated').count()).toBe(1)
      })
      await page.click('#server-first')
      await autoRetry(
        async () => {
          expect(JSON.parse((await page.textContent('#server-first-result')) ?? 'null')).deep.equal(['p1', 'p2', 'p3'])
        },
        { timeout: 15_000 },
      )
    }
  })
}

/** A session token the server routes to shard `shard` of the five `new Telefunc({ scale: 5 })` has. */
function sessionTokenOfShard(shard: number): string {
  for (;;) {
    const token = crypto.randomUUID()
    let hash = 0
    for (let i = 0; i < token.length; i++) hash = (hash * 31 + token.charCodeAt(i)) >>> 0
    if (hash % 5 === shard) return token
  }
}
