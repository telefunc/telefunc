export { testRun }
export { testCloudflareBindings }

import { autoRetry, expect, getServerUrl, page, test } from '@brillout/test-e2e'
import type { WebSocket } from 'playwright-chromium'
import { testCounter, testRunClassic, withNetworkConditions } from '../../test/utils'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  const isDev = cmd === 'pnpm run dev'
  // First, so no session Durable Object exists in the isolate yet.
  testBroadcastPublishFromWorker()
  testSubscribeOutsideSession()
  // Before any page loads a .telefunc file, which would register Room.
  testRoomRoleFromWorker()
  testCloudflareBindings()
  testRunClassic(cmd, {
    // testResubscribe() waits out a presence's TTL.
    additionalTimeout: 60_000,
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
      // testRefusedDelivery()'s session refuses a delivery, which its coordinator reports.
      log.logText.includes('Test: the session refused the delivery'),
  })
  testTodolist()
  testChannel()
  testChannelOversized()
  testBacklog()
  testChat()
  testChatMembers()
  testUnsubscribeTwice()
  testResubscribe()
  testRefusedPresence()
  testRefusedDelivery()
  testColdPublish()
  testPublishOrder()
  testRoom()
  // Under `vite dev`, @cloudflare/vite-plugin awaits each Durable Object call apart, so one stub's calls can run out of order.
  if (!isDev) testRoomOrderAcrossSessions()
  testRoomAcrossSessions()
  testRoomAcrossRegions()
  testRoomCreatedInRegion()
  testRoomStorage()
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
    page.off('websocket', onWebSocket)
    // The channel goes over WebSocket, though +server.ts assigns config.channel after the adapter.
    expect(webSockets.some((url) => new URL(url).pathname === '/_telefunc')).toBe(true)
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
    await page.click('#send-oversized-btn')
    await autoRetry(async () => {
      expect(await page.textContent('#log')).toContain('Channel message too large')
    })
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
      const text = (await page.textContent('#root'))!
      expect(text).toContain(`welcome ${username}`)
      expect(text).toContain(`${username} is online`)
      expect(text.indexOf(`welcome ${username}`)).lessThan(text.indexOf(`${username} is online`))
    })
  })
}

function testBacklog() {
  test('Backlog: 96 MiB the server sends nobody awaiting to a page that stops reading all go out, and arrive whole and in order once it reads, the channel open', async () => {
    await page.goto(getServerUrl() + '/backlog')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#backlog-open')
    let channelId: string | null = null
    await autoRetry(async () => {
      channelId = JSON.parse((await page.textContent('#backlog-state'))!).channelId
      expect(channelId).not.toBe(null)
    })
    const runUrl = `${getServerUrl()}/backlog-run?channel=${channelId}`
    await page.click('#backlog-run')
    await autoRetry(async () => {
      expect(await (await fetch(runUrl)).json()).deep.equal({ started: true })
    })
    // A page that reads nothing: what the server sends waits on the way. The page can't see the run while stalled.
    const { sent, refused } = await withNetworkConditions({ downloadThroughput: 1, uploadThroughput: -1 }, async () => {
      await fetch(runUrl, { method: 'POST' })
      let run: { sent: number; refused: string | null } | undefined
      await autoRetry(async () => {
        run = await (await fetch(runUrl)).json()
        expect(run!.sent).not.toBe(undefined)
      })
      return run!
    })
    expect({ sent, refused }).deep.equal({ sent: 6000, refused: null })
    await autoRetry(
      async () => {
        const state = JSON.parse((await page.textContent('#backlog-state'))!)
        expect(state.received).toBe(6000)
        expect(state.outOfOrder).toBe(0)
        expect(state.closeErr).toBe(null)
      },
      { timeout: 30_000 },
    )
  })
}

function testChatMembers() {
  test('Chat: members in separate browsers each get every message published after they joined, once', async () => {
    const browser = page.context().browser()!
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
      expect(JSON.parse((await page.textContent('#unsubscribe-twice-state'))!).subscribed).toBe(true)
    })
    // Another browser context is another session, routed by its own token.
    const publisher = await (await page.context().browser()!.newContext()).newPage()
    await publisher.goto(url)
    await autoRetry(async () => {
      expect(await publisher.locator('#hydrated').count()).toBe(1)
    })
    // Each try publishes again: the subscriber's presence may still be on its way to KV.
    await autoRetry(
      async () => {
        await publisher.click('#publish')
        await page.click('#read')
        expect(JSON.parse((await page.textContent('#unsubscribe-twice-state'))!).received).include('hello')
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
      expect(JSON.parse((await page.textContent('#resubscribe-state'))!).subscribed).toBe(true)
    })
    // A presence lasts 90 s unless refreshed.
    await page.waitForTimeout(95_000)
    await page.click('#publish')
    await autoRetry(
      async () => {
        expect(JSON.parse((await page.textContent('#resubscribe-state'))!).received).deep.equal(['hello'])
      },
      { timeout: 5000 },
    )
  })
}

function testRefusedPresence() {
  test("Broadcast: a publish made while the key's authority refuses its subscriber's presence settles, and the subscription that replaces it receives the next publish", async () => {
    await page.goto(getServerUrl() + '/refused-presence')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#subscribe')
    await autoRetry(async () => {
      expect(JSON.parse((await page.textContent('#refused-presence-state'))!).published).toBe(true)
    })
    // Each try publishes again, until the replacing subscription's presence is recorded.
    await autoRetry(async () => {
      await page.click('#publish')
      expect(JSON.parse((await page.textContent('#refused-presence-state'))!).received).toContain('again')
    })
  })
}

function testRefusedDelivery() {
  test('Broadcast: a publish a session Durable Object refuses to take rejects, the other session still gets it, and the next publish reaches both', async () => {
    const key = `refused-delivery:${Math.random()}`
    const browser = page.context().browser()!
    // Two regions, so each session takes its deliveries in a Durable Object of its own (LAX is wnam, FRA is weur).
    const pages = await Promise.all(
      Object.entries({ p1: 'LAX', p2: 'FRA' }).map(async ([name, colo]) => {
        const context = await browser.newContext()
        await context.addCookies([{ name: 'test-colo', value: colo, url: getServerUrl() }])
        const session = await context.newPage()
        await session.goto(`${getServerUrl()}/refused-delivery?key=${key}&page=${name}`)
        await autoRetry(async () => {
          expect(await session.locator('#hydrated').count()).toBe(1)
        })
        await session.click('#subscribe')
        await autoRetry(async () => {
          expect(JSON.parse((await session.textContent('#refused-delivery-state'))!).subscribed).toBe(true)
        })
        return session
      }),
    )
    const publish = async (message: string) => {
      const query = new URLSearchParams({ key, message })
      const response = await fetch(`${getServerUrl()}/__refused-delivery-publish?${query}`)
      return ((await response.json()) as { rejected: string | null }).rejected
    }
    const received = async () =>
      await Promise.all(
        pages.map(async (session) => {
          await session.click('#read')
          return JSON.parse((await session.textContent('#refused-delivery-state'))!).received as string[]
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
    expect(await publish('refused')).toContain('lost to 1/2 Durable Objects')
    await autoRetry(async () => {
      expect((await received()).map((got) => got.includes('refused')).sort()).deep.equal([false, true])
    })
    expect(await publish('after')).toBe(null)
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
          const own = first[lane].filter((message: string) => message.startsWith(`${from}:`))
          expect({ lane, own }).deep.equal({ lane, own: calls.map((n) => `${from}:${n}`) })
        }
        expect({ lane, second: second[lane] }).deep.equal({ lane, second: first[lane] })
      }
    })
    for (const page of pages) await page.context().close()
  })
}

function testRoomAcrossSessions() {
  test('Room and Broadcast: two sessions listening in one isolate each get a message once, on their own listeners', async () => {
    const id = crypto.randomUUID()
    const pages = await openSessions(id, ['p1', 'p2'])
    for (const page of pages) await clickRoomCard(page, 'listen')
    await clickRoomCard(pages[0]!, 'publish')
    await autoRetry(async () => {
      for (const page of pages) {
        expect((await clickRoomCard(page, 'received')).sort()).deep.equal(['broadcast:hello', 'room:hello'])
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
    await clickRoomCard(pages[0]!, 'publish')
    await autoRetry(async () => {
      for (const page of pages) {
        expect((await clickRoomCard(page, 'received')).sort()).deep.equal(['broadcast:hello', 'room:hello'])
      }
    })
    const trace = await fetch(`${getServerUrl()}/__region-trace`)
    const { forwards, deliveries } = (await trace.json()) as {
      forwards: { members: string[] }[]
      deliveries: { member: string; region: string }[]
    }
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
      [pages[0]!, 'p1', 'wnam'],
      [pages[1]!, 'p2', 'weur'],
    ] as const) {
      await fetch(`${getServerUrl()}/__room-calls`, { method: 'PUT' })
      expect(await clickRoomCard(page, 'create')).toBe('created')
      const calls = (await (await fetch(`${getServerUrl()}/__room-calls`)).json()) as {
        object: string
        room: unknown
      }[]
      // Other tests' rooms keep their sessions' routes and listings meanwhile: only this room's calls count.
      const room = `regions:${id}:${name}`
      const objects = new Set(
        calls.filter((call) => call.room === room || call.object.endsWith(room)).map((call) => call.object),
      )
      expect([...objects].sort()).deep.equal([`__telefunc_room__:${room}`, `__telefunc_room_directory__:${region}`])
    }
    expect(await clickRoomCard(pages[0]!, 'list-created')).deep.equal([`regions:${id}:p1`, `regions:${id}:p2`])
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
    await clickRoomCard(holder!, 'hold-view')
    await clickRoomCard(closer!, 'close-held')
    expect(await clickRoomCard(holder!, 'subscribe-held')).deep.equal([])
    for (const page of [holder!, closer!]) await page.context().close()
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

async function openRoomPage() {
  await page.goto(getServerUrl() + '/room')
  await autoRetry(async () => {
    expect(await page.locator('#hydrated').count()).toBe(1)
  })
}

/** Pages in browsers of their own, so each has its own Telefunc session; `colos` places a page's session in a colo's
 *  region. */
async function openSessions(id: string, names: string[], colos: Record<string, string> = {}) {
  const browser = page.context().browser()!
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

async function clickRoomCard(target: typeof page, id: string): Promise<any> {
  await target.locator('#room-card-result').evaluate((element) => {
    element.textContent = ''
  })
  await target.click(`#room-${id}`)
  await target.waitForFunction(() => document.querySelector('#room-card-result')!.textContent !== '', undefined, {
    timeout: 30_000,
  })
  return JSON.parse((await target.textContent('#room-card-result'))!)
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
  test("Broadcast: publishes past the buffer's budget while the key's first presence is written all arrive", async () => {
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
