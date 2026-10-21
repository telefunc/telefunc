export { testRun }
export { testCloudflareBindings }

import { autoRetry, expect, getServerUrl, page, test } from '@brillout/test-e2e'
import { testCounter, testRunClassic } from '../../test/utils'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  testCloudflareBindings()
  testRunClassic(cmd, {
    tolerateError: (log) =>
      log.logText.includes('Detected multiple renderers concurrently rendering') ||
      // The Channel/Chat streaming (SSE) connection surfaces a benign
      // `net::ERR_ALPN_NEGOTIATION_FAILED` browser error; the channel still works
      // (the message flow is asserted by testChannel()/testChat()).
      log.logText.includes('ERR_ALPN_NEGOTIATION_FAILED'),
  })
  testTodolist()
  testChannel()
  testChannelOversized()
  testBacklog()
  testChat()
  testChatMembers()
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
  test('Backlog: sends nobody awaits to a page that stops reading are refused with ChannelOverflowError, and what was taken arrives in order once it reads', async () => {
    await page.goto(getServerUrl() + '/backlog')
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })
    await page.click('#backlog-open')
    await autoRetry(async () => {
      expect(JSON.parse((await page.textContent('#backlog-state'))!).open).toBe(true)
    })
    const cdp = await page.context().newCDPSession(page.mainFrame().page())
    await cdp.send('Network.enable')
    // A page that reads nothing: what the server sends waits on the way.
    await cdp.send('Network.emulateNetworkConditions', {
      offline: false,
      latency: 0,
      downloadThroughput: 1,
      uploadThroughput: -1,
    })
    await page.click('#backlog-run')
    // The server's sends are refused or taken within a turn of the request, which the page can't see while stalled.
    await page.waitForTimeout(1000)
    await cdp.send('Network.emulateNetworkConditions', {
      offline: false,
      latency: 0,
      downloadThroughput: -1,
      uploadThroughput: -1,
    })
    await autoRetry(
      async () => {
        const state = JSON.parse((await page.textContent('#backlog-state'))!)
        expect(state.refused.length).greaterThan(0)
        expect(new Set(state.refused)).deep.equal(new Set(['ChannelOverflowError']))
        expect(state.received).toBe(6000 - state.refused.length)
        expect(state.outOfOrder).toBe(0)
      },
      { timeout: 30_000 },
    )
    await cdp.detach()
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

async function getNumberOfItems() {
  return await page.evaluate(() => document.querySelectorAll('li').length)
}

async function fullPageReload() {
  await page.goto(getServerUrl() + '/')
  await testCounter()
  await page.goto(getServerUrl() + '/todo')
}
