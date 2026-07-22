export { testRun }
export { testCloudflareBindings }

import { autoRetry, expect, getServerUrl, page, test } from '@brillout/test-e2e'
import { testCounter, testRunClassic } from '../../test/utils'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  testCloudflareBindings()
  testRoomAsyncContextRecipe()
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
  testChat()
}

function testRoomAsyncContextRecipe() {
  test('Cloudflare Room async-context recipe', async () => {
    const response = await fetch(getServerUrl() + '/__telefunc-room-async-context-probe')
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
