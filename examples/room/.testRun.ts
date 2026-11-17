export { testRun }

import { page, test, expect, run, autoRetry } from '@brillout/test-e2e'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  const port = cmd.includes('dev') ? '5173' : '4173'
  const serverUrl = `http://localhost:${port}`
  run(cmd, {
    serverIsReadyMessage: (log) => log.includes('Local:') && log.includes(port),
    serverUrl,
    // On HTTP/1.1, the SSE transport's streaming upload is refused, and it falls back to plain requests.
    tolerateError: (log) => log.logText.includes('ERR_ALPN_NEGOTIATION_FAILED'),
  })

  test('Two tabs join the room, see each other and exchange a message', async () => {
    const otherContext = await page.context().browser()!.newContext()
    const otherTab = await otherContext.newPage()
    await page.goto(serverUrl)
    await otherTab.goto(serverUrl)
    for (const tab of [page, otherTab]) {
      await autoRetry(async () => {
        expect(await tab.locator('#members li').count()).toBe(2)
      })
    }

    await page.fill('input', 'hello')
    await page.click('button')
    for (const tab of [page, otherTab]) {
      await autoRetry(async () => {
        expect(await tab.textContent('#messages')).toContain(': hello')
      })
    }
    await otherContext.close()
  })
}
