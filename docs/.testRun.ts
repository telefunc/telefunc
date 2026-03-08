export { testRun }

import { test, expect, run, fetchHtml, partRegex, page, getServerUrl, autoRetry } from '@brillout/test-e2e'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  run(cmd)

  test('HTML', async () => {
    const html = await fetchHtml('/')
    expect(html).toMatch(partRegex`<h2>${/[^\/]+/}Zero boilerplate</h2>`)
    expect(html).toMatch(partRegex`<h2>${/[^\/]+/}Streaming</h2>`)
    expect(html).toMatch(partRegex`<h2>${/[^\/]+/}Any stack</h2>`)
  })
  test('DOM', async () => {
    await page.goto(getServerUrl() + '/')
    await autoRetry(async () => {
      const body = await page.textContent('body')
      expect(body).toContain('No routers, no procedure builders, no link chains.')
      expect(body).toContain('Bidirectional WebSocket channels')
    })
  })
}
