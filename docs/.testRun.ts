export { testRun }

import { test, expect, run, fetchHtml, partRegex, page, getServerUrl, autoRetry } from '@brillout/test-e2e'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  run(cmd)

  test('HTML', async () => {
    const html = await fetchHtml('/')
    expect(html).toContain('<meta name="description" content="Remote Functions." />')
    expect(html).toMatch(partRegex`<h2>${/[^\/]+/}Simple</h2>`)
    expect(html).toMatch(partRegex`<h2>${/[^\/]+/}Rock-solid</h2>`)
    expect(html).toContain('no known bug')
  })
  test('DOM', async () => {
    await page.goto(getServerUrl() + '/')
    await autoRetry(async () => {
      const body = await page.textContent('body')
      expect(body).toContain('Seamless TypeScript support')
      expect(body).toContain("Telefunc enables programmatically defined permissions. It's both simple and flexible.")
    })
  })
}
