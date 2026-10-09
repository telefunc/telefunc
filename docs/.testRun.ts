export { testRun }

import { test, expect, run, fetchHtml, partRegex, page, getServerUrl, autoRetry } from '@brillout/test-e2e'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  run(cmd)

  test('HTML', async () => {
    const html = await fetchHtml('/')
    expect(html).toContain('<meta name="description" content="Remote Functions." />')
    expect(html).toMatch(partRegex`<h2>${/<img .+>/} Type-Safe by definition</h2>`)
    expect(html).toMatch(partRegex`<h2>${/[^\/]+/} Schemaless by design</h2>`)
    expect(html).toContain('Couple code, not environments.')
  })
  test('DOM', async () => {
    await page.goto(getServerUrl() + '/')
    await autoRetry(async () => {
      const body = await page.textContent('body')
      expect(body).toContain('Iterate flexibly and rapidly.')
      expect(body).toContain('The types are the contract.')
    })
  })
}
