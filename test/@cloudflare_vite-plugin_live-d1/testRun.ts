export { testRun }

import { autoRetry, expect, getServerUrl, page, run, test } from '@brillout/test-e2e'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  run(cmd)

  test('a live query on D1 refetches on a write through another binding, which only Broadcast carries', async () => {
    await page.goto(getServerUrl() + '/')
    await page.locator('#todos[data-fetches="1"]').waitFor({ state: 'attached' })
    const text = `todo ${Date.now()}`
    await page.fill('#text', text)
    await page.click('#add')
    await autoRetry(async () => {
      expect(await page.textContent('#todos')).toContain(text)
    })
    expect(await page.getAttribute('#todos', 'data-fetches')).toBe('2')
  })
}
