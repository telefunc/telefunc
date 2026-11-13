export { testRun }

import { autoRetry, expect, getServerUrl, page, run, test } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  run(cmd)

  test('a live query on D1 refetches on a write from another session Durable Object, which only Broadcast carries', async () => {
    await page.goto(getServerUrl() + '/')
    await page.locator('#todos[data-fetches="1"]').waitFor({ state: 'attached' })
    await page.locator('#notes[data-fetches="1"]').waitFor({ state: 'attached' })
    const writer = await openOnAnotherShard((await page.textContent('#shard'))!)
    try {
      const text = `todo ${Date.now()}`
      await writer.fill('#text', text)
      await writer.click('#add')
      await autoRetry(async () => {
        expect(await page.textContent('#todos')).toContain(text)
      })
      expect(await page.getAttribute('#todos', 'data-fetches')).toBe('2')
      // A write to another table leaves it be.
      expect(await page.getAttribute('#notes', 'data-fetches')).toBe('1')
    } finally {
      await writer.context().close()
    }
  })
}

/** A page of its own session, on a session Durable Object other than `shard`. */
async function openOnAnotherShard(shard: string): Promise<Page> {
  // Each try lands on one of the four Durable Objects at random.
  for (let tries = 0; tries < 20; tries++) {
    const context = await page.context().browser()!.newContext()
    const other = await context.newPage()
    await other.goto(getServerUrl() + '/')
    await other.locator('#shard:not(:empty)').waitFor({ state: 'attached' })
    if ((await other.textContent('#shard')) !== shard) return other
    await context.close()
  }
  throw new Error(`Every page landed on ${shard}`)
}
