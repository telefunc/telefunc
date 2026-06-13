export { testLiveQuery }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { waitForHydration } from '../../e2e-utils'

function testLiveQuery() {
  // ── Local key: invalidation stays on the current client ──────────────

  test('live-query: local key — mutation invalidates only the current client', async () => {
    await page.goto(`${getServerUrl()}/live-query`)
    await waitForHydration()

    // Wait for initial load
    await autoRetry(async () => {
      const text = await page.textContent('#local-todo-list')
      expect(text).toContain('Local todo 1')
    })

    // Add a local todo
    await page.fill('#local-todo-input', 'Local e2e test')
    await page.click('#local-todo-add')

    // Current client sees it
    await autoRetry(async () => {
      const text = await page.textContent('#local-todo-list')
      expect(text).toContain('Local e2e test')
    })
  })

  // ── Global key: invalidation reaches all connected clients ──────────

  test('live-query: global key — mutation invalidates across clients', async () => {
    await page.goto(`${getServerUrl()}/live-query`)
    await waitForHydration()

    // Wait for global todos to load on tab 1
    await autoRetry(async () => {
      const text = await page.textContent('#global-todo-list')
      expect(text).toContain('Global todo 1')
    })

    // Open a second tab in a new context
    const browser = page.context().browser()!
    const context2 = await browser.newContext()
    const page2 = await context2.newPage()
    await page2.goto(`${getServerUrl()}/live-query`)
    await autoRetry(async () => {
      expect(await page2.locator('#hydrated').count()).toBe(1)
    })

    // Tab 2 should also see existing global todos
    await autoRetry(async () => {
      const text = await page2.textContent('#global-todo-list')
      expect(text).toContain('Global todo 1')
    })

    // Add a global todo from tab 1
    await page.fill('#global-todo-input', 'Global e2e test')
    await page.click('#global-todo-add')

    // Tab 1 sees it
    await autoRetry(async () => {
      const text = await page.textContent('#global-todo-list')
      expect(text).toContain('Global e2e test')
    })

    // Tab 2 also sees it (cross-client invalidation via server pub/sub)
    await autoRetry(async () => {
      const text = await page2.textContent('#global-todo-list')
      expect(text).toContain('Global e2e test')
    })

    await page2.close()
    await context2.close()
  })
}
