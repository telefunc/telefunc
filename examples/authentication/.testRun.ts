import { page, test, expect, run, getServerUrl, autoRetry, isLinux, sleep } from '@brillout/test-e2e'

export { testRun }

function testRun(cmd: 'pnpm run dev' | 'pnpm run prod') {
  run(cmd, {
    additionalTimeout: 10 * 1000,
  })

  test('Log-in', async () => {
    await page.goto(`${getServerUrl()}/`)
    await page.waitForSelector('button:not([disabled])')
    await page.click('text=Login as Rom')
    await autoRetry(async () => {
      expect(await page.textContent('body')).toContain('Cherries')
    })
  })

  test('Add to-do item', async () => {
    await page.waitForSelector('fieldset:not([disabled])')
    await page.fill('input[type="text"]', 'Banana')
    await page.click('button[type="submit"]')
    await autoRetry(async () => {
      expect(await page.textContent('body')).toContain('Banana')
    })
  })

  test('New to-do item is persisted & rendered to HTML', async () => {
    // We don't use `fetchHtml()` because it's missing the session's cookie
    const html = await page.evaluate(async () => await (await fetch('/')).text())
    expect(html).toContain('Banana')
  })

  if (isLinux()) {
    test('Sessions', async () => {
      await page.goto(`${getServerUrl()}/`)
      expect(await page.textContent('body')).toContain('Logout')
      expect(await page.$('button >> text=Logout')).toBeTruthy()
      await page.waitForSelector('button:not([disabled]) >> text=Logout')
      await Promise.all([
        // It is important to call waitForNavigation before click to set up waiting.
        page.waitForNavigation(),
        // Does a page reload
        page.click('button >> text=Logout'),
      ])
      await sleep(3 * 1000) // Blind attempt to remove test flakiness
      expect(await page.$('button >> text=Logout')).toBeFalsy()
      expect(await page.$('button >> text=Create Account')).toBeTruthy()
      expect(await page.$('fieldset')).toBeTruthy()
      await autoRetry(async () => {
        expect(await page.$('fieldset:not([disabled])')).toBeTruthy()
      })

      await page.fill('input[type="text"]', 'Seb')
      // Doesn't do a page reload
      await page.click('button >> text=Create Account')
      expect(await page.$('fieldset')).toBeTruthy()
      await autoRetry(async () => {
        expect(await page.$('fieldset:not([disabled])')).toBeTruthy()
      })
      expect(await page.$('button >> text=Login as Seb')).toBeTruthy()
      expect(await page.$('button:not([disabled]) >> text=Login as Seb')).toBeTruthy()

      await Promise.all([
        // It is important to call waitForNavigation before click to set up waiting.
        page.waitForNavigation(),
        // Does a page reload
        page.click('button >> text=Login as Seb'),
      ])
      expect(await page.$('button >> text=Logout')).toBeTruthy()
      expect(await page.$('button >> text=Create Account')).toBeFalsy()
      expect(await page.$('fieldset')).toBeTruthy()
      // await page.waitForSelector('fieldset:not([disabled])', { state: 'attached' })
      await autoRetry(async () => {
        expect(await page.$('fieldset:not([disabled])')).toBeTruthy()
      })
      expect(await page.textContent('body')).toContain('Logged-in as Seb')
      expect((await page.$$('li')).length).toBe(1)
      expect(await page.textContent('body')).not.toContain('Cherries')

      await page.fill('input[type="text"]', 'Apples')
      // Doesn't do a page reload
      await page.click('button[type="submit"]')
      await autoRetry(async () => {
        expect(await page.textContent('body')).toContain('Apples')
      })
    })
  }
}
