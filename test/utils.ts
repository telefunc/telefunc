export { testCounter }
export { testRunClassic }

import {
  page,
  expect,
  autoRetry,
  run,
  skip,
  test,
  fetchHtml,
  getServerUrl,
  editFile,
  editFileRevert,
} from '@brillout/test-e2e'

async function testCounter(currentValue = 0) {
  // autoRetry() in case page just got client-side navigated
  await autoRetry(
    async () => {
      const btn = page.locator('button', { hasText: 'Counter' })
      expect(await btn.textContent()).toBe(`Counter ${currentValue}`)
    },
    { timeout: 5 * 1000 },
  )
  // autoRetry() in case page isn't hydrated yet
  await autoRetry(
    async () => {
      const btn = page.locator('button', { hasText: 'Counter' })
      await btn.click()
      expect(await btn.textContent()).toBe(`Counter ${currentValue + 1}`)
    },
    { timeout: 5 * 1000 },
  )
}

function testRunClassic(
  cmd: 'pnpm run dev' | 'pnpm run preview' | 'pnpm run prod',
  {
    skipAboutPage,
    skipViteEcosystemCi,
    tolerateError,
    testHmr,
    isVue,
  }: {
    skipAboutPage?: true
    skipViteEcosystemCi?: true
    testHmr?: false | string
    isVue?: true
    tolerateError?: NonNullable<Parameters<typeof run>[1]>['tolerateError']
  } = {},
) {
  const isDev = cmd === 'pnpm run dev'

  if (skipViteEcosystemCi && process.env.VITE_ECOSYSTEM_CI) {
    skip("SKIPPED: skipping this test from Vite's ecosystem CI, see https://github.com/vikejs/vike/pull/2220")
    return
  }

  run(cmd, { tolerateError })

  test('page content is rendered to HTML', async () => {
    const html = await fetchHtml('/')
    expect(html).toContain('<h1>Welcome</h1>')
  })

  test('page is rendered to the DOM and interactive', async () => {
    await page.goto(getServerUrl() + '/')
    await page.click('a[href="/"]')
    expect(await page.textContent('h1')).toBe('Welcome')
    await testCounter()
  })

  if (isDev && testHmr !== false) {
    test('HMR', async () => {
      await testCounter(1)
      const org = 'Welcome'
      const mod = 'Wilkommen'
      expect(await page.textContent('h1')).toBe(org)
      editFile(testHmr || `./pages/index/+Page.${isVue ? 'vue' : 'tsx'}`, (s) => s.replace(org, mod))
      await autoRetry(
        async () => {
          expect(await page.textContent('h1')).toBe(mod)
        },
        { timeout: 5000 },
      )
      editFileRevert()
      await autoRetry(
        async () => {
          expect(await page.textContent('h1')).toBe(org)
        },
        { timeout: 5000 },
      )
      await testCounter(2)
    })
  }

  if (!skipAboutPage) {
    test('about page', async () => {
      await page.click('a[href="/about"]')
      await autoRetry(async () => {
        expect(await page.textContent('h1')).toBe('About')
      })
      expect(await page.textContent('p')).toBe('Example of using Vike.')
      const html = await fetchHtml('/about')
      expect(html).toContain('<h1>About</h1>')
    })
  }
}
