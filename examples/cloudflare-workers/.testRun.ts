export { testRun }

import { page, test, expect, run, skip, fetchHtml, isCI, getServerUrl, autoRetry } from '@brillout/test-e2e'

function testRun(cmd: 'pnpm run dev' | 'pnpm run preview') {
  const isWrangler = cmd === 'pnpm run preview'

  // - `CLOUDFLARE_ACCOUNT_ID`/`CLOUDFLARE_API_TOKEN` not available for:
  //   - Vite's ecosystem CI
  //   - Pull Requests
  //     - https://github.community/t/feature-request-allow-secrets-in-approved-external-pull-requests/18071/4
  if (!process.env['CLOUDFLARE_ACCOUNT_ID']) {
    expect(process.env['CLOUDFLARE_ACCOUNT_ID']).toBeFalsy()
    expect(process.env['CLOUDFLARE_API_TOKEN']).toBeFalsy()
    if ((isCI() || process.env.VITE_ECOSYSTEM_CI) && isWrangler) {
      skip(
        "SKIPPED: wrangler tests cannot be run. Because missing environment variables `CLOUDFLARE_ACCOUNT_ID` and `CLOUDFLARE_API_TOKEN`. (This is expected in Pull Requests and Vite's ecosystem CI.)",
      )
      return
    }
  } else {
    expect(process.env['CLOUDFLARE_ACCOUNT_ID']).toBeTruthy()
    expect(process.env['CLOUDFLARE_API_TOKEN']).toBeTruthy()
  }

  {
    const additionalTimeout = !isWrangler ? 0 : (isCI() ? 2 : 1) * 10 * 1000
    const serverIsReadyMessage = (() => {
      if (isWrangler) {
        return (log: string) => log.includes('Listening at') || log.includes('Ready on')
      }
      // Vite/Express.js dev server
      return undefined
    })()
    run(cmd, {
      additionalTimeout,
      serverIsReadyMessage,
      // Randomly fails because of Cloudflare: it seems like uploading assets to Cloudflare sometimes fails.
      isFlaky: true,
    })
  }

  test('frontend', async () => {
    const html = await fetchHtml('/')
    expect(html).toContain('<div id="view">Loading...</div>')
  })

  test('telefunction hello()', async () => {
    await page.goto(getServerUrl() + '/')
    await autoRetry(async () => {
      expect(await page.textContent('#view')).toBe('Welcome Eva')
    })
  })
}
