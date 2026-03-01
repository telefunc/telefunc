export { testKitchenSink }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'

function testKitchenSink() {
  test('kitchen-sink: full run — scalars, fileEcho, progress, midEcho, summary', async () => {
    await page.goto(`${getServerUrl()}/kitchen-sink`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })

    await page.click('button:has-text("Run")')

    // Scalars arrive immediately once the response starts
    await autoRetry(async () => {
      expect(await page.textContent('body')).toContain('demo-1mb.bin')
    })

    // Wait for full completion — all streaming values done
    await autoRetry(async () => {
      const body = await page.textContent('body')
      expect(body).toContain('fileEcho ✓')
      expect(body).toContain('progress ✓')
      expect(body).toContain('midEcho ✓')
      expect(body).toContain('summary ✓')
    })

    const body = await page.textContent('body')

    // Checksum verified on client against server summary
    expect(body).toContain('echo checksum matches')

    // midEcho resolved before echo finished
    expect(body).not.toContain('assertion failed')

    // Blob preview is present
    expect(body).toContain('Hello from the nested Blob!')

    // No errors
    expect(body).not.toContain('checksum mismatch')
    expect(body).not.toContain('midEcho ✗')
  })

  test('kitchen-sink: abort mid-stream stops processing', async () => {
    await page.goto(`${getServerUrl()}/kitchen-sink`)
    await autoRetry(async () => {
      expect(await page.locator('#hydrated').count()).toBe(1)
    })

    await page.click('button:has-text("Run")')

    // Wait for streaming to start
    await autoRetry(async () => {
      expect(await page.textContent('body')).toContain('demo-1mb.bin')
    })

    // Abort mid-stream
    await page.click('button:has-text("Abort")')

    await autoRetry(async () => {
      const body = await page.textContent('body')
      expect(body).toContain('cancelled')
    })
  })
}
