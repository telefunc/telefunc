import { autoRetry, expect, getServerUrl, page, run, test } from '@brillout/test-e2e'

run('pnpm run preview')

test('telefunction call', async () => {
  await page.goto(getServerUrl() + '/')
  await autoRetry(async () => {
    expect(await page.textContent('#greeting')).toBe('Hello world')
  })
})
