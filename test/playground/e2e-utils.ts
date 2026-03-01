export { resetCleanupState, getCleanupState, waitForHydration, getResult, sleep }

import { page, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function resetCleanupState() {
  await fetch(`${getServerUrl()}/api/cleanup-state/reset`, { method: 'POST' })
}

async function getCleanupState(): Promise<Record<string, string>> {
  const resp = await fetch(`${getServerUrl()}/api/cleanup-state`)
  return resp.json()
}

async function waitForHydration() {
  await autoRetry(async () => {
    expect(await page.locator('#hydrated').count()).toBe(1)
  })
}

async function getResult<T = any>(selector: string): Promise<T> {
  return JSON.parse((await page.textContent(selector))!)
}
