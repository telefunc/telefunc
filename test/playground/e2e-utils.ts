export { resetCleanupState, getCleanupState, waitForHydration, getResult, sleep, restartProxy, stopProxy, startProxy }

import { page, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { execSync } from 'node:child_process'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const playgroundDir = dirname(fileURLToPath(import.meta.url))

/**
 * Force-severs every browser-to-origin connection by restarting the Caddy container.
 * Used by reconnect tests where browser offline mode is insufficient — Chromium's
 * `setOffline(true)` doesn't kill in-flight chunked POSTs (long-lived `streamRequest`
 * survives), so the only reliable way to test reconnect on those wires is to terminate
 * Caddy at the TCP layer.
 */
function restartProxy() {
  execSync('docker compose restart proxy', { cwd: playgroundDir, stdio: 'pipe' })
}

/** Like `restartProxy` but split — stop indefinitely, then `startProxy` brings it back.
 *  Use when the test needs to perform actions while disconnected (queued sends, clicks). */
function stopProxy() {
  execSync('docker compose stop proxy', { cwd: playgroundDir, stdio: 'pipe' })
}

function startProxy() {
  execSync('docker compose start proxy', { cwd: playgroundDir, stdio: 'pipe' })
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function resetCleanupState() {
  await fetch(`${getServerUrl()}/api/cleanup-state/reset`, { method: 'POST' })
}

async function getCleanupState(): Promise<Record<string, string>> {
  const resp = await fetch(`${getServerUrl()}/api/cleanup-state`)
  return resp.json()
}

async function waitForHydration() {
  // Chromium-in-Docker can drop in-flight asset fetches with `ERR_NETWORK_CHANGED`
  // when the bridge interface state shifts, leaving the page un-hydrated. Retry
  // the navigation a few times before giving up.
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      await page.locator('#hydrated').waitFor({ state: 'attached', timeout: 5_000 })
      return
    } catch {
      if (attempt === 3) throw new Error('Page never hydrated after 4 attempts')
      await page.reload({ waitUntil: 'load' })
    }
  }
}

async function getResult<T = any>(selector: string): Promise<T> {
  return JSON.parse((await page.textContent(selector))!)
}
