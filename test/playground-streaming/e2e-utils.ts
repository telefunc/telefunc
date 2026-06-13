export {
  resetCleanupState,
  getCleanupState,
  resilientGoto,
  waitForHydration,
  getResult,
  sleep,
  restartProxy,
  stopProxy,
  startProxy,
}

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

/**
 * `page.goto()` that retries the navigation on transient Chromium-in-Docker network errors.
 * Right after the containers come up, Chromium's `NetworkChangeNotifier` can abort the in-flight
 * navigation with `ERR_NETWORK_CHANGED` (and connection-family friends) when the Docker bridge
 * interface state shifts — sometimes surfacing as a bounce to `chrome-error://chromewebdata/`
 * (see `isTransientNetworkError`). Re-navigating recovers, so retry a few times before giving up.
 */
async function resilientGoto(url: string, options?: Parameters<typeof page.goto>[1]) {
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      await page.goto(url, options)
      return
    } catch (err) {
      if (attempt === 3 || !isTransientNetworkError(err)) throw err
      // Pause before retrying so the next navigation lands after the bridge interface shift has
      // settled, instead of hammering the same broken window with back-to-back attempts.
      await sleep(500)
    }
  }
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
      // The reload can itself hit a transient `ERR_NETWORK_CHANGED`; swallow it and let the
      // next attempt retry, instead of failing the whole test on the recovery step.
      try {
        await page.reload({ waitUntil: 'load' })
      } catch (err) {
        if (!isTransientNetworkError(err)) throw err
      }
    }
  }
}

// Transient errors Chromium-in-Docker surfaces when the bridge interface state shifts. Mirrors
// the connection-family errors `.testRun-docker.ts`'s `tolerateError()` already allows in logs.
const transientNetworkErrors = [
  'ERR_NETWORK_CHANGED',
  'ERR_CONNECTION_CLOSED',
  'ERR_CONNECTION_RESET',
  'ERR_CONNECTION_REFUSED',
  'ERR_INTERNET_DISCONNECTED',
  'ERR_ALPN_NEGOTIATION_FAILED',
  'ERR_SSL_PROTOCOL_ERROR',
]
function isTransientNetworkError(err: unknown): boolean {
  const message = err instanceof Error ? err.message : String(err)
  // When a transient network error aborts an in-flight navigation, Chromium bounces to its internal
  // error page and Playwright reports the original navigation as "interrupted by another navigation
  // to chrome-error://chromewebdata/" — without surfacing the underlying ERR_* code. Treat that
  // bounce as transient too, so the navigation still gets retried.
  if (message.includes('chrome-error://chromewebdata')) return true
  return transientNetworkErrors.some((code) => message.includes(code))
}

async function getResult<T = any>(selector: string): Promise<T> {
  return JSON.parse((await page.textContent(selector))!)
}
