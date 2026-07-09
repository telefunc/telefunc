export {
  resetCleanupState,
  getCleanupState,
  forceServerGc,
  navigate,
  getResult,
  sleep,
  restartProxy,
  stopProxy,
  startProxy,
}

import { page, getServerUrl } from '@brillout/test-e2e'
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

/** Forces a full GC on the server (via `/api/gc`) so GC-driven cleanup is deterministic in tests. */
async function forceServerGc() {
  const resp = await fetch(`${getServerUrl()}/api/gc`, { method: 'POST' })
  // `/api/gc` 500s when the server wasn't started with `--expose-gc`, in which case it forced
  // nothing. Fail loudly and immediately instead of letting the GC test flake on incidental GC
  // pressure and time out ~90 s later with an inscrutable 'not-fired'.
  if (!resp.ok) {
    const reason = await resp.text().catch(() => '')
    throw new Error(`forceServerGc: /api/gc returned ${resp.status} — server GC not forced. ${reason}`)
  }
}

/**
 * Loads a page and waits for it to hydrate — resiliently, because the Docker suite runs the browser
 * against a Caddy container over the Docker bridge.
 *
 * When the bridge interface state shifts — right after the containers come up, and right after a
 * reconnect test restarts the proxy (`restartProxy`/`startProxy`) — Chromium's `NetworkChangeNotifier`
 * aborts whatever request is in flight with a transient `ERR_*` (see `isTransientNetworkError`). That
 * can kill the navigation outright, or drop an asset fetch so the page never hydrates. Re-navigating
 * recovers from both, so retry the navigate-and-hydrate as a unit before giving up.
 *
 * This is the navigation half of the suite's Docker-resilience story; the channel handshake half lives
 * in `openChannel()` (pages/channel/Channel.tsx), which retries the telefunc call the same aborts can
 * kill. Steady-state message delivery needs no such retry — telefunc's own reconnect/replay covers it.
 *
 * `target` defaults to the shared `page`; pass another page to load a second tab/context resiliently.
 */
async function navigate(url: string, options?: Parameters<typeof page.goto>[1], target: typeof page = page) {
  for (let attempt = 1; attempt <= NAVIGATE_ATTEMPTS; attempt++) {
    const lastAttempt = attempt === NAVIGATE_ATTEMPTS
    try {
      await target.goto(url, options)
    } catch (err) {
      // A transient bridge error aborted the navigation itself; re-navigate. Surface anything else.
      if (lastAttempt || !isTransientNetworkError(err)) throw err
      await sleep(NAVIGATE_RETRY_DELAY)
      continue
    }
    try {
      await target.locator('#hydrated').waitFor({ state: 'attached', timeout: HYDRATION_TIMEOUT })
      return
    } catch (err) {
      // Navigated, but an asset fetch was dropped on the bridge shift so the page never hydrated.
      // Re-navigate to refetch the missing chunks; the pause lets the interface shift settle first.
      if (lastAttempt) throw err
      await sleep(NAVIGATE_RETRY_DELAY)
    }
  }
}
const NAVIGATE_ATTEMPTS = 4
const HYDRATION_TIMEOUT = 5_000
const NAVIGATE_RETRY_DELAY = 500

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
