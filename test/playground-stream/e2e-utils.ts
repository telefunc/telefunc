export {
  resetCleanupState,
  getCleanupState,
  forceServerGc,
  cutWire,
  dropNextWebSocket,
  runServerAction,
  holdNextSsePost,
  holdNextSseWire,
  holdNextSseOpen,
  heldSsePosts,
  refuseNextSsePost,
  releaseHeldSsePosts,
  navigate,
  getResult,
  sleep,
  restartProxy,
  stopProxy,
  startProxy,
  countConnections,
  serverRuntime,
  setServerRuntime,
  canCutWire,
}

import { page, getServerUrl } from '@brillout/test-e2e'
import type { Response, WebSocket } from 'playwright-chromium'
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

type ServerRuntime = 'node' | 'bun' | 'deno'
let runtime: ServerRuntime = 'node'

/** The runtime serving the playground, as `runPlayground()` set it. */
function serverRuntime(): ServerRuntime {
  return runtime
}

function setServerRuntime(serving: ServerRuntime) {
  runtime = serving
}

/** Whether `cutWire()` can drop the page's connections: it destroys the Node sockets the server tracked, and Bun.serve and Deno.serve hand a request no socket. */
function canCutWire(): boolean {
  return runtime === 'node'
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
  // Fail loudly on `gc not exposed` (server missing `--expose-gc`): silently ignoring it
  // degrades GC tests into waiting for natural GC, which times out with a misleading assertion.
  if (!resp.ok) throw new Error(`forceServerGc() failed: ${await resp.text()}`)
}

/** Runs what a telefunction left under `key` with `onServerAction()` (server-actions.ts), also while the page is offline. */
async function runServerAction(key: string) {
  const resp = await fetch(`${getServerUrl()}/api/server-action?key=${encodeURIComponent(key)}`, { method: 'POST' })
  if (!resp.ok) throw new Error(`runServerAction() failed: ${await resp.text()}`)
}

/** Drops the page's WebSocket and SSE connections at the server, as a failing network would. */
async function cutWire() {
  const resp = await fetch(`${getServerUrl()}/api/wire/cut`, { method: 'POST' })
  const { cut } = (await resp.json()) as { cut: number }
  if (cut === 0) throw new Error('cutWire() found no connection to cut')
}

/** The server drops the page's next WebSocket before it opens, as a brief network drop would. */
async function dropNextWebSocket() {
  await fetch(`${getServerUrl()}/api/wire/next-websocket`, { method: 'POST' })
}

/** The server hands the page's next SSE batch POST to Telefunc only at `releaseHeldSsePosts()`, and answers it then. */
async function holdNextSsePost() {
  await fetch(`${getServerUrl()}/api/wire/next-sse-post?action=hold`, { method: 'POST' })
}

/** The POST that opens the page's next SSE wire is answered, and its first batch reaches Telefunc only at `releaseHeldSsePosts()`. */
async function holdNextSseWire() {
  await fetch(`${getServerUrl()}/api/wire/next-sse-post?action=hold-stream`, { method: 'POST' })
}

/** The server hands the POST that opens the page's next SSE wire to Telefunc only at `releaseHeldSsePosts()`. */
async function holdNextSseOpen() {
  await fetch(`${getServerUrl()}/api/wire/next-sse-post?action=hold-open`, { method: 'POST' })
}

/** How many SSE POSTs the server holds. */
async function heldSsePosts(): Promise<number> {
  const resp = await fetch(`${getServerUrl()}/api/wire/held`)
  return ((await resp.json()) as { held: number }).held
}

/** The server answers the page's next SSE batch POST with 400, as it does for a wire it closed. */
async function refuseNextSsePost() {
  await fetch(`${getServerUrl()}/api/wire/next-sse-post?action=refuse`, { method: 'POST' })
}

/** The server reads and answers the SSE POSTs it held; returns how many. */
async function releaseHeldSsePosts(): Promise<number> {
  const resp = await fetch(`${getServerUrl()}/api/wire/release`, { method: 'POST' })
  const { released } = (await resp.json()) as { released: number }
  return released
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
  // Chromium aborts a navigation that starts just after a test's setOffline(false).
  'ERR_ABORTED',
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

/** Counts the WebSockets and SSE streams the page opens to Telefunc from now until `stop()`. */
function countConnections() {
  let opened = 0
  const onWebSocket = (ws: WebSocket) => {
    if (ws.url().includes('/_telefunc')) opened++
  }
  const onResponse = (response: Response) => {
    if (response.headers()['content-type']?.startsWith('text/event-stream')) opened++
  }
  page.on('websocket', onWebSocket)
  page.on('response', onResponse)
  return {
    stop() {
      page.off('websocket', onWebSocket)
      page.off('response', onResponse)
      return opened
    },
  }
}
