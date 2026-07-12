export { testRun }

// Two real browser contexts (two commanders) drive the whole stack: cookie identity, the lobby
// room (presence + team pick via `setAttributes` + all-chat), match start (the authority joins and
// the 10 Hz binary state broadcast begins), the fog-filtered snapshot stream feeding the HUD, a
// command round-trip (`me.send` → authority `listen` → sim → `publishBinary` → decode → HUD), and a
// deterministic resign → win/lose across both clients.

import { autoRetry, expect, getServerUrl, page, run, test } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'

const uid = Math.random().toString(36).slice(2, 7)
const HOST = `Ironside${uid}` // creates the match → host
const GUEST = `Vega${uid}`

let b: Page // the second commander (own browser context = own session)
const extraContexts: Array<() => Promise<void>> = []

function testRun(cmd: 'pnpm dev' | 'pnpm preview') {
  run(cmd, {
    serverIsReadyMessage: (log) =>
      log.includes('Listening on') || (log.includes('Local:') && log.includes('http://localhost:3000')),
    tolerateError(log) {
      const t = log.logText
      return (
        // Telefunctions live in telefunc/ (an API layer) instead of collocated with components.
        t.includes('We recommend to collocate') ||
        // Dev serves over SSE; the browser's opportunistic WS upgrade fails ALPN — expected.
        t.includes('ERR_ALPN_NEGOTIATION_FAILED') ||
        (t.includes('WebSocket connection to') && t.includes('failed')) ||
        // Page navigations/closures tear down live room connections mid-flight.
        t.includes('Channel timed out: client did not reconnect') ||
        t.includes('The user aborted a request') ||
        t.includes('Telefunc call cancelled') ||
        // Headless runners have no GPU, so PixiJS renders through software (SwiftShader) WebGL —
        // which emits driver/perf chatter that is not an app error.
        t.includes('software WebGL') ||
        t.includes('SwiftShader') ||
        t.includes('swiftshader') ||
        t.includes('GL Driver Message') ||
        t.includes('GPU stall') ||
        t.includes('GroupMarkerNotSet') ||
        t.includes('Automatic fallback to software WebGL')
      )
    },
  })

  test('rts: sign in — cookie identity, reach the match browser', async () => {
    await signIn(page, HOST)
    await autoRetry(async () => {
      expect(await page.$('#create-match')).not.toBe(null)
    })
  })

  test('rts: create a match and enter its lobby', async () => {
    await page.fill('#create-match-name', 'Operation Nightfall')
    await page.click('#create-match')
    await autoRetry(async () => {
      expect(await page.$('#lobby')).not.toBe(null)
      expect(await page.textContent('#lobby-title')).toContain('Operation Nightfall')
    })
  })

  test('rts: a second commander sees the match and joins', async () => {
    b = await newContextPage()
    await signIn(b, GUEST)
    // The browser is `Room.list`-polled (no live directory event), so wait for the row.
    await autoRetry(async () => {
      expect(await b.$('[data-join]')).not.toBe(null)
    })
    await b.click('[data-join]')
    await autoRetry(async () => {
      expect(await b.$('#lobby')).not.toBe(null)
      // The host sees the guest appear in the roster (snapshot/onChange presence).
      expect(await page.$(`[data-lobby-player="${GUEST}"]`)).not.toBe(null)
    })
  })

  test('rts: all-chat crosses the wire (publish/subscribe)', async () => {
    await page.fill('#lobby-chat-input', 'gg hf')
    await page.press('#lobby-chat-input', 'Enter')
    await autoRetry(async () => {
      expect(await b.textContent('#lobby-chat-log')).toContain('gg hf')
    })
  })

  test('rts: pick teams, ready up, launch', async () => {
    await page.click('#team-red')
    await b.click('#team-blue')
    await page.click('#ready-btn')
    await b.click('#ready-btn')
    await autoRetry(async () => {
      expect(await page.$('#start-btn:not([disabled])')).not.toBe(null)
    })
    await page.click('#start-btn')
    await autoRetry(async () => {
      expect(await page.$('#match')).not.toBe(null)
      expect(await b.$('#match')).not.toBe(null)
    })
  })

  test('rts: the binary state stream is live — seeded army + running economy', async () => {
    // `@brillout/test-e2e`'s `expect` has no numeric matchers, so compare to a boolean and `.toBe`.
    await autoRetry(
      async () => {
        expect(digits(await page.textContent('#unit-count')) >= 5).toBe(true)
        expect(digits(await b.textContent('#unit-count')) >= 5).toBe(true)
      },
      { timeout: 25_000 },
    )
    const crystal0 = digits(await page.textContent('#crystal'))
    await autoRetry(
      async () => {
        expect(digits(await page.textContent('#crystal')) > crystal0).toBe(true)
      },
      { timeout: 25_000 },
    )
  })

  test('rts: command round-trip — produce a unit from HQ', async () => {
    // The camera centers on the player's HQ at match start, so screen-center selects the HQ.
    // Retry the click too (not just the panel check) in case centering hasn't settled yet.
    await autoRetry(
      async () => {
        const bb = await boundingBox(page, '#match')
        await page.mouse.click(bb.x + bb.width / 2, bb.y + bb.height / 2)
        expect(await page.$('[data-produce="worker"]')).not.toBe(null)
      },
      { timeout: 15_000 },
    )
    const units0 = digits(await page.textContent('#unit-count'))
    await page.click('[data-produce="worker"]')
    await autoRetry(
      async () => {
        expect(digits(await page.textContent('#unit-count')) > units0).toBe(true)
      },
      { timeout: 25_000 },
    )
  })

  test('rts: resign → deterministic victory/defeat across both clients', async () => {
    await page.click('#resign-btn')
    await autoRetry(async () => {
      expect(await page.$('#result-defeat')).not.toBe(null) // the host resigned
      expect(await b.$('#result-victory')).not.toBe(null) // the guest wins
    })
    await closeExtraContexts()
  })
}

// --- helpers ---

async function signIn(target: Page, name: string): Promise<void> {
  await target.goto(`${getServerUrl()}/`)
  await target.waitForSelector('#hydrated')
  await target.waitForSelector('#auth-name')
  await target.fill('#auth-name', name)
  await target.click('#auth-submit')
  await target.waitForSelector('#create-match-name')
}

async function newContextPage(): Promise<Page> {
  const browser = page.context().browser()
  if (browser === null) throw new Error('No browser')
  const context = await browser.newContext()
  extraContexts.push(() => context.close())
  const extraPage = await context.newPage()
  return extraPage
}

async function closeExtraContexts(): Promise<void> {
  for (const close of extraContexts.splice(0)) await close().catch(() => {})
}

function digits(text: string | null): number {
  return Number((text ?? '').replace(/\D/g, '')) || 0
}

async function boundingBox(
  target: Page,
  selector: string,
): Promise<{ x: number; y: number; width: number; height: number }> {
  const el = await target.$(selector)
  if (!el) throw new Error(`no ${selector}`)
  const box = await el.boundingBox()
  if (!box) throw new Error(`no box for ${selector}`)
  return box
}
