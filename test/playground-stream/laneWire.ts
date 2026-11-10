export { test, checkWire, expectWire }

// A lane that names its wire (`checkWire`) has each test end by checking, from every page the test used, that the
// channel data went on that wire: a page that falls back a rung still passes a test that only checks what arrived.
import { page, test as testE2e, expect, autoRetry } from '@brillout/test-e2e'
import type { Browser, BrowserContext, Page } from 'playwright-chromium'
import { forgetWireData, installWireProbe, readWire, type Wire, type WireReport } from './wireProbe'

type WireCheck = {
  wire: Wire
  /** Each Room track on a domain of its own: on WebTransport, no binary publish on a stream that announced domains. */
  tracksOnOwnStreams?: boolean
}

let laneCheck: WireCheck | null = null
let testCheck: WireCheck | null = null
/** The pages the running test used, and the contexts it opened, which close after it. */
const testPages = new Set<Page>()
const testContexts = new Set<BrowserContext>()
const probed = new WeakSet<Page | Browser>()
/** How long a page has, after its test, to retire the wires it climbed from. */
const WIRE_SETTLE_MS = 5_000

/** The wire every test of this lane runs its channels on. */
function checkWire(check: WireCheck): void {
  laneCheck = check
}

/** The wire this test runs its channels on, where it isn't its lane's. */
function expectWire(check: WireCheck): void {
  testCheck = check
}

function test(name: string, fn: () => void | Promise<void>): void {
  testE2e(name, async () => {
    testCheck = laneCheck
    testPages.clear()
    testContexts.clear()
    if (laneCheck) {
      await probe(page)
      await page.evaluate(forgetWireData)
    }
    try {
      await fn()
      if (testCheck) await assertWire(testCheck)
    } finally {
      for (const context of testContexts) await context.close()
    }
  })
}

/** Installs the probe in `target` and in every page the browser opens from now on. */
async function probe(target: Page): Promise<void> {
  testPages.add(target)
  if (!probed.has(target)) {
    probed.add(target)
    await target.addInitScript(installWireProbe)
  }
  const browser = target.context().browser()!
  if (probed.has(browser)) return
  probed.add(browser)
  const newPage = browser.newPage.bind(browser)
  browser.newPage = async (...args) => {
    const opened = await newPage(...args)
    testContexts.add(opened.context())
    await probe(opened)
    return opened
  }
  const newContext = browser.newContext.bind(browser)
  browser.newContext = async (...args) => {
    const context = await newContext(...args)
    testContexts.add(context)
    await context.addInitScript(installWireProbe)
    context.on('page', (opened) => testPages.add(opened))
    return context
  }
}

async function assertWire(check: WireCheck): Promise<void> {
  await autoRetry(
    async () => {
      const reports = await Promise.all(
        [...testPages].filter((used) => !used.isClosed()).map((used) => used.evaluate(readWire)),
      )
      const carried = reports.filter(
        (report): report is WireReport =>
          report !== null && report.webtransport.dataIn + report.ws.dataIn + report.sse.bytesIn > 0,
      )
      expect(carried.length).greaterThan(0)
      for (const report of carried) expectOn(report, check)
    },
    { timeout: WIRE_SETTLE_MS },
  )
}

/** The page's channel data went on `wire`, and no other wire is still open. */
function expectOn(report: WireReport, { wire, tracksOnOwnStreams }: WireCheck): void {
  const fail = (what: string) => {
    throw new Error(`Not on ${wire}: ${what}, in ${JSON.stringify(report)}`)
  }
  const others = report.live.filter((live) => live !== wire)
  if (others.length > 0) fail(`${others.join(' and ')} still open`)
  if (wire === 'webtransport' && report.webtransport.dataIn === 0) fail('no data on WebTransport')
  if (wire === 'ws' && report.ws.dataIn === 0) fail('no data on the WebSocket')
  if (wire !== 'webtransport' || !tracksOnOwnStreams) return
  for (const way of ['in', 'out'] as const) {
    const streams = report.streams[way]
    if (streams.both > 0) fail(`binary publishes ${way} on a stream with DOMAIN frames`)
    if (streams.binaryPublish > 0 && streams.domain === 0) fail(`binary publishes ${way} with no domain announced`)
  }
}
