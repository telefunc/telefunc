export { testMedia, testMediaSlowLink }

import { page, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'
import { navigate } from '../../e2e-utils'
import { test, type WireCheck } from '../../laneWire'
import type { MeetingResult } from './Meeting'

/** `check`: the wire, in a lane that names none. */
function testMedia(check?: WireCheck) {
  test(
    'media: two pages publish and play each other, every frame whole, at the size that fits, with the tone',
    async () => {
      const room = `two-${Date.now()}`
      const bob = await page.context().browser()!.newPage()
      await join(page, room, 'Alice')
      await join(bob, room, 'Bob')

      await autoRetry(
        async () => {
          for (const [viewer, other] of [
            [page, 'Bob'],
            [bob, 'Alice'],
          ] as const) {
            const seen = (await meeting(viewer))[other]!
            expect(seen.frames.checked).greaterThan(60)
            expect(seen.frames.broken).toBe(0)
            // The element is 360 px tall: the picture starts at the smallest size, and goes up while frames come on time.
            expect(seen.stats.video!.height).toBe(360)
            expect(seen.stats.audio!.played).greaterThan(100)
            // Not heard: the failure shows how loud it was, and the listening context's state.
            expect(seen.tone.heard || seen.tone).toBe(true)
          }
        },
        { timeout: 30_000 },
      )
      allowSkips()
      await bob.close()
    },
    check,
  )
}

/** On a wire Chrome's network emulation slows: WebSocket and SSE, not WebTransport's QUIC. */
function testMediaSlowLink(check: WireCheck) {
  test(
    'media: a page on a downlink slower than the picture skips whole groups, and shows no broken frame',
    async () => {
      const room = `slow-${Date.now()}`
      const bob = await page.context().browser()!.newPage()
      await join(page, room, 'Alice')
      // The page loads at full speed: only the call is on the slow downlink.
      await open(bob, room, 'Bob', 180)
      const cdp = await bob.context().newCDPSession(bob)
      await cdp.send('Network.enable')
      // 160 kbit/s: under the smallest picture's 160 kbit/s and the sound's 64 kbit/s together.
      await cdp.send('Network.emulateNetworkConditions', {
        offline: false,
        latency: 0,
        downloadThroughput: 20_000,
        uploadThroughput: -1,
      })
      await bob.click('#media-join')

      await autoRetry(
        async () => {
          const seen = (await meeting(bob)).Alice!
          expect(seen.stats.video!.skippedGroups).greaterThan(0)
          expect(seen.frames.checked).greaterThan(10)
          expect(seen.frames.broken).toBe(0)
        },
        { timeout: 40_000 },
      )
      expectLog('fell behind and skipped frames of its lossy tracks')
      await cdp.detach()
      await bob.close()
    },
    check,
  )
}

/** A page whose path is busy for a moment skips to the newest group, which the server logs once per page: the media
 *  plays on, so the log is no failure, nor something every run shows. */
function allowSkips(): void {
  try {
    expectLog('fell behind and skipped frames of its lossy tracks')
  } catch {}
}

async function join(target: Page, room: string, name: string): Promise<void> {
  await open(target, room, name, 360)
  await target.click('#media-join')
}

async function open(target: Page, room: string, name: string, height: number): Promise<void> {
  await navigate(`${getServerUrl()}/media?room=${room}&name=${name}&height=${height}`, undefined, target)
}

async function meeting(target: Page): Promise<MeetingResult> {
  return await target.evaluate(() => (window as { meetingResult?(): MeetingResult }).meetingResult?.() ?? {})
}
