export { testVideoChat }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'
import { canCutWire, cutWire } from '../../e2e-utils'

type Tally = { draws: Record<string, Record<string, number>>; decodeErrors: number }

function testVideoChat() {
  test('video chat: alone in the call, you see your own video come back', async () => {
    await openPage(page, ['Alice'])
    await join(page, 'Alice', 'Join as Alice')

    await autoRetry(async () => {
      const { draws, decodeErrors } = await tally(page)
      expect(draws.Alice?.Alice).to.be.greaterThan(15)
      expect(decodeErrors).to.equal(0)
    })
    await leave(page, 'Alice')
  })

  test('video chat: each side sees only the other side’s video', async () => {
    await openPage(page, ['Alice', 'Bob'])
    await join(page, 'Alice', 'Join as Alice')
    await join(page, 'Bob', 'Join as Bob')
    await page.evaluate(() => {
      ;(window as unknown as { tally: Tally }).tally.draws = {}
    })

    await autoRetry(async () => {
      const { draws, decodeErrors } = await tally(page)
      const seen = Object.fromEntries(
        Object.entries(draws).map(([column, senders]) => [column, Object.keys(senders).filter((s) => s !== 'either')]),
      )
      expect(seen).deep.equal({ Alice: ['Bob'], Bob: ['Alice'] })
      expect(draws.Alice!.Bob).to.be.greaterThan(15)
      expect(draws.Bob!.Alice).to.be.greaterThan(15)
      expect(decodeErrors).to.equal(0)
    })
    await leave(page, 'Alice')
    await leave(page, 'Bob')
  })

  test('video chat: a chat message reaches the other side with its sender’s name', async () => {
    await openPage(page, ['Alice', 'Bob'])
    await join(page, 'Alice', 'Join as Alice')
    await join(page, 'Bob', 'Join as Bob')

    await column(page, 'Alice').getByPlaceholder('Message').fill('hello from Alice')
    await column(page, 'Alice').getByRole('button', { name: 'Send' }).click()
    await autoRetry(async () => {
      expect(await column(page, 'Bob').getByRole('list', { name: 'Chat' }).textContent()).to.equal(
        'Alice: hello from Alice',
      )
    })
    await leave(page, 'Alice')
    await leave(page, 'Bob')
  })

  test('video chat: Switch here moves you from another tab', async () => {
    await openPage(page, ['Alice'])
    await join(page, 'Alice', 'Join as Alice')

    const otherTab = await page.context().browser()!.newPage()
    await openPage(otherTab, ['Alice'])
    await autoRetry(async () => {
      expect(await column(otherTab, 'Alice').textContent()).to.contain('Alice is already in the call in another tab')
    })
    await join(otherTab, 'Alice', 'Switch here')

    await autoRetry(async () => {
      expect(await otherTab.locator('p', { hasText: 'In the call:' }).textContent()).to.equal('In the call: Alice')
      expect(await column(page, 'Alice').textContent()).to.contain('Alice moved to another tab')
      const cameraStopped = await column(page, 'Alice')
        .locator('video')
        .evaluate((video: HTMLVideoElement) =>
          (video.srcObject as MediaStream).getTracks().every((track) => track.readyState === 'ended'),
        )
      expect(cameraStopped).to.equal(true)
      expect((await tally(otherTab)).draws.Alice?.Alice).to.be.greaterThan(15)
    })
    await leave(otherTab, 'Alice')
    await otherTab.close()
  })

  if (canCutWire())
    test('video chat: while the other side’s page can’t be reached, you see them reconnecting and your own video', async () => {
      await openPage(page, ['Alice'])
      await join(page, 'Alice', 'Join as Alice')
      const bobTab = await page.context().browser()!.newPage()
      await openPage(bobTab, ['Bob'])
      await join(bobTab, 'Bob', 'Join as Bob')
      const callText = () => page.locator('p', { hasText: 'In the call:' }).textContent()
      // Bob's frames come from another page's encoder, so they count as 'unknown' here
      const aliceDraws = async (expectDraws: (draws: Record<string, number>) => void) => {
        await page.evaluate(() => {
          ;(window as unknown as { tally: Tally }).tally.draws = {}
        })
        await autoRetry(async () => expectDraws((await tally(page)).draws.Alice ?? {}))
      }
      const seesBob = (draws: Record<string, number>) => expect(draws.unknown).to.be.greaterThan(15)
      const seesOwnOnly = (draws: Record<string, number>) => {
        expect(Object.keys(draws)).deep.equal(['Alice'])
        expect(draws.Alice).to.be.greaterThan(15)
      }
      await autoRetry(async () => expect(await callText()).to.equal('In the call: Alice, Bob'))
      await aliceDraws(seesBob)

      await bobTab.context().setOffline(true)
      await cutWire()
      await autoRetry(async () => expect(await callText()).to.equal('In the call: Alice, Bob (reconnecting…)'))
      await aliceDraws(seesOwnOnly)

      await bobTab.context().setOffline(false)
      await autoRetry(async () => expect(await callText()).to.equal('In the call: Alice, Bob'))
      await aliceDraws(seesBob)
      await leave(bobTab, 'Bob')
      await bobTab.close()
      await leave(page, 'Alice')
    })
}

async function openPage(p: Page, encoderNames: string[]) {
  await p.goto(`${getServerUrl()}/video-chat`)
  await p.evaluate(tallyDraws, encoderNames)
}

function column(p: Page, name: string) {
  return p.locator('h2', { hasText: name }).locator('..')
}

async function join(p: Page, name: string, button: string) {
  // Retried until hydration makes the button act
  await autoRetry(async () => {
    await column(p, name).getByRole('button', { name: button }).click({ timeout: 1000 })
    await column(p, name).getByRole('button', { name: 'Leave' }).waitFor({ timeout: 1000 })
  })
}

async function leave(p: Page, name: string) {
  await column(p, name).getByRole('button', { name: 'Leave' }).click()
  await column(p, name)
    .getByRole('button', { name: `Join as ${name}` })
    .waitFor()
}

async function tally(p: Page) {
  return await p.evaluate(() => (window as unknown as { tally: Tally }).tally)
}

/** Labels each VP8 chunk with the person whose encoder produced it (encoders start in `encoderNames` order), and
 *  counts, per column, the frames of each sender drawn in its Remote. Both encoders read the same fake camera, so a
 *  chunk both produced byte for byte counts as 'either'. */
function tallyDraws(encoderNames: string[]) {
  const hash = (chunk: EncodedVideoChunk) => {
    const bytes = new Uint8Array(chunk.byteLength)
    chunk.copyTo(bytes)
    let h = 2166136261
    for (const b of bytes) h = Math.imul(h ^ b, 16777619)
    return `${h >>> 0}:${bytes.length}`
  }
  const senderOf = new Map<string, string>()
  const tally: Tally = { draws: {}, decodeErrors: 0 }
  let drawing: string | null = null
  ;(window as unknown as { tally: Tally }).tally = tally

  const Encoder = VideoEncoder
  window.VideoEncoder = class extends Encoder {
    constructor(init: VideoEncoderInit) {
      const name = encoderNames.shift()!
      super({
        ...init,
        output(chunk, metadata) {
          const key = hash(chunk)
          const known = senderOf.get(key)
          senderOf.set(key, known === undefined || known === name ? name : 'either')
          init.output(chunk, metadata)
        },
      })
    }
  }
  const Decoder = VideoDecoder
  window.VideoDecoder = class extends Decoder {
    from: { sender: string }
    constructor(init: VideoDecoderInit) {
      const from = { sender: 'unknown' }
      super({
        output(frame) {
          drawing = from.sender
          init.output(frame)
          drawing = null
        },
        error(err) {
          tally.decodeErrors++
          init.error(err)
        },
      })
      this.from = from
    }
    override decode(chunk: EncodedVideoChunk) {
      this.from.sender = senderOf.get(hash(chunk)) ?? 'unknown'
      super.decode(chunk)
    }
  }
  const drawImage = CanvasRenderingContext2D.prototype.drawImage
  CanvasRenderingContext2D.prototype.drawImage = function (this: CanvasRenderingContext2D, ...args: unknown[]) {
    if (drawing) {
      let el: Element = this.canvas
      while (!el.querySelector('h2')) el = el.parentElement!
      const column = (tally.draws[el.querySelector('h2')!.textContent!] ??= {})
      column[drawing] = (column[drawing] ?? 0) + 1
    }
    return (drawImage as (...a: unknown[]) => void).apply(this, args)
  } as typeof drawImage
}
