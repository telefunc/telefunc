export { testVideoChat }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'

type DecoderTally = { column: string | null; senders: Record<string, number>; errors: number }

function testVideoChat() {
  test('video chat: each side decodes only the other side’s video', async () => {
    await page.goto(`${getServerUrl()}/video-chat`)
    await page.evaluate(tallyFramesPerDecoder)
    // Retried until hydration makes the button act
    await autoRetry(async () => {
      await page.click('button:has-text("Join as Alice")')
      await page.locator('button:has-text("Join as Alice")').waitFor({ state: 'detached', timeout: 1000 })
    })
    await page.click('button:has-text("Join as Bob")')

    await autoRetry(async () => {
      const decoders = await page.evaluate(() => (window as unknown as { decoders: DecoderTally[] }).decoders)
      const seen = Object.fromEntries(
        decoders.map((d) => [d.column, Object.keys(d.senders).filter((name) => name !== 'either')]),
      )
      expect(seen).deep.equal({ Alice: ['Bob'], Bob: ['Alice'] })
      for (const d of decoders) {
        expect(Object.values(d.senders)[0]).to.be.greaterThan(15)
        expect(d.errors).to.equal(0)
      }
    })
  })
}

/** Labels each VP8 chunk with the participant whose encoder produced it (Alice joins first), and counts per decoder,
 *  named after the column it draws in, the chunks of each sender. Both encoders read the same fake camera, so a chunk
 *  both produced byte for byte counts as 'either'. */
function tallyFramesPerDecoder() {
  const hash = (bytes: Uint8Array) => {
    let h = 2166136261
    for (const b of bytes) h = Math.imul(h ^ b, 16777619)
    return `${h >>> 0}:${bytes.length}`
  }
  const bytesOf = (chunk: EncodedVideoChunk) => {
    const bytes = new Uint8Array(chunk.byteLength)
    chunk.copyTo(bytes)
    return bytes
  }
  const sender = new Map<string, string>()
  const encoderNames = ['Alice', 'Bob']
  const w = window as unknown as { decoders: DecoderTally[]; drawing: DecoderTally | null }
  w.decoders = []
  w.drawing = null

  const Encoder = VideoEncoder
  window.VideoEncoder = class extends Encoder {
    constructor(init: VideoEncoderInit) {
      const name = encoderNames.shift()!
      super({
        ...init,
        output(chunk, metadata) {
          const key = hash(bytesOf(chunk))
          const known = sender.get(key)
          sender.set(key, known === undefined || known === name ? name : 'either')
          init.output(chunk, metadata)
        },
      })
    }
  }
  const Decoder = VideoDecoder
  window.VideoDecoder = class extends Decoder {
    tally: DecoderTally
    constructor(init: VideoDecoderInit) {
      const tally: DecoderTally = { column: null, senders: {}, errors: 0 }
      super({
        output(frame) {
          w.drawing = tally
          init.output(frame)
          w.drawing = null
        },
        error(err) {
          tally.errors++
          init.error(err)
        },
      })
      this.tally = tally
      w.decoders.push(tally)
    }
    override decode(chunk: EncodedVideoChunk) {
      const name = sender.get(hash(bytesOf(chunk))) ?? 'unknown'
      this.tally.senders[name] = (this.tally.senders[name] ?? 0) + 1
      super.decode(chunk)
    }
  }
  const drawImage = CanvasRenderingContext2D.prototype.drawImage
  CanvasRenderingContext2D.prototype.drawImage = function (this: CanvasRenderingContext2D, ...args: unknown[]) {
    const column = this.canvas.parentElement!.parentElement!.parentElement!.querySelector('h2')!.textContent
    if (w.drawing) w.drawing.column ??= column
    return (drawImage as (...a: unknown[]) => void).apply(this, args)
  } as typeof drawImage
}
