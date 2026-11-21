export { testUploadRefusal }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getCleanupState } from '../../e2e-utils'

type UploadRefused = {
  refused: { channelId: string; close: string }
  kept: { channelId: string; sent: number; close: string }
}

/** The card's state on `target`, the page of the row's own. */
async function cardOf(target: typeof page): Promise<UploadRefused | null> {
  const text = await target.textContent('#wire-state')
  if (text === null) throw new Error('no #wire-state')
  const state: { uploadRefused: UploadRefused | null } = JSON.parse(text)
  return state.uploadRefused
}

/** The bytes of the wire that the page's fetch wrapper needs (packages/telefunc/wire-protocol/wire/tags.ts and
 *  constants.ts): a frame is `[u8 tag][u16 LE channel][u32 LE seq][payload]`, sent on the upload after its u32 BE length. */
const WIRE = { textTag: 0x10, closeTag: 0x30, maxFrameBytes: 64 * 1024 * 1024 }

type Injection = { uploads: number; injected: boolean }
declare global {
  interface Window {
    uploadInjection?: Injection
  }
}

/** In the page: each streaming upload gets, after the frame of the text `"refused"`, the length of a frame over the
 *  server's limit and that frame's header, as a page that breaks the rule would send it. `window.uploadInjection`
 *  counts the uploads and says whether the frame went. */
function injectOverLimitFrame(wire: typeof WIRE) {
  const injection: Injection = { uploads: 0, injected: false }
  window.uploadInjection = injection
  const fetchPage = window.fetch
  window.fetch = (input, init) => {
    const body = init?.body
    if (!(body instanceof ReadableStream)) return fetchPage(input, init)
    injection.uploads++
    const injecting = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        controller.enqueue(chunk)
        if (injection.injected || chunk[0] !== wire.textTag || chunk.length < 7) return
        if (!new TextDecoder().decode(chunk.subarray(7)).includes(JSON.stringify('refused'))) return
        const view = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength)
        const length = new Uint8Array(4)
        new DataView(length.buffer).setUint32(0, wire.maxFrameBytes + 1)
        const header = new Uint8Array(7)
        const headerView = new DataView(header.buffer)
        header[0] = wire.closeTag
        headerView.setUint16(1, view.getUint16(1, true), true)
        headerView.setUint32(3, view.getUint32(3, true) + 1, true)
        controller.enqueue(length)
        controller.enqueue(header)
        injection.injected = true
      },
    })
    return fetchPage(input, { ...init, body: body.pipeThrough(injecting) })
  }
}

/** Docker lanes only: there the browser reaches the server over HTTP/2, the only way Chromium streams an upload. */
function testUploadRefusal() {
  test("channel: over HTTP/2, a frame of one channel's over the server's limit on the page's streaming upload ends that channel with the server's reason, and the other channel's messages each arrive once", async () => {
    const browser = page.context().browser()
    if (browser === null) throw new Error('no browser')
    // A page of its own: its fetch stays wrapped for as long as it lives.
    const context = await browser.newContext({ ignoreHTTPSErrors: true })
    try {
      const own = await context.newPage()
      await own.addInitScript(injectOverLimitFrame, WIRE)
      await navigate(`${getServerUrl()}/channel`, undefined, own)
      await own.click('#wire-upload-refused-open')
      await autoRetry(async () => {
        const uploadRefused = await cardOf(own)
        expect(uploadRefused?.kept.close).toBe('open')
      })
      await own.click('#wire-upload-refused-send-kept')
      await own.click('#wire-upload-refused-send-refused')
      await own.click('#wire-upload-refused-send-kept')
      await autoRetry(async () => {
        const uploadRefused = await cardOf(own)
        expect(uploadRefused?.refused.close).toBe(
          "Channel closed: the server refused what the page sent on it: A frame is over the server's frame limit.",
        )
      })
      await own.click('#wire-upload-refused-send-kept')
      await autoRetry(async () => {
        const uploadRefused = await cardOf(own)
        if (uploadRefused === null) throw new Error('no channels')
        expect(uploadRefused.kept).deep.equal({ channelId: uploadRefused.kept.channelId, sent: 3, close: 'open' })
        const ss = await getCleanupState()
        expect(JSON.parse(ss[`wireUpload_${uploadRefused.kept.channelId}`] ?? 'null')).deep.equal({
          text: 3,
          binary: 0,
          inOrder: true,
        })
      })
      // The frame went on a streaming upload, which the page took: not on a batch POST.
      const injection = await own.evaluate(() => window.uploadInjection)
      expect(injection?.injected).toBe(true)
    } finally {
      await context.close()
    }
  })
}
