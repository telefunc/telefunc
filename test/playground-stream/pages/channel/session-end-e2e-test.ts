export { testSessionEnd }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, sleep } from '../../e2e-utils'
import { cardOf } from './upload-refusal-e2e-test'
import { exposeSendUnreadable, sessionEndedByUnreadable } from '../../../utils'

/** Lanes on WebSocket: whether the runtime's WebSocket close hands the browser's page the code and the reason the server
 *  closed it with, which carry the end. */
function testSessionEnd() {
  test("channel: a session the server ends tells each of its channels its cause by the WebSocket's close, its code and reason as the browser hands them to the page, and the page opens no other WebSocket", async () => {
    const browser = page.context().browser()
    if (browser === null) throw new Error('no browser')
    // A page of its own: its WebSocket stays wrapped for as long as it lives.
    const context = await browser.newContext({ ignoreHTTPSErrors: true })
    try {
      const own = await context.newPage()
      await own.addInitScript(exposeSendUnreadable)
      const sockets: string[] = []
      own.on('websocket', (socket) => void sockets.push(socket.url()))
      await navigate(`${getServerUrl()}/channel`, undefined, own)
      await own.click('#wire-upload-refused-open')
      await autoRetry(async () => {
        expect((await cardOf(own))?.kept.close).toBe('open')
      })
      const opened = sockets.length
      expect(await own.evaluate(() => window.sendUnreadable?.() ?? false)).toBe(true)
      await autoRetry(async () => {
        const card = await cardOf(own)
        expect([card?.refused.close, card?.kept.close]).deep.equal([sessionEndedByUnreadable, sessionEndedByUnreadable])
      })
      expect(await own.evaluate(() => window.socketCloses?.at(-1))).deep.equal({
        code: 4002,
        reason: 'text/vnd.telefunc.refusal; kind=frames; cause=8',
      })
      // A channel told its end needs no other wire.
      await sleep(1_000)
      expect(sockets.length).toBe(opened)
    } finally {
      await context.close()
    }
  })
}
