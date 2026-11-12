export { testZeroSettings }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

type ChannelState = {
  welcomeReceived: boolean
  pingAck: string | null
  perSendAck: string | null
  mainChannelOnCloseFiredClean: boolean | null
}

// Run by the lanes whose server sets every channel buffer and replay buffer to 0.
function testZeroSettings() {
  test('channel: with zero buffers and replay buffers, a page opens a channel, then another, and each answers', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#channel-connect')
    await autoRetry(async () => {
      const state = await getResult<ChannelState>('#channel-state')
      expect(state.welcomeReceived).toBe(true)
      expect(state.pingAck).toBe('server-ack:ping')
    })
    await page.click('#channel-test-per-send-ack')
    await autoRetry(async () => {
      expect((await getResult<ChannelState>('#channel-state')).perSendAck).toBe('ack:per-send-test')
    })
    // The server's ticks to a page gone would overflow a zero bufferLimit.
    await page.click('#channel-disconnect')
    await autoRetry(async () => {
      expect((await getResult<ChannelState>('#channel-state')).mainChannelOnCloseFiredClean).toBe(true)
    })
  })

  test('broadcast: with zero buffers and replay buffers, a broadcast delivers what is published once it opened, on both ends', async () => {
    await navigate(`${getServerUrl()}/publish`)
    await page.click('#test-broadcast-self-unsubscribe')
    await autoRetry(async () => {
      const result = await getResult<{ pageSeen: string[]; serverSeen: string[] }>('#publish-result')
      expect(result.pageSeen).deep.equal(['once:one', 'other:one', 'other:two'])
      expect(result.serverSeen).deep.equal(['once:one', 'other:one', 'other:two'])
    })
  })
}
