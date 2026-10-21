export { testWire }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState } from '../../e2e-utils'

type Tally = { text: number; binary: number; inOrder: boolean }
type WireState = {
  resume: { channelId: string; received: Tally } | null
}

// Preview only: dev serves `/_telefunc` from Vite's middleware, where the server can't drop the page's connections.
function testWire(isDev: boolean) {
  if (isDev) return

  test('channel: 256 messages of 16 KiB each way, text and binary, each send awaited, all arrive once and in order though the server drops the connection halfway through each direction', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#wire-resume')
    const all: Tally = { text: 256, binary: 256, inOrder: true }
    await autoRetry(async () => {
      const { resume } = await getResult<WireState>('#wire-state')
      expect(resume?.received).deep.equal(all)
      const ss = await getCleanupState()
      expect(JSON.parse(ss[`wireResume_${resume!.channelId}`]!)).deep.equal(all)
      const cuts: number[] = JSON.parse(ss[`wireResume_${resume!.channelId}_cut`]!)
      expect(cuts.length).toBe(2)
      expect(cuts.every((cut) => cut > 0)).toBe(true)
    })
  })
}
