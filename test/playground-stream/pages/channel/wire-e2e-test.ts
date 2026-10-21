export { testWire, testWireSmallReplay }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState, runServerAction, cutWire } from '../../e2e-utils'

type Tally = { text: number; binary: number; inOrder: boolean }
type WireState = {
  resume: { channelId: string; received: Tally } | null
  overrun: { channelId: string; received: Tally; onClose: string | null } | null
  end: { channelId: string; received: string[]; close: string | null } | null
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

  // The server sends these before it ends the channel.
  const sentBefore = ['m0', 'm1', 'm2']
  const ends = [
    { by: 'server', dropped: 'after' },
    { by: 'server', dropped: 'before' },
    { by: 'page', dropped: 'after' },
  ] as const
  for (const { by, dropped } of ends) {
    const when = dropped === 'after' ? 'on the connection, which then drops,' : 'once the connection dropped,'
    const end = async (channelId: string, action: 'close' | 'abort') => {
      await page.context().setOffline(true)
      if (dropped === 'before') await cutWire()
      if (by === 'server') await runServerAction(`wireEnd_${channelId}_${action}`)
      else await page.click(`#wire-end-${action}`)
      if (dropped === 'after') await cutWire()
      await page.context().setOffline(false)
    }
    const received = (onClose: string) => (by === 'server' ? [...sentBefore, onClose] : [onClose])

    test(`channel: a close() the ${by} makes while the page is offline, ${when} ends the channel gracefully on both ends, after what the server sent before it`, async () => {
      const channelId = await openEnd()
      await end(channelId, 'close')
      await autoRetry(async () => {
        const state = await getResult<WireState>('#wire-state')
        expect(state.end!.received).deep.equal(received('onClose:none'))
        const ss = await getCleanupState()
        expect(ss[`wireEnd_${channelId}_onClose`]).toBe('none')
        expect(by === 'server' ? ss[`wireEnd_${channelId}_close`] : state.end!.close).toBe('0')
      })
    })

    test(`channel: an abort(value) the ${by} makes while the page is offline, ${when} reaches the other end as Abort with that value, after what the server sent before it`, async () => {
      const channelId = await openEnd()
      await end(channelId, 'abort')
      const ended = `abort:${JSON.stringify({ by })}`
      await autoRetry(async () => {
        const state = await getResult<WireState>('#wire-state')
        expect(state.end!.received).deep.equal(received(`onClose:${ended}`))
        const ss = await getCleanupState()
        expect(ss[`wireEnd_${channelId}_onClose`]).toBe(ended)
      })
    })
  }

  test('channel: a channel the page closes while offline, its close() timing out, ends gracefully on the server once the page is back', async () => {
    const channelId = await openEnd()
    await page.context().setOffline(true)
    await page.click('#wire-end-close-timeout')
    await autoRetry(async () => {
      const { end } = await getResult<WireState>('#wire-state')
      expect(end!.close).toBe('1')
    })
    await cutWire()
    await page.context().setOffline(false)
    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`wireEnd_${channelId}_onClose`]).toBe('none')
    })
  })

  test('channel: aborting the signal of the call that returned a channel ends the server end with Abort', async () => {
    const channelId = await openEnd()
    await page.click('#wire-end-abort-call')
    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`wireEnd_${channelId}_onClose`]).toBe('abort:undefined')
    })
  })
}

async function openEnd(): Promise<string> {
  await navigate(`${getServerUrl()}/channel`)
  await page.click('#wire-end-open')
  let channelId = ''
  await autoRetry(async () => {
    const { end } = await getResult<WireState>('#wire-state')
    expect(end).not.toBe(null)
    channelId = end!.channelId
  })
  return channelId
}

// Run by the lanes whose server keeps 64 KiB of each end's text for a reconnect.
function testWireSmallReplay() {
  for (const from of ['server', 'page'] as const) {
    test(`channel: 256 KiB the ${from} sends without awaiting while the page is offline is more than the replay keeps: the channel closes with NetworkError on both ends, and no message after the gap arrives`, async () => {
      await navigate(`${getServerUrl()}/channel`)
      await page.click('#wire-overrun-open')
      let channelId = ''
      await autoRetry(async () => {
        const { overrun } = await getResult<WireState>('#wire-state')
        expect(overrun).not.toBe(null)
        channelId = overrun!.channelId
      })
      await page.context().setOffline(true)
      if (from === 'server') {
        await runServerAction(`wireOverrun_${channelId}`)
      } else {
        await page.click('#wire-overrun-send')
        await cutWire()
      }
      await page.context().setOffline(false)
      await autoRetry(async () => {
        const { overrun } = await getResult<WireState>('#wire-state')
        expect(overrun!.onClose).toBe('NetworkError')
        const ss = await getCleanupState()
        expect(ss[`wireOverrun_${channelId}_onClose`]).toBe('NetworkError')
        const received: Tally = from === 'server' ? overrun!.received : JSON.parse(ss[`wireOverrun_${channelId}`]!)
        expect(received.inOrder).toBe(true)
        expect(received.text).lessThan(16)
      })
    })
  }
}
