export { testWire, testWireSmallReplay, testWireSlowPing, testWireShortReconnect }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { startBlackHoleProxy } from '../../black-hole-proxy'
import type { Response, WebSocket } from 'playwright-chromium'
import {
  navigate,
  getResult,
  getCleanupState,
  runServerAction,
  cutWire,
  canCutWire,
  dropNextWebSocket,
  forceServerGc,
  holdNextSsePost,
  holdNextSseWire,
  holdNextSseOpen,
  heldSsePosts,
  refuseNextSsePost,
  releaseHeldSsePosts,
} from '../../e2e-utils'
import { withNetworkConditions } from '../../../utils'

type Tally = { text: number; binary: number; inOrder: boolean }
type WireState = {
  resume: { channelId: string; received: Tally } | null
  overrun: { channelId: string; received: Tally; onClose: string | null } | null
  end: { channelId: string; received: string[]; close: string | null } | null
  large: number | null
  many: { closed: number; next: string } | null
  held: { run: string; first: number; closed: number } | null
  release: { channelId: string; close: string } | null
  upload: { channelId: string; sent: number } | null
}

// The end card's channel sends these before the server ends it.
const sentBefore = ['m0', 'm1', 'm2']

// Preview only: dev serves `/_telefunc` from Vite's middleware, where the server can't drop the page's connections.
function testWire(isDev: boolean) {
  if (isDev) return

  if (canCutWire())
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

  const ends = [
    { by: 'server', dropped: 'after' },
    { by: 'server', dropped: 'before' },
    { by: 'page', dropped: 'after' },
  ] as const
  for (const { by, dropped } of canCutWire() ? ends : []) {
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

  if (canCutWire())
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

  if (process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS === JSON.stringify(['ws'])) {
    // MAX_CHANNELS_PER_CONNECTION
    const maxChannels = 4_096
    const connect = async () => {
      // The wire's version, as the page's `getWireUrl` names it.
      const url = new URL('/_telefunc?wire=1', getServerUrl())
      url.protocol = 'ws:'
      const ws = new globalThis.WebSocket(url)
      ws.binaryType = 'arraybuffer'
      await new Promise((resolve) => (ws.onopen = resolve))
      let sessionId: string | undefined
      // Takes the first answer to the RECONCILE in flight.
      let settle: ((answer: 'reconciled' | 'closed') => void) | undefined
      const answered = (answer: 'reconciled' | 'closed') => {
        settle?.(answer)
        settle = undefined
      }
      ws.onclose = () => answered('closed')
      ws.onmessage = (event) => {
        const frame = new Uint8Array(event.data as ArrayBuffer)
        // RECONCILED, its JSON after the 7-byte header.
        if (frame[0] !== 0x05) return
        sessionId = JSON.parse(new TextDecoder().decode(frame.subarray(7))).sessionId
        answered('reconciled')
      }
      // A RECONCILE naming what changed once the first is answered; one on a socket the server closed isn't sent.
      const answer = (reconcile: { open: object[]; released?: number[] }) =>
        new Promise<'reconciled' | 'closed' | 'already closed'>((resolve) => {
          // CLOSING from the server's close frame on, ahead of the close event.
          if (ws.readyState !== globalThis.WebSocket.OPEN) return resolve('already closed')
          settle = resolve
          const payload = new TextEncoder().encode(JSON.stringify({ sessionId, ...reconcile }))
          // A RECONCILE: tag 0x04, then index 0 and seq 0 in the 7-byte header.
          const frame = new Uint8Array(7 + payload.byteLength)
          frame[0] = 0x04
          frame.set(payload, 7)
          ws.send(frame)
        })
      return { ws, answer }
    }

    test('channel: a channel a call returns while the WebSocket is dead without a word opens on both ends once the page notices', async () => {
      const proxy = await startBlackHoleProxy(getServerUrl())
      try {
        await navigate(`${proxy.url}/channel`)
        await page.click('#wire-end-open')
        let firstId = ''
        await autoRetry(async () => {
          const { end } = await getResult<WireState>('#wire-state')
          expect((await getCleanupState())[`wireEnd_${end!.channelId}_onOpen`]).toBe('true')
          firstId = end!.channelId
        })
        expect(proxy.blackHoleWebSockets()).toBe(1)
        await page.click('#wire-end-open')
        await autoRetry(async () => {
          const { end } = await getResult<WireState>('#wire-state')
          expect(end!.channelId).not.toBe(firstId)
          const ss = await getCleanupState()
          expect(ss[`wireEnd_${end!.channelId}_onOpen`]).toBe('true')
          expect(ss[`wireEnd_${end!.channelId}_onClose`]).toBe(undefined)
          expect(end!.received).deep.equal([])
        })
      } finally {
        await proxy.close()
      }
    })

    test('channel: a RECONCILE naming more channels than a connection may hold closes the WebSocket before any RECONCILED', async () => {
      const open = (entries: number) =>
        Array.from({ length: entries }, (_, ix) => ({ id: `unknown-${ix}`, ix, lastSeq: 0 }))
      const fits = await connect()
      const over = await connect()
      try {
        expect(await fits.answer({ open: open(maxChannels) })).toBe('reconciled')
        expect(await over.answer({ open: open(maxChannels + 1) })).toBe('closed')
      } finally {
        fits.ws.close()
        over.ws.close()
      }
    })

    test('channel: a RECONCILE naming new channels past what a connection may hold, on top of those it holds, or naming a channel it holds under another id, closes the WebSocket', async () => {
      // Channels the page opened that the server hasn't registered yet, so it awaits each one.
      const opened = (from: number, count: number, name = 'unregistered') =>
        Array.from({ length: count }, (_, n) => ({
          id: `${name}-${from + n}`,
          ix: from + n,
          lastSeq: 0,
          initial: true,
        }))
      const full = await connect()
      const renaming = await connect()
      try {
        expect(await full.answer({ open: opened(0, maxChannels) })).toBe('reconciled')
        // The page released 16 and opened 16: it still holds as many.
        const released = Array.from({ length: 16 }, (_, ix) => ix)
        expect(await full.answer({ released, open: opened(maxChannels, 16) })).toBe('reconciled')
        expect(await full.answer({ released: [], open: opened(maxChannels + 16, 1) })).toBe('closed')

        expect(await renaming.answer({ open: opened(0, 1) })).toBe('reconciled')
        expect(await renaming.answer({ released: [], open: opened(0, 1, 'renamed') })).toBe('closed')
      } finally {
        full.ws.close()
        renaming.ws.close()
      }
    })

    test('channel: a LENGTH frame that lies about the frame after it closes its WebSocket alone, and the server serves the others', async () => {
      // A frame's 7-byte header: tag, u16 index, u32 seq (a LENGTH's seq: the bytes of the frame after it).
      const frame = (tag: number, seq: number, bytes = 7) => {
        const f = new Uint8Array(bytes)
        f[0] = tag
        new DataView(f.buffer).setUint32(3, seq, true)
        return f
      }
      const LENGTH = 0x0d
      const text = frame(0x10, 1, 10)
      const window = frame(0x34, 0, 15)
      const message = (...frames: Uint8Array[]) => {
        const m = new Uint8Array(frames.reduce((n, f) => n + f.byteLength, 0))
        let at = 0
        for (const f of frames) {
          m.set(f, at)
          at += f.byteLength
        }
        return m
      }
      const lies = {
        'last in its message': message(frame(LENGTH, 10)),
        'past its message': message(frame(LENGTH, 1000), text, text),
        'shorter than a header': message(frame(LENGTH, 3), text, text),
        'ahead of a frame of fixed size': message(frame(LENGTH, 15), window, text),
        'ahead of a LENGTH': message(frame(LENGTH, 17), frame(LENGTH, 10), text, text),
      }
      const healthy = await connect()
      try {
        expect(await healthy.answer({ open: [] })).toBe('reconciled')
        for (const [lie, bytes] of Object.entries(lies)) {
          const hostile = await connect()
          expect(await hostile.answer({ open: [] })).toBe('reconciled')
          const closed = await new Promise<boolean>((resolve) => {
            hostile.ws.addEventListener('close', () => resolve(true))
            setTimeout(() => resolve(false), 5_000)
            hostile.ws.send(bytes)
          })
          expect({ lie, closed }).deep.equal({ lie, closed: true })
        }
        expect(await healthy.answer({ released: [], open: [] })).toBe('reconciled')
      } finally {
        healthy.ws.close()
      }
    })

    test('channel: a channel whose first WebSocket a network drop stops opens on the next', async () => {
      await navigate(`${getServerUrl()}/channel`)
      await dropNextWebSocket()
      await page.click('#wire-end-open')
      await autoRetry(async () => {
        const { end } = await getResult<WireState>('#wire-state')
        expect((await getCleanupState())[`wireEnd_${end!.channelId}_onOpen`]).toBe('true')
        expect(end!.received).deep.equal([])
      })
    })
  }

  if (process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS === JSON.stringify(['sse', 'ws'])) {
    test('channel: a 1 MB message the server sends at 1 Mbit/s as the page upgrades from SSE to WebSocket arrives, and the page moves to the WebSocket without opening another SSE stream', async () => {
      await navigate(`${getServerUrl()}/channel`)
      let sseStreams = 0
      const onResponse = (response: Response) => {
        if (response.headers()['content-type']?.startsWith('text/event-stream')) sseStreams++
      }
      let webSockets = 0
      const onWebSocket = (ws: WebSocket) => {
        if (ws.url().includes('/_telefunc')) webSockets++
      }
      page.on('response', onResponse)
      page.on('websocket', onWebSocket)
      try {
        // 1 Mbit/s each way.
        await withNetworkConditions({ downloadThroughput: 125_000, uploadThroughput: 125_000 }, async () => {
          await page.click('#wire-large')
          await autoRetry(
            async () => {
              const { large } = await getResult<WireState>('#wire-state')
              expect(large).toBe(1_000_000)
            },
            { timeout: 30_000 },
          )
          expect(sseStreams).toBe(1)
          expect(webSockets).toBe(1)
        })
      } finally {
        page.off('response', onResponse)
        page.off('websocket', onWebSocket)
      }
    })
  }

  for (const { button, when } of [
    { button: 'wire-release', when: 'once the page has all the server sent' },
    { button: 'wire-release-early', when: 'as it opened, with what the server sent on its way' },
  ]) {
    test(`channel: the server lets go of a channel the page closed ${when}`, async () => {
      await navigate(`${getServerUrl()}/channel`)
      await page.click(`#${button}`)
      let channelId = ''
      await autoRetry(async () => {
        const { release } = await getResult<WireState>('#wire-state')
        expect(release!.close).toBe('0')
        channelId = release!.channelId
      })
      // Well within reconnectTimeout (60 s), which the server waits for a page that is away.
      await autoRetry(
        async () => {
          await forceServerGc()
          await runServerAction(`wireRelease_${channelId}`)
          expect((await getCleanupState())[`wireRelease_${channelId}`]).toBe('released')
        },
        { timeout: 10_000 },
      )
    })
  }

  test('channel: a page that opened and closed 4,200 channels on one connection opens the next', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#wire-many')
    await autoRetry(async () => {
      const { many } = await getResult<WireState>('#wire-state')
      expect(many).deep.equal({ closed: 4_200, next: 'open' })
    })
  })

  test('channel: 100 channels a page opens one at a time while 1,000 stay open each get their first message, every close is acknowledged on both ends, and what the page sends on a kept one as each opens arrives', async () => {
    await navigate(`${getServerUrl()}/channel`)
    await page.click('#wire-held')
    await autoRetry(
      async () => {
        const { held } = await getResult<WireState>('#wire-state')
        expect(held && { first: held.first, closed: held.closed }).deep.equal({ first: 1_100, closed: 1_100 })
        const ss = await getCleanupState()
        expect(ss[`wireHeld_${held!.run}`]).toBe('1100')
        expect(ss[`wireHeldKept_${held!.run}`]).toBe('100')
      },
      { timeout: 30_000 },
    )
  })

  if (process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS === JSON.stringify(['sse'])) {
    test("channel: a message sent with an SSE wire's first batch, which the server held until the connection dropped, arrives once and before the next", async () => {
      await navigate(`${getServerUrl()}/channel`)
      await holdNextSseWire()
      await page.click('#wire-upload-open')
      await autoRetry(async () => {
        expect(await heldSsePosts()).toBe(1)
      })
      await cutWire()
      await page.click('#wire-upload-send')
      await autoRetry(async () => {
        const { upload } = await getResult<WireState>('#wire-state')
        expect(upload!.sent).toBe(2)
        const ss = await getCleanupState()
        expect(JSON.parse(ss[`wireUpload_${upload!.channelId}`] ?? 'null')).deep.equal({
          text: 2,
          binary: 0,
          inOrder: true,
        })
      })
      await releaseHeldSsePosts()
    })

    for (const post of ['refuses', 'reads late'] as const) {
      test(`channel: messages the page sends in an SSE POST the server ${post} arrive once and in order`, async () => {
        await navigate(`${getServerUrl()}/channel`)
        await page.click('#channel-test-upstream-open')
        let channelId = ''
        await autoRetry(async () => {
          channelId = (await getResult<{ upstreamReconnectChannelId: string | null }>('#channel-state'))
            .upstreamReconnectChannelId!
          expect(channelId).not.toBe(null)
        })
        await page.click('#channel-test-upstream-send')
        await autoRetry(async () => {
          expect((await getCleanupState())[`upstream_${channelId}_receivedCount`]).toBe('1')
        })
        if (post === 'refuses') await refuseNextSsePost()
        else await holdNextSsePost()
        for (let i = 0; i < 3; i++) await page.click('#channel-test-upstream-send')
        if (post === 'reads late') {
          // The page gives a POST up 30 s after it went out, if no ping round trip failed first.
          await autoRetry(
            async () => {
              expect((await getCleanupState())[`upstream_${channelId}_receivedCount`]).toBe('4')
            },
            { timeout: 40_000 },
          )
          expect(await releaseHeldSsePosts()).toBe(1)
        }
        await page.click('#channel-test-upstream-send')
        await autoRetry(async () => {
          const ss = await getCleanupState()
          expect(ss[`upstream_${channelId}_receivedCount`]).toBe('5')
          expect(ss[`upstream_${channelId}_lastSeq`]).toBe('5')
          expect(ss[`upstream_${channelId}_hasGap`]).toBe('false')
        })
      })
    }

    test("channel: messages the page sends while the server holds its SSE open past the page's 10 s deadline arrive once and in order on the wire the page opens next", async () => {
      await navigate(`${getServerUrl()}/channel`)
      await page.click('#channel-test-upstream-open')
      let channelId = ''
      await autoRetry(async () => {
        channelId = (await getResult<{ upstreamReconnectChannelId: string | null }>('#channel-state'))
          .upstreamReconnectChannelId!
        expect(channelId).not.toBe(null)
      })
      await page.click('#channel-test-upstream-send')
      await autoRetry(async () => {
        expect((await getCleanupState())[`upstream_${channelId}_receivedCount`]).toBe('1')
      })
      await holdNextSseOpen()
      await cutWire()
      await autoRetry(async () => {
        expect(await heldSsePosts()).toBe(1)
      })
      for (let i = 0; i < 3; i++) await page.click('#channel-test-upstream-send')
      await autoRetry(
        async () => {
          expect((await getCleanupState())[`upstream_${channelId}_receivedCount`]).toBe('4')
        },
        { timeout: 40_000 },
      )
      // The held open is answered now, after the page moved on: it changes nothing.
      expect(await releaseHeldSsePosts()).toBe(1)
      await page.click('#channel-test-upstream-send')
      await autoRetry(async () => {
        const ss = await getCleanupState()
        expect(ss[`upstream_${channelId}_receivedCount`]).toBe('5')
        expect(ss[`upstream_${channelId}_lastSeq`]).toBe('5')
        expect(ss[`upstream_${channelId}_hasGap`]).toBe('false')
      })
    })
  }
}

async function openEnd(): Promise<string> {
  await navigate(`${getServerUrl()}/channel`)
  await page.click('#wire-end-open')
  let channelId = ''
  // Open on the server: the page's wire reached it, so cutWire() has a connection to drop.
  await autoRetry(async () => {
    const { end } = await getResult<WireState>('#wire-state')
    expect(end).not.toBe(null)
    channelId = end!.channelId
    expect((await getCleanupState())[`wireEnd_${channelId}_onOpen`]).toBe('true')
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
        expect((await getCleanupState())[`wireOverrun_${channelId}_onOpen`]).toBe('true')
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

// A server pinging every 7 s outlives, by its 14 s ping deadline, a WebSocket the page gives up at its 10 s RECONCILE deadline.
function testWireSlowPing() {
  test("channel: a channel keeps delivering after two drops in a row, the first reconnect's answer lost, once the server closes that dead WebSocket", async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    try {
      await navigate(`${proxy.url}/channel`)
      await page.click('#wire-end-open')
      let channelId = ''
      await autoRetry(async () => {
        const { end } = await getResult<WireState>('#wire-state')
        expect((await getCleanupState())[`wireEnd_${end!.channelId}_onOpen`]).toBe('true')
        channelId = end!.channelId
      })
      // The reconnect's RECONCILED never reaches the page, which gives that WebSocket up and reconnects again.
      proxy.muteNextWebSocket()
      await cutWire()
      // Only then does the server's ping deadline close it.
      await autoRetry(() => expect(proxy.openedBeforeMutedClosed()).not.toBe(null))
      expect(proxy.openedBeforeMutedClosed()).toBe(1)
      await runServerAction(`wireEnd_${channelId}_close`)
      await autoRetry(async () => {
        const { end } = await getResult<WireState>('#wire-state')
        expect(end!.received).deep.equal([...sentBefore, 'onClose:none'])
        expect((await getCleanupState())[`wireEnd_${channelId}_close`]).toBe('0')
      })
    } finally {
      await proxy.close()
    }
  })
}

// Run by the lane whose server waits 2 s for a page that is away.
function testWireShortReconnect() {
  const startHeldDownload = async (button: string, ready: (id: string, state: Record<string, string>) => void) => {
    await navigate(`${getServerUrl()}/file-download`)
    await page.click(button)
    let id = ''
    await autoRetry(
      async () => {
        ;({ id } = await getResult<{ id: string }>('#download-result'))
        ready(id, await getCleanupState())
      },
      { timeout: 30_000 },
    )
    return id
  }
  const goAway = async () => {
    await page.context().setOffline(true)
    await cutWire()
  }
  const expectServerRuns = async () => {
    const state = await getCleanupState()
    expect(Object.keys(state).filter((key) => key.startsWith('unhandledRejection:'))).deep.equal([])
    await page.context().setOffline(false)
    await navigate(`${getServerUrl()}/file-download`)
    await page.click('#test-file-eager')
    await autoRetry(async () => {
      expect((await getResult<{ name: string }>('#download-result')).name).toBe('eager.txt')
    })
  }

  test('file download: a page that goes away for good while the server waits to send it more ends the download, and the server keeps running', async () => {
    let sent: string | undefined
    const id = await startHeldDownload('#test-nobody-reads', (id, state) => {
      const before = sent
      sent = state[`downloadNobodyReads_${id}_sent`]
      expect(sent).not.toBe(undefined)
      expect(sent).toBe(before)
    })
    await goAway()
    await autoRetry(async () => {
      expect((await getCleanupState())[`downloadNobodyReads_${id}_cancelled`]).toBe('true')
    })
    // A rejection nothing handles comes just after.
    await new Promise((resolve) => setTimeout(resolve, 1_000))
    await expectServerRuns()
  })

  test('file download: a source that fails while its page is away, which then goes for good, leaves the server running', async () => {
    const id = await startHeldDownload('#test-fails-when-told', (id, state) => {
      expect(state[`downloadFailsWhenTold_${id}_reading`]).toBe('true')
    })
    await goAway()
    await new Promise((resolve) => setTimeout(resolve, 500))
    await runServerAction(`downloadFailsWhenTold_${id}`)
    // Past the 2 s the server waits for the page.
    await new Promise((resolve) => setTimeout(resolve, 3_000))
    await expectServerRuns()
  })
}
