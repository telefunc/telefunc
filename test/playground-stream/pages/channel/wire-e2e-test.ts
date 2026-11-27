export { testWire, testWireSlowPing, testWireShortReconnect, testUploadSessionEnd, testUploadAfterNetworkLoss }

import { page, test, expect, expectLog, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { startBlackHoleProxy } from '../../black-hole-proxy'
import type { Response, WebSocket } from 'playwright-chromium'
import { WIRE_VERSION } from '../../../../packages/telefunc/wire-protocol/constants'
import { messageFrames } from '../../../../packages/telefunc/wire-protocol/wire/frame'
import { TAG } from '../../../../packages/telefunc/wire-protocol/wire/tags'
import {
  navigate,
  getResult,
  getCleanupState,
  runServerAction,
  cutWire,
  canCutWire,
  countConnections,
  dropNextWebSocket,
  forceServerGc,
  holdNextSsePost,
  holdNextSseWire,
  holdNextSseOpen,
  heldSsePosts,
  refuseNextSsePost,
  releaseHeldSsePosts,
  sleep,
  defined,
} from '../../e2e-utils'
import { cardOf } from './upload-refusal-e2e-test'
import { burstOf, burstWhileOffline, memoryPeakOf } from './burst-e2e'
import { sendRefusedWarning, sessionEndedByUnreadable, withNetworkConditions } from '../../../utils'
import { parseChannelTransports } from '../../channelTransports'

type Tally = { text: number; binary: number; inOrder: boolean }
type WireState = {
  resume: { channelId: string; received: Tally } | null
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

  if (canCutWire())
    test('channel: a burst of 200 MiB the server sends without awaiting all arrives in order though the connection drops early in it', async () => {
      await navigate(`${getServerUrl()}/channel`)
      await page.click('#channel-test-binary-burst-unawaited-200')
      await autoRetry(async () => {
        expect(await page.evaluate(() => (window as any).__burstReceived)).greaterThanOrEqual(1)
      })
      await cutWire()
      await autoRetry(
        async () => {
          const { binaryBurst } = await getResult<{ binaryBurst: unknown }>('#channel-state')
          expect(binaryBurst).deep.include({ received: 200, inOrder: true, closeErr: null })
        },
        { timeout: 60_000 },
      )
    })

  for (const kind of canCutWire() ? (['text', 'binary'] as const) : []) {
    test(`channel: all that maxMemory lets through of a ${kind} burst nobody awaits, 5 messages of up to 64 MiB, arrives once the page is back from being offline`, async () => {
      const channelId = await burstWhileOffline(`${kind}-5`)
      await autoRetry(
        async () => {
          expect(await burstOf()).deep.include({ received: [0, 1, 2, 3, 4], onClose: null })
        },
        { timeout: 60_000 },
      )
      expect((await getCleanupState())[`wireBurst_${channelId}`]).toBe(undefined)
    })

    // 2 MiB and five of 64 MiB: the 6th is past the 320 MiB one channel holds till the page confirms the first.
    test(`channel: a 6th ${kind} message of up to 64 MiB nobody awaits, past what its channel holds, waits for the page to confirm the ones before it and arrives, the channel open and the server within maxMemory`, async () => {
      const channelId = await burstWhileOffline(`${kind}-6`)
      await autoRetry(
        async () => {
          expect(await burstOf()).deep.include({ received: [0, 1, 2, 3, 4, 5], onClose: null })
        },
        { timeout: 60_000 },
      )
      expect((await getCleanupState())[`wireBurst_${channelId}`]).toBe(undefined)
      const { held, heldWithWaiting, maxMemory } = await memoryPeakOf(channelId)
      // Five held, the 6th waiting with them.
      expect(held).greaterThanOrEqual((2 + 4 * 64) * 1024 * 1024 - 5 * 1024)
      expect(heldWithWaiting).greaterThanOrEqual((2 + 5 * 64) * 1024 * 1024 - 6 * 1024)
      expect(heldWithWaiting).lessThanOrEqual(maxMemory)
    })
  }

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
        expect(state.end?.received).deep.equal(received('onClose:none'))
        const ss = await getCleanupState()
        expect(ss[`wireEnd_${channelId}_onClose`]).toBe('none')
        expect(by === 'server' ? ss[`wireEnd_${channelId}_close`] : state.end?.close).toBe('clean')
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
        expect(end?.close).toBe('ChannelClosedError')
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

  if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).join() === 'ws') {
    // MAX_CHANNELS_PER_CONNECTION
    const maxChannels = 4_096
    const connect = async () => {
      // The wire's version, as the page's `getWireUrl` names it.
      const url = new URL(`/_telefunc?wire=${WIRE_VERSION}`, getServerUrl())
      url.protocol = 'ws:'
      const ws = new globalThis.WebSocket(url)
      ws.binaryType = 'arraybuffer'
      await new Promise((resolve) => (ws.onopen = resolve))
      // The page makes its session's id and names it from its first RECONCILE on; the server's RECONCILED names it back.
      let sessionId: string = crypto.randomUUID()
      let reconciledOnce = false
      // Takes the first answer to the RECONCILE in flight.
      let settle: ((answer: 'reconciled' | 'closed') => void) | undefined
      const answered = (answer: 'reconciled' | 'closed') => {
        settle?.(answer)
        settle = undefined
      }
      ws.onclose = () => answered('closed')
      ws.onmessage = ({ data }: MessageEvent<ArrayBuffer>) => {
        // RECONCILED, alone or among the frames of a message of several; its JSON after the 7-byte header.
        const frames = Array.from(messageFrames(new Uint8Array(data)))
        const reconciled = frames.find((frame) => frame[0] === TAG.RECONCILED)
        if (reconciled === undefined) return
        sessionId = JSON.parse(new TextDecoder().decode(reconciled.subarray(7))).sessionId
        reconciledOnce = true
        answered('reconciled')
      }
      // A RECONCILE naming what changed once the first is answered; one on a socket the server closed isn't sent.
      const answer = (reconcile: { open: object[]; released?: number[] }) =>
        new Promise<'reconciled' | 'closed' | 'already closed'>((resolve) => {
          // CLOSING from the server's close frame on, ahead of the close event.
          if (ws.readyState !== globalThis.WebSocket.OPEN) return resolve('already closed')
          settle = resolve
          // The first, on a wire that holds no session yet, numbers its attempt, as the page's does.
          const attempt = reconciledOnce ? {} : { attempt: 1 }
          const payload = new TextEncoder().encode(JSON.stringify({ sessionId, ...attempt, ...reconcile }))
          // A RECONCILE: its tag, then index 0 and seq 0 in the 7-byte header.
          const frame = new Uint8Array(7 + payload.byteLength)
          frame[0] = TAG.RECONCILE
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

    if (canCutWire())
      test('channel: a channel whose reconnect the network takes and never answers gets what the server sends on another WebSocket', async () => {
        const proxy = await startBlackHoleProxy(getServerUrl())
        try {
          await navigate(`${proxy.url}/channel`)
          await page.click('#wire-end-open')
          let channelId = ''
          await autoRetry(async () => {
            const { end } = await getResult<WireState>('#wire-state')
            if (!end) throw new Error('no end card yet')
            expect((await getCleanupState())[`wireEnd_${end.channelId}_onOpen`]).toBe('true')
            channelId = end.channelId
          })
          // Chrome holds a WebSocket whose upgrade gets no answer for 240 s before it gives up.
          proxy.holdNextWebSocket()
          await cutWire()
          await runServerAction(`wireEnd_${channelId}_close`)
          await autoRetry(async () => {
            const { end } = await getResult<WireState>('#wire-state')
            expect(end?.received).deep.equal([...sentBefore, 'onClose:none'])
          })
          // The reconnect it held was the page's: an upgrade the proxy didn't read in one piece would pass it by.
          expect(proxy.heldWebSockets()).toBe(1)
        } finally {
          await proxy.close()
        }
      })
  }

  if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).join() === 'sse,ws') {
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
      const channelId = await autoRetry(async () => {
        const { release } = await getResult<WireState>('#wire-state')
        expect(release?.close).toBe('clean')
        return defined(release, 'the release row').channelId
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

  test('channel: a page that opened and closed 4,200 channels on one connection, each close acknowledged, opens the next', async () => {
    await navigate(`${getServerUrl()}/channel`)
    const connections = countConnections()
    await page.click('#wire-many')
    await autoRetry(async () => {
      const { many } = await getResult<WireState>('#wire-state')
      expect(many).deep.equal({ closed: 4_200, next: 'open' })
    })
    // At most the one it opened for them: a page that upgrades opens its WebSocket beside its first SSE stream.
    const opened = connections.stop()
    if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).length === 1) {
      expect(opened).lessThanOrEqual(1)
    }
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

  if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).join() === 'sse') {
    testWireProxyAnswers()
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

// What a proxy in front of the server answers the page's SSE requests with.
function testWireProxyAnswers() {
  const telefuncRequests = '**/_telefunc**'

  test('channel: an SSE reopen a proxy answers with 503 and Retry-After: 2 goes again no sooner than 2 s later, and the channel delivers', async () => {
    const channelId = await openEnd()
    let refusedAt = 0
    const opensAfter: number[] = []
    await page.route(telefuncRequests, async (route) => {
      if (route.request().headers().accept !== 'text/event-stream') return route.fallback()
      if (!refusedAt) {
        refusedAt = Date.now()
        return route.fulfill({ status: 503, headers: { 'Retry-After': '2' }, contentType: 'text/html', body: 'Busy' })
      }
      opensAfter.push(Date.now() - refusedAt)
      return route.fallback()
    })
    try {
      await cutWire()
      await autoRetry(() => expect(opensAfter.length).not.toBe(0), { timeout: 20_000 })
      expect(opensAfter[0]! >= 2_000).toBe(true)
      await runServerAction(`wireEnd_${channelId}_close`)
      await autoRetry(async () => {
        const { end } = await getResult<WireState>('#wire-state')
        expect(end!.received).deep.equal([...sentBefore, 'onClose:none'])
      })
      expectLog('the server responded with a status of 503', { filter: (log) => log.logSource === 'Browser Error' })
    } finally {
      await page.unroute(telefuncRequests)
    }
  })

  test('channel: behind a proxy that answers SSE POSTs over 64 KiB with 413, 256 messages of 16 KiB each way all arrive once and in order', async () => {
    let refused = 0
    await page.route(telefuncRequests, async (route) => {
      const request = route.request()
      const isPost = request.method() === 'POST' && request.headers().accept !== 'text/event-stream'
      if (!isPost || (request.postDataBuffer()?.byteLength ?? 0) <= 64 * 1024) return route.fallback()
      refused++
      return route.fulfill({ status: 413, contentType: 'text/html', body: 'Payload Too Large' })
    })
    try {
      await navigate(`${getServerUrl()}/channel`)
      await page.click('#wire-resume')
      const all: Tally = { text: 256, binary: 256, inOrder: true }
      await autoRetry(
        async () => {
          const { resume } = await getResult<WireState>('#wire-state')
          expect(resume?.received).deep.equal(all)
          expect(JSON.parse((await getCleanupState())[`wireResume_${resume!.channelId}`]!)).deep.equal(all)
        },
        { timeout: 30_000 },
      )
      expect(refused).not.toBe(0)
      expectLog('the server responded with a status of 413', { filter: (log) => log.logSource === 'Browser Error' })
    } finally {
      await page.unroute(telefuncRequests)
    }
  })
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
        expect((await getCleanupState())[`wireEnd_${channelId}_close`]).toBe('clean')
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

  test('channel: a channel the page closes as its connection goes for good ends on the page with ChannelCloseUnknownError', async () => {
    await openEnd()
    await goAway()
    await page.click('#wire-end-close')
    await autoRetry(
      async () => {
        const { end } = await getResult<WireState>('#wire-state')
        expect(end?.close).toBe('ChannelCloseUnknownError')
        expect(end?.received).deep.equal(['onClose:ChannelCloseUnknownError'])
      },
      { timeout: 60_000 },
    )
    await page.context().setOffline(false)
  })

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

/** What the page's upload and event stream did once the frame went: when each settled, and how, in ms from the frame. */
type UploadEnd = {
  sentAt: number | null
  upload: { at: number; how: string } | null
  stream: { at: number; how: string } | null
}
declare global {
  interface Window {
    uploadEnd?: UploadEnd
    sendUnreadableOnUpload?: () => boolean
    /** The kind of each request the page made, in order. */
    requestKinds?: RequestKind[]
    /** Resolves once the browser may send the upload the page is about to make. */
    beforeUpload?: () => Promise<void>
  }
}

type RequestKind = 'open' | 'upload' | 'post'

/** For `addInitScript`: `window.requestKinds` gets the kind of each request the page makes, its SSE opens, streaming
 *  uploads and batch POSTs; each upload waits for `window.beforeUpload()` where the test exposes it. */
function watchRequestKinds() {
  const kinds: RequestKind[] = []
  window.requestKinds = kinds
  const fetchPage = window.fetch
  window.fetch = (input, init) => {
    const upload = init?.body instanceof ReadableStream
    kinds.push(upload ? 'upload' : new Headers(init?.headers).get('accept') === 'text/event-stream' ? 'open' : 'post')
    const ready = upload && window.beforeUpload ? window.beforeUpload() : Promise.resolve()
    return ready.then(() => fetchPage(input, init))
  }
}

/** For `addInitScript`: `window.sendUnreadableOnUpload()` writes a frame of a tag no version has on the page's newest
 *  streaming upload (packages/telefunc/wire-protocol/wire/tags.ts), after its u32 length; `window.uploadEnd` says how
 *  the upload and the event stream ended after it. */
function watchUploadEnd() {
  const seen: UploadEnd = { sentAt: null, upload: null, stream: null }
  window.uploadEnd = seen
  const at = () => Math.round(performance.now() - (seen.sentAt ?? performance.now()))
  let upload: TransformStreamDefaultController<Uint8Array> | null = null
  const fetchPage = window.fetch
  window.fetch = (input, init) => {
    const body = init?.body
    if (body instanceof ReadableStream) {
      const tap = new TransformStream<Uint8Array, Uint8Array>({ start: (controller) => void (upload = controller) })
      return fetchPage(input, { ...init, body: body.pipeThrough(tap) }).then(
        (response) => {
          if (seen.sentAt !== null) seen.upload = { at: at(), how: `answered ${response.status}` }
          return response
        },
        (err: unknown) => {
          if (seen.sentAt !== null) seen.upload = { at: at(), how: `failed ${String(err)}` }
          throw err
        },
      )
    }
    if (new Headers(init?.headers).get('accept') !== 'text/event-stream') return fetchPage(input, init)
    return fetchPage(input, init).then((response) => {
      if (response.body === null) return response
      const reader = response.body.getReader()
      const tracked = new ReadableStream<Uint8Array>({
        pull: (controller) =>
          reader.read().then(
            ({ done, value }) => {
              if (!done) return controller.enqueue(value)
              if (seen.sentAt !== null) seen.stream = { at: at(), how: 'ended' }
              controller.close()
            },
            (err: unknown) => {
              if (seen.sentAt !== null) seen.stream = { at: at(), how: `failed ${String(err)}` }
              controller.error(err)
            },
          ),
        cancel: (reason) => {
          if (seen.sentAt !== null) seen.stream = { at: at(), how: 'cancelled by the page' }
          return reader.cancel(reason)
        },
      })
      return new Response(tracked, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      })
    })
  }
  window.sendUnreadableOnUpload = () => {
    if (upload === null) return false
    seen.sentAt = performance.now()
    upload.enqueue(new Uint8Array([0, 0, 0, 3, 0x0f, 0, 0]))
    return true
  }
}

/** Docker lanes on SSE only: there the browser reaches the server over HTTP/2, the only way Chromium streams an upload. */
function testUploadSessionEnd() {
  test("channel: over HTTP/2, a session the server ends while the page's streaming upload is open and idle reaches every channel with its cause by the event stream's last event; the upload fails no fetch before it, and the page opens nothing more", async () => {
    const browser = page.context().browser()
    if (browser === null) throw new Error('no browser')
    // A page of its own: its fetch stays wrapped for as long as it lives.
    const context = await browser.newContext({ ignoreHTTPSErrors: true })
    try {
      const own = await context.newPage()
      await own.addInitScript(watchUploadEnd)
      const requests: string[] = []
      let counting = false
      own.on(
        'request',
        (request) => void (counting && requests.push(`${request.method()} ${new URL(request.url()).pathname}`)),
      )
      await navigate(`${getServerUrl()}/channel`, undefined, own)
      await own.click('#wire-upload-refused-open')
      await autoRetry(async () => {
        expect((await cardOf(own))?.kept.close).toBe('open')
      })
      // The upload idle: the page writes nothing on it meanwhile.
      await sleep(1_000)
      counting = true
      expect(await own.evaluate(() => window.sendUnreadableOnUpload?.() ?? false)).toBe(true)
      await autoRetry(async () => {
        const card = await cardOf(own)
        expect([card?.refused.close, card?.kept.close]).deep.equal([sessionEndedByUnreadable, sessionEndedByUnreadable])
      })
      await sleep(1_000)
      const seen = await own.evaluate(() => window.uploadEnd)
      if (!seen?.stream) throw new Error('the event stream did not end')
      // The upload settles, where it does, no sooner than the stream's end, and not as a failed fetch.
      expect(seen.upload === null || (seen.upload.at >= seen.stream.at && !seen.upload.how.includes('TypeError'))).toBe(
        true,
      )
      expect(requests).deep.equal([])
    } finally {
      await context.close()
    }
  })
}

function testUploadAfterNetworkLoss() {
  test('channel: over HTTP/2, a page whose first streaming upload the network fails, and which loses its wire to it, uploads again on the wire it reconnects on: its sends take no batch POST', async () => {
    const browser = page.context().browser()
    if (browser === null) throw new Error('no browser')
    const context = await browser.newContext({ ignoreHTTPSErrors: true })
    // The network goes as the page's first upload goes out, and comes back half a second later, as a phone leaving
    // Wi-Fi: the upload fails to fetch, and so does what the page sends meanwhile, which loses it the wire.
    let networkLost = false
    await context.exposeBinding('beforeUpload', async () => {
      if (networkLost) return
      networkLost = true
      await context.setOffline(true)
      setTimeout(() => void context.setOffline(false), 500)
    })
    try {
      const own = await context.newPage()
      await own.addInitScript(watchRequestKinds)
      await navigate(`${getServerUrl()}/channel`, undefined, own)
      await own.click('#wire-upload-refused-open')
      await autoRetry(async () => {
        expect((await cardOf(own))?.kept.close).toBe('open')
      })
      await sleep(3_000)
      const before = (await own.evaluate(() => window.requestKinds?.length)) ?? 0
      for (let i = 0; i < 10; i++) {
        await own.click('#wire-upload-refused-send-kept')
        await sleep(300)
      }
      await autoRetry(async () => {
        expect((await cardOf(own))?.kept.sent).toBe(10)
      })
      const kinds = (await own.evaluate(() => window.requestKinds)) ?? []
      // What the page sent from its second open on: the wires it reconnected on.
      const second = kinds.indexOf('open', kinds.indexOf('open') + 1)
      const reopened = second === -1 ? [] : kinds.slice(second)
      expect([networkLost, reopened.includes('upload'), kinds.slice(before)]).deep.equal([true, true, []])
    } finally {
      await context.close()
    }
  })
}
