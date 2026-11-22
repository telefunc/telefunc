import { expect, test, vi } from 'vitest'

import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { base64urlToUint8Array } from '../../wire/base64url.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { config } from '../../../node/server/serverConfig.js'
import { handleSseChannelRequest } from './sse.js'

// A ping deadline of 2 s.
config.channel = { pingInterval: 1000 }

function post(body: BodyInit) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  return handleSseChannelRequest(new Request(url, { method: 'POST', body, duplex: 'half' } as RequestInit))
}

/** The event stream an open was answered with. */
function eventsOf({ body }: Awaited<ReturnType<typeof post>>) {
  if (!(body instanceof ReadableStream)) throw new Error('The open was answered without an event stream')
  return body.getReader()
}

/** The frames an open's event stream carries, one per call, past its comments; null once it ended. */
function framesOf(opened: Awaited<ReturnType<typeof post>>) {
  const events = eventsOf(opened)
  const decoder = new TextDecoder()
  let buffered = ''
  return async (): Promise<Uint8Array<ArrayBuffer> | null> => {
    while (true) {
      const end = buffered.indexOf('\n\n')
      if (end !== -1) {
        const data = buffered
          .slice(0, end)
          .split('\n')
          .find((line) => line.startsWith('data: '))
        buffered = buffered.slice(end + 2)
        if (data !== undefined) return base64urlToUint8Array(data.slice('data: '.length))
        continue
      }
      const { done, value } = await events.read()
      if (done) return null
      buffered += decoder.decode(value, { stream: true })
    }
  }
}

/** The next frame of `tag` the stream carries; fails, saying so, if it ends first. */
async function nextOf(next: () => Promise<Uint8Array<ArrayBuffer> | null>, tag: number, name: string) {
  while (true) {
    const frame = await next()
    if (frame === null) throw new Error(`The event stream ended before its ${name}`)
    if (frame[0] === tag) return frame
  }
}

test('a batch POST whose wire closed before its frames ran is refused', async () => {
  const connId = crypto.randomUUID()
  const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
  expect(opened.statusCode).toBe(200)

  // The batch's metadata arrives at once, its frame only once the handler reads for it.
  const reading = createDeferred<ReadableStreamDefaultController<Uint8Array>>()
  let reads = 0
  const body = new ReadableStream<Uint8Array>(
    {
      pull: (controller) => {
        if (++reads === 1) controller.enqueue(encodeSseRequestMetadata({ connId }))
        else reading.resolve(controller)
      },
    },
    { highWaterMark: 0 },
  )
  const answer = post(body)
  const controller = await reading.promise

  // The page's event stream is cancelled, closing the wire, while the batch's frame is on its way.
  await (opened.body as ReadableStream).cancel()
  const ping = encode.ping([])
  controller.enqueue(encodeU32(ping.byteLength))
  controller.enqueue(ping)
  controller.close()

  expect((await answer).statusCode).toBe(400)
})

test('a page whose upload POST is read only after server code held the event loop past the ping deadline keeps its wire', async () => {
  const connId = crypto.randomUUID()
  const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
  expect(opened.statusCode).toBe(200)
  // Its RECONCILED: the server ran the open's RECONCILE, which starts the deadline. The stream's first bytes are a
  // comment the server writes before it reads the open.
  const next = framesOf(opened)
  await nextOf(next, TAG.RECONCILED, 'RECONCILED')

  // Server code holds the event loop 1 s past the deadline, as a synchronous loop of sends does: the deadline's timer
  // runs 1 s late, which leaves the read of the upload below room on a loaded machine.
  const until = performance.now() + 3_000
  while (performance.now() < until);

  // The page sent its upload POST meanwhile; the server reads it some turns after letting go of the event loop. Its
  // 200 ms are armed in the first timers phase after the hold, which reads the clock again: a timer armed during the
  // hold counts from the loop's time read before it, so it would already be due.
  const ping = encode.ping([])
  const upload = post(
    new ReadableStream<Uint8Array>({
      start: (controller) => {
        setTimeout(() =>
          setTimeout(() => {
            controller.enqueue(encodeSseRequestMetadata({ connId, streamRequest: true }))
            controller.enqueue(encodeU32(ping.byteLength))
            controller.enqueue(ping)
            controller.close()
          }, 200),
        )
      },
    }),
  )

  expect((await upload).statusCode).toBe(200)
  // The wire it came on is the one the server answers it on.
  await nextOf(next, TAG.PONG, 'PONG')
})

test('a page that stays silent past the ping deadline loses its wire', async () => {
  const connId = crypto.randomUUID()
  const openedAt = performance.now()
  const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
  expect(opened.statusCode).toBe(200)

  const events = eventsOf(opened)
  while (!(await events.read()).done);
  expect(performance.now() - openedAt).toBeGreaterThanOrEqual(2_000)
})

test('a silent page whose ping deadline timer runs before its time on the performance clock loses its wire a timer after its deadline, and no timer gets a negative delay', async () => {
  // Node runs a timer by the loop's clock, read as its turn began; `performance.now()` moves on as a turn works. Here
  // it reads 1 ms ahead while the open is handled, and in each turn of timers from the loop's time on, 1 ms later at each
  // read: so the deadline, judged 1 ms early and armed again for that millisecond, runs a timer before it is due.
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
  let readsThisTurn: number | null = null
  const now = vi
    .spyOn(performance, 'now')
    .mockImplementation(() => Date.now() + (readsThisTurn === null ? 1 : readsThisTurn++))
  const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')
  try {
    const connId = crypto.randomUUID()
    const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
    expect(opened.statusCode).toBe(200)
    const next = framesOf(opened)
    await nextOf(next, TAG.RECONCILED, 'RECONCILED')
    const reconciledAt = Date.now()
    let endedAt: number | null = null
    async function readToEnd() {
      while ((await next()) !== null);
      endedAt = Date.now()
    }
    void readToEnd()
    // The page stays silent: each turn of timers, until the wire is cut.
    for (let turns = 0; endedAt === null && turns < 100; turns++) {
      readsThisTurn = 0
      await vi.advanceTimersToNextTimerAsync()
      readsThisTurn = null
    }
    // At the 2 s deadline and a recheck a timer later, not a whole deadline more.
    expect((endedAt ?? Number.POSITIVE_INFINITY) - reconciledAt).toBeLessThan(2_100)
    expect(setTimeoutSpy.mock.calls.map(([, delay]) => delay ?? 0).filter((delay) => delay < 0)).toEqual([])
  } finally {
    setTimeoutSpy.mockRestore()
    now.mockRestore()
    vi.useRealTimers()
  }
})
