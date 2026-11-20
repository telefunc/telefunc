import { expect, test, vi } from 'vitest'

import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { config } from '../../../node/server/serverConfig.js'
import { handleSseChannelRequest } from './sse.js'

// A ping deadline of 2.5 s: two intervals, after the half interval the page may hold its PING for a POST.
config.channel = { pingInterval: 1000 }

function post(body: BodyInit) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  return handleSseChannelRequest(
    new Request(url, { method: 'POST', body, duplex: 'half' } as RequestInit),
    undefined,
    () => false,
  )
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
  // Its RECONCILED: the server ran the open's RECONCILE, which starts the deadline.
  expect((await (opened.body as ReadableStream<Uint8Array>).getReader().read()).done).toBe(false)

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
})

test('a page that held its PING for a POST, then stalled for most of an interval, keeps its wire', async () => {
  const connId = crypto.randomUUID()
  const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
  expect(opened.statusCode).toBe(200)
  // Its RECONCILED: the server ran the open's RECONCILE, which starts the deadline.
  expect((await (opened.body as ReadableStream<Uint8Array>).getReader().read()).done).toBe(false)

  // Its PING goes an interval on, held half an interval for a POST, and the page stalls 0.8 interval more.
  await new Promise((resolve) => setTimeout(resolve, 2_300))
  expect((await post(encodeSseRequest({ connId }, [encode.ping([])]))).statusCode).toBe(200)
})

test('a page that stays silent past the ping deadline loses its wire', async () => {
  const connId = crypto.randomUUID()
  const openedAt = performance.now()
  const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
  expect(opened.statusCode).toBe(200)

  const events = (opened.body as ReadableStream<Uint8Array>).getReader()
  while (!(await events.read()).done);
  expect(performance.now() - openedAt).toBeGreaterThanOrEqual(2_000)
})

test('a ping deadline judged while its timer runs ahead of the performance clock rechecks one timer later, with no negative delay', async () => {
  // Node runs a timer by the loop's clock, read as its turn began; `performance.now()` moves on as the turn works. Here
  // it reads 1 ms later at each read in a turn, so a timer armed after some reads runs before its time on that clock.
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
  let readsThisTurn = 0
  const now = vi.spyOn(performance, 'now').mockImplementation(() => Date.now() + readsThisTurn++)
  const delays: number[] = []
  let turns = 0
  // The timer turn that armed a timer due at once: the recheck's.
  let armedNowAt: number | undefined
  const realSetTimeout = globalThis.setTimeout
  const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout').mockImplementation(((
    callback: () => void,
    delay?: number,
  ) => {
    delays.push(delay ?? 0)
    if ((delay ?? 0) <= 0) armedNowAt ??= turns
    return realSetTimeout(callback, delay)
  }) as typeof setTimeout)
  try {
    const connId = crypto.randomUUID()
    const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
    expect(opened.statusCode).toBe(200)
    const events = (opened.body as ReadableStream<Uint8Array>).getReader()
    // The open's RECONCILE arms the deadline again once the server read the POST's body, some turns after it answered:
    // as many as this Node takes. Its RECONCILED, the stream's first event after a comment, shows it ran.
    const decoder = new TextDecoder()
    let head = ''
    while (!head.includes('data: ')) {
      const { done, value } = await events.read()
      expect(done).toBe(false)
      head += decoder.decode(value, { stream: true })
    }
    let ended = false
    let endedAt: number | undefined
    void (async () => {
      while (!(await events.read()).done);
      ended = true
      endedAt = turns
    })()
    // The page stays silent: each turn of timers, until the wire is cut.
    while (!ended && turns < 100) {
      readsThisTurn = 0
      await vi.advanceTimersToNextTimerAsync()
      turns++
    }
    expect(ended).toBe(true)
    expect(delays.filter((delay) => delay < 0)).toEqual([])
    expect(armedNowAt).toBeDefined()
    expect(endedAt).toBeGreaterThan(armedNowAt ?? Infinity)
  } finally {
    setTimeoutSpy.mockRestore()
    now.mockRestore()
    vi.useRealTimers()
  }
})
