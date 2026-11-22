// What an SSE request does late, once its connId names a newer wire, it does to the wire it was made for: a second
// open under one connId replaces the first, and the first's later cancel, its request's later fault or the end of the
// time the server keeps its record leave the newer one alone.

import { expect, test, vi } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
function post(body: BodyInit) {
  const init = { method: 'POST', body, duplex: 'half' }
  return handleSseChannelRequest(new Request(url, init), undefined, () => false)
}

async function open(connId: string): Promise<ReadableStream> {
  const answer = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }),
    ]),
  )
  if (!(answer.body instanceof ReadableStream)) throw new Error('No event stream')
  return answer.body
}

test('cancelling the older of two event streams for one connId leaves the newer wire open', async () => {
  const connId = crypto.randomUUID()
  const mux = getChannelMux()
  const older = await open(connId)
  const newer = await open(connId)
  const replacement = mux.getWireByConnId(connId)
  try {
    await older.cancel()
    expect([replacement !== undefined, mux.getWireByConnId(connId) === replacement]).toEqual([true, true])
  } finally {
    await newer.cancel()
  }
})

test('a batch POST of the older wire that meets an over-limit frame after the newer one opened ends the older wire alone', async () => {
  const connId = crypto.randomUUID()
  const mux = getChannelMux()
  const older = await open(connId)
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
  const newer = await open(connId)
  const replacement = mux.getWireByConnId(connId)
  try {
    controller.enqueue(encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1))
    controller.enqueue(new Uint8Array(16))
    controller.close()
    await answer
    expect([replacement !== undefined, mux.getWireByConnId(connId) === replacement]).toEqual([true, true])
  } finally {
    await Promise.allSettled([older.cancel(), newer.cancel()])
  }
})

test("the record the server keeps of the older of two closed event streams for one connId, at its `connectTtl`, leaves the newer one's: the page's close POST ends the newer one's session", async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  const mux = getChannelMux()
  const connId = crypto.randomUUID()
  const sessionId = crypto.randomUUID()
  const channel = new ServerChannel()
  const ends: string[] = []
  channel.onClose((error) => void ends.push(error?.name ?? 'clean'))
  mux.registerChannel(channel)
  /** An event stream of the session under `connId`, read up to its RECONCILED. */
  const reconciled = async (attempt: number) => {
    const answer = await post(
      encodeSseRequest({ connId, streamResponse: true }, [
        encode.reconcile({ sessionId, attempt, open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
      ]),
    )
    if (!(answer.body instanceof ReadableStream)) throw new Error('No event stream')
    const reader = answer.body.getReader()
    const events = new SseEvents()
    let done = false
    while (!done) {
      const read = await reader.read()
      if (read.done) throw new Error('No RECONCILED')
      events.push(read.value, (frame) => void (done ||= frame[0] === TAG.RECONCILED))
    }
    return reader
  }
  try {
    // The newer stream closes the older one, whose record the server keeps from then on.
    const older = await reconciled(1)
    const newer = await reconciled(2)
    await older.cancel()
    await vi.advanceTimersByTimeAsync(mux.connectTtl / 3)
    await newer.cancel()
    // Past the older record's `connectTtl`, inside the newer one's.
    await vi.advanceTimersByTimeAsync(mux.connectTtl * (2 / 3) + 100)
    const left = await post(encodeSseRequest({ connId, close: true }))
    expect({ left: left.statusCode, ends: [...ends] }).toEqual({ left: 200, ends: ['ChannelPageLeftError'] })
  } finally {
    channel.abort()
    vi.useRealTimers()
  }
})
