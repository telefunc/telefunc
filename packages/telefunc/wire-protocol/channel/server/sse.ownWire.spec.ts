// What an SSE request does late, once its connId names a newer wire, it does to the wire it was made for: a second
// open under one connId replaces the first, and the first's later cancel or its request's later fault leave the
// newer one open.

import { expect, test } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { getChannelMux } from './ChannelMux.js'
import { handleSseChannelRequest } from './sse.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
function post(body: BodyInit) {
  const init = { method: 'POST', body, duplex: 'half' }
  return handleSseChannelRequest(new Request(url, init), undefined, () => false)
}

async function open(connId: string): Promise<ReadableStream> {
  const answer = await post(
    encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ attempt: 1, open: [] })]),
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
